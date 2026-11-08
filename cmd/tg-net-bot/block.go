package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/theillarion/tg-net-bot/internal/blockcheck"
	"github.com/theillarion/tg-net-bot/internal/cheburcheck"
	"github.com/theillarion/tg-net-bot/internal/config"
	"github.com/theillarion/tg-net-bot/internal/storage"
)

type blockCLIService interface {
	CheckBatch(context.Context, []string, func(blockcheck.BatchItem)) error
	Probe(context.Context, string) (blockcheck.ProbeResult, error)
}

type blockCLIFactory func(string, string, time.Duration) (blockCLIService, io.Closer, error)
type blockCacheOpener func(context.Context, string, string) (blockcheck.Cache, io.Closer, error)

func newBlockCommand() *cobra.Command {
	return newBlockCommandWith(openBlockService)
}

func openBlockService(redisAddress, redisPassword string, cacheTTL time.Duration) (blockCLIService, io.Closer, error) {
	return openBlockServiceWith(context.Background(), redisAddress, redisPassword, cacheTTL, func(ctx context.Context, address, password string) (blockcheck.Cache, io.Closer, error) {
		cache, err := storage.OpenRedisCache(ctx, address, password)
		if err != nil {
			return nil, nil, err
		}
		return cache, cache, nil
	})
}

func openBlockServiceWith(ctx context.Context, redisAddress, redisPassword string, cacheTTL time.Duration, openCache blockCacheOpener) (blockCLIService, io.Closer, error) {
	if cacheTTL <= 0 {
		return nil, nil, fmt.Errorf("cache ttl must be positive")
	}
	cache, closer, err := openCache(ctx, redisAddress, redisPassword)
	if err != nil {
		return nil, nil, err
	}
	service, err := blockcheck.New(cheburcheck.New(), cache, cacheTTL)
	if err != nil {
		_ = closer.Close()
		return nil, nil, err
	}
	return service, closer, nil
}

func newBlockCommandWith(factory blockCLIFactory) *cobra.Command {
	var redisURL, output string
	var cacheTTL time.Duration
	cmd := &cobra.Command{
		Use:   "block <server> [server...]",
		Short: "Check targets against block-lists (probe is a separate command)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (runErr error) {
			for _, target := range args {
				if strings.ContainsAny(target, "/ ") || strings.TrimSpace(target) != target {
					return fmt.Errorf("invalid target %q: expected an IP or domain", target)
				}
			}
			if output != "text" && output != "json" && output != "ndjson" {
				return fmt.Errorf("unsupported output %q: use text, json, or ndjson", output)
			}
			address, password, configuredTTL, err := config.LoadBlockCheckSettings()
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("redis-url") {
				address = redisURL
			}
			if cmd.Flags().Changed("cache-ttl") {
				configuredTTL = cacheTTL
			}
			if configuredTTL <= 0 {
				return fmt.Errorf("cache ttl must be positive")
			}
			service, closer, err := factory(address, password, configuredTTL)
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := closer.Close(); closeErr != nil && runErr == nil {
					runErr = fmt.Errorf("close block check storage: %w", closeErr)
				}
			}()
			items := make([]blockcheck.BatchItem, 0, len(args))
			var writeErr error
			ctx, cancel := context.WithTimeout(cmd.Context(), time.Duration(len(args))*3*time.Second+30*time.Second)
			defer cancel()
			if output == "ndjson" {
				writeErr = json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{
					"type": "limits", "check_requests_per_minute_default": 30,
					"probe_requests_per_minute_default": 5, "burst_default": 1,
					"note": "upstream deployment settings may differ",
				})
			}
			if writeErr != nil {
				return fmt.Errorf("write block check output: %w", writeErr)
			}
			err = service.CheckBatch(ctx, args, func(item blockcheck.BatchItem) {
				if writeErr != nil {
					cancel()
					return
				}
				if output == "ndjson" {
					writeErr = json.NewEncoder(cmd.OutOrStdout()).Encode(item)
					return
				}
				items = append(items, item)
				if output == "text" {
					write := func(format string, values ...any) {
						if writeErr == nil {
							_, writeErr = fmt.Fprintf(cmd.OutOrStdout(), format, values...)
						}
					}
					write("%s: %s", item.Target, item.Status)
					if item.Result.Target != "" {
						write(" · %s", baseVerdict(item.Result.Blocked))
						if item.Result.CacheKey != "" {
							write(" · probe key: %s", item.Result.CacheKey)
						}
					}
					if item.Error != "" {
						write(" · %s", item.Error)
					}
					if writeErr == nil {
						_, writeErr = fmt.Fprintln(cmd.OutOrStdout())
					}
					if writeErr != nil {
						cancel()
					}
				}
			})
			if writeErr != nil {
				return fmt.Errorf("write block check output: %w", writeErr)
			}
			if err != nil {
				return err
			}
			if output == "json" {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{
					"limits":  map[string]any{"check_requests_per_minute_default": 30, "probe_requests_per_minute_default": 5, "burst_default": 1, "note": "upstream deployment settings may differ"},
					"results": items,
				})
			}
			if output == "text" {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "Cheburcheck defaults: 30 check/min and 5 probe/min, burst 1; upstream deployment may differ.")
				return err
			}
			return nil
		},
	}
	cmd.PersistentFlags().StringVar(&redisURL, "redis-url", "", "Redis address or redis:// URL (overrides REDIS_ADDR)")
	cmd.PersistentFlags().DurationVar(&cacheTTL, "cache-ttl", 24*time.Hour, "Maximum probe context lifetime; upstream expiry always wins (overrides BLOCKCHECK_CACHE_TTL)")
	cmd.Flags().StringVar(&output, "output", "text", "Output format: text, json, or ndjson")
	cmd.AddCommand(newBlockProbeCommandWith(factory, &redisURL, &cacheTTL))
	return cmd
}

func newBlockProbeCommandWith(factory blockCLIFactory, redisURL *string, cacheTTL *time.Duration) *cobra.Command {
	return &cobra.Command{
		Use:   "probe <cache-key>",
		Short: "Run a manual extended Cheburcheck probe",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (runErr error) {
			address, password, configuredTTL, err := config.LoadBlockCheckSettings()
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("redis-url") || cmd.Parent().Flags().Changed("redis-url") {
				address = *redisURL
			}
			if cmd.Flags().Changed("cache-ttl") || cmd.Parent().Flags().Changed("cache-ttl") {
				configuredTTL = *cacheTTL
			}
			if configuredTTL <= 0 {
				return fmt.Errorf("cache ttl must be positive")
			}
			service, closer, err := factory(address, password, configuredTTL)
			if err != nil {
				return err
			}
			defer func() {
				if closeErr := closer.Close(); closeErr != nil && runErr == nil {
					runErr = fmt.Errorf("close block check storage: %w", closeErr)
				}
			}()
			ctx, cancel := context.WithTimeout(cmd.Context(), 90*time.Second)
			defer cancel()
			result, err := service.Probe(ctx, args[0])
			if err != nil {
				return err
			}
			if err = json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{
				"limits": map[string]any{"probe_requests_per_minute_default": 5, "burst_default": 1, "note": "upstream deployment settings may differ"},
				"result": result,
			}); err != nil {
				return fmt.Errorf("write probe output: %w", err)
			}
			return nil
		},
	}
}

func baseVerdict(blocked bool) string {
	if blocked {
		return "listed as blocked"
	}
	return "not found in block-lists"
}
