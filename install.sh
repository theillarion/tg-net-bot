#!/bin/sh
set -eu

REPO_URL=${REPO_URL:-https://github.com/theillarion/tg-net-bot.git}
INSTALL_DIR=${INSTALL_DIR:-.}

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install Docker Engine and the Compose plugin first." >&2
  exit 1
fi
if ! command -v git >/dev/null 2>&1; then
  echo "Git is required to clone the project." >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose plugin is required." >&2
  exit 1
fi

printf 'Installation directory [%s]: ' "$INSTALL_DIR"
if ! IFS= read -r requested_dir; then
  requested_dir=
fi
if [ -n "$requested_dir" ]; then
  INSTALL_DIR=$requested_dir
fi

if [ ! -d "$INSTALL_DIR/.git" ]; then
  git clone "$REPO_URL" "$INSTALL_DIR"
fi
cd "$INSTALL_DIR"
printf 'Installing in: %s\n' "$PWD"

configure_env=yes
if [ -f .env ]; then
  printf 'Existing .env found. Replace it? [y/N] '
  if ! IFS= read -r answer; then
    answer=
  fi
  case "$answer" in
    y|Y|yes|YES) ;;
    *) echo "Keeping existing configuration."; configure_env=no ;;
  esac
fi

if [ "$configure_env" = yes ]; then
  printf 'Telegram bot token (from @BotFather): '
  read token
  if [ -z "$token" ]; then echo "Token is required." >&2; exit 1; fi
  printf 'Allowed Telegram user IDs (comma separated): '
  read users
  if [ -z "$users" ]; then echo "At least one user ID is required." >&2; exit 1; fi
  printf 'Telegram proxy URL (optional, press Enter to skip): '
  read proxy

  umask 077
  {
    printf 'TELEGRAM_TOKEN=%s\n' "$token"
    printf 'TALAGRAM_ALLOW_USERS=%s\n' "$users"
    printf 'TELEGRAM_PROXY=%s\n' "$proxy"
    printf 'DATABASE_PATH=/app/data/tg-net-bot.sqlite\n'
  } > .env
fi
mkdir -p data
for existing_name in tg-net-bot tg-net-tools-bot; do
  if docker container inspect "$existing_name" >/dev/null 2>&1; then
    printf 'An existing container named %s is running. Remove it before starting this installation? [y/N] ' "$existing_name"
    if ! IFS= read -r answer; then
      answer=
    fi
    case "$answer" in
      y|Y|yes|YES) docker rm -f "$existing_name" ;;
      *) echo "Existing container kept; installation cancelled." >&2; exit 1 ;;
    esac
  fi
done
docker compose up -d --build
echo "Bot is running. Open Telegram and send /start to the bot."
