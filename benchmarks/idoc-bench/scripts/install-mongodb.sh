#!/usr/bin/env bash
set -euo pipefail

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to install MongoDB on macOS."
  echo "Install it from https://brew.sh and rerun this script."
  exit 1
fi

brew tap mongodb/brew
brew install mongodb-community@7.0
brew services start mongodb-community@7.0

echo "MongoDB service started."
echo "Check status: brew services list"
echo "Stop service: brew services stop mongodb-community@7.0"
