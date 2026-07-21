#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR="${HOME}/.hermes/plugins/platforms/maxru"
REPO_URL="https://github.com/m0rtal/hermes-maxru-platform.git"

echo "Installing Hermes MAX.ru platform plugin..."

mkdir -p "${PLUGIN_DIR}"

if [ -d "${PLUGIN_DIR}/.git" ]; then
    echo "Plugin already installed. Pulling latest..."
    cd "${PLUGIN_DIR}"
    git pull --ff-only
else
    echo "Cloning plugin..."
    git clone --depth=1 "${REPO_URL}" "${PLUGIN_DIR}"
fi

echo "Plugin installed at ${PLUGIN_DIR}"
echo "Restart the gateway: hermes gateway restart"
