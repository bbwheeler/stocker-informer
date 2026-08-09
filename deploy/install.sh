#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="/opt/stocker-informer"
QUADLET_DIR="/etc/containers/systemd"
ENV_DIR="%h/.config/stocker-informer"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ $EUID -ne 0 ]]; then
  echo "Error: run as root (sudo ./install.sh)" >&2
  exit 1
fi

# Expand tilde path for non-root env dir if needed
ENV_DIR=$(eval echo "$ENV_DIR")

echo "==> Installing project to $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"
cp -r "$PROJECT_DIR"/Containerfile "$PROJECT_DIR"/go.mod "$PROJECT_DIR"/go.sum \
      "$PROJECT_DIR"/cmd "$PROJECT_DIR"/internal "$INSTALL_DIR/"

echo "==> Installing Quadlet units to $QUADLET_DIR"
mkdir -p "$QUADLET_DIR"
cp "$SCRIPT_DIR"/quadlet/stocker-informer.build "$QUADLET_DIR/"
cp "$SCRIPT_DIR"/quadlet/stocker-informer.container "$QUADLET_DIR/"

echo "==> Installing environment file to $ENV_DIR"
mkdir -p "$(dirname "$ENV_DIR")"
cp "$PROJECT_DIR"/.env.podman "$ENV_DIR/"
chmod 600 "$ENV_DIR/.env.podman"

echo "==> Reloading systemd user services"
systemctl --user daemon-reload

echo ""
echo "Done. Next steps:"
echo ""
echo "  1. Edit $ENV_DIR/.env.podman and fill in GOTOSOCIAL_INSTANCE, GOTOSOCIAL_USER, GOTOSOCIAL_TOKEN, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP"
echo ""
echo "  2. Build the image:"
echo "       systemctl --user start stocker-informer-build.service"
echo ""
echo "  3. Enable and start the service:"
echo "       systemctl --user enable --now stocker-informer.service"
echo ""
echo "  4. Check status:"
echo "       systemctl status stocker-informer"
echo "       podman logs stocker-informer"
