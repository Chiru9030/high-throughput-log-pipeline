#!/usr/bin/env bash
set -euo pipefail

# Hardened Linux VM provisioning script for high-throughput-log-pipeline (Ubuntu/Debian)
APP_USER="logpipeline"
BIN_PATH="/usr/local/bin/log-pipeline"
DATA_DIR="/var/lib/log-pipeline"

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  sudo useradd --system --no-create-home --shell /usr/sbin/nologin "$APP_USER"
fi

CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/log-pipeline ./cmd/pipeline
sudo install -m 0755 /tmp/log-pipeline "$BIN_PATH"
sudo mkdir -p "$DATA_DIR"
sudo chown -R "$APP_USER:$APP_USER" "$DATA_DIR"

if [ ! -f /etc/log-pipeline.env ]; then
  API_KEY="$(openssl rand -hex 24)"
  sudo bash -c "umask 077 && printf 'LISTEN_ADDR=:8090\nNDJSON_PATH=$DATA_DIR/logs.ndjson\nWAL_PATH=$DATA_DIR/logs.wal\nINGEST_API_KEY=%s\n' '$API_KEY' > /etc/log-pipeline.env"
fi

sudo tee /etc/systemd/system/log-pipeline.service >/dev/null <<EOF
[Unit]
Description=High-Throughput Log Pipeline Service
After=network.target

[Service]
User=$APP_USER
Group=$APP_USER
EnvironmentFile=/etc/log-pipeline.env
ExecStart=$BIN_PATH
Restart=always
RestartSec=3
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now log-pipeline.service
