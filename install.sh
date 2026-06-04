#!/usr/bin/env bash
# Install trader dashboard as a systemd service on Linux.
# Binary source: https://github.com/berserkkv/trader_backtest/blob/main/trader
set -euo pipefail

REPO_OWNER="berserkkv"
REPO_NAME="trader_backtest"
BINARY_PATH="trader"
# GitHub "blob" links are HTML; raw content is served from raw.githubusercontent.com
BINARY_URL="https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/main/${BINARY_PATH}"

INSTALL_DIR="${INSTALL_DIR:-/opt/trader}"
SERVICE_NAME="${SERVICE_NAME:-trader}"
SERVICE_USER="${SERVICE_USER:-trader}"
HTTP_ADDR="${HTTP_ADDR:-:8080}"
BRANCH="${BRANCH:-main}"

if [[ "${BRANCH}" != "main" ]]; then
  BINARY_URL="https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}/${BINARY_PATH}"
fi

log() { printf '[install] %s\n' "$*"; }
die() { printf '[install] ERROR: %s\n' "$*" >&2; exit 1; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    die "Run as root: sudo $0"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

install_deps() {
  if command -v apt-get >/dev/null 2>&1; then
  if ! command -v curl >/dev/null 2>&1; then
      log "Installing curl…"
      apt-get update -qq
      DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl ca-certificates
    fi
  elif command -v dnf >/dev/null 2>&1; then
    if ! command -v curl >/dev/null 2>&1; then
      log "Installing curl…"
      dnf install -y curl ca-certificates
    fi
  elif command -v yum >/dev/null 2>&1; then
    if ! command -v curl >/dev/null 2>&1; then
      log "Installing curl…"
      yum install -y curl ca-certificates
    fi
  fi
  require_cmd curl
}

create_user() {
  if ! id "${SERVICE_USER}" &>/dev/null; then
    log "Creating system user ${SERVICE_USER}…"
    useradd --system --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
  fi
}

download_binary() {
  local dest="${INSTALL_DIR}/${BINARY_PATH}"
  local tmp
  tmp="$(mktemp)"
  trap 'rm -f "${tmp}"' RETURN

  log "Downloading binary from ${BINARY_URL}"
  if ! curl -fsSL --retry 3 --retry-delay 2 -o "${tmp}" "${BINARY_URL}"; then
    die "Download failed. Check network and that the file exists on GitHub."
  fi

  # Reject HTML error pages (common when raw URL is wrong)
  if head -c 512 "${tmp}" | grep -qi '<!DOCTYPE html\|<html'; then
    die "Downloaded content looks like HTML, not a binary. Verify BINARY_URL."
  fi

  if [[ ! -s "${tmp}" ]]; then
    die "Downloaded file is empty."
  fi

  install -d -m 0755 "${INSTALL_DIR}"
  install -m 0755 "${tmp}" "${dest}"
  chown root:root "${dest}"
  log "Installed binary to ${dest}"
}

write_env_file() {
  local env_file="/etc/default/${SERVICE_NAME}"
  cat >"${env_file}" <<EOF
# Trader service environment (edit and: systemctl restart ${SERVICE_NAME})
HTTP_ADDR=${HTTP_ADDR}
EOF
  chmod 0644 "${env_file}"
  log "Wrote ${env_file}"
}

install_systemd_unit() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  cat >"${unit}" <<EOF
[Unit]
Description=EMA Pullback Trader (Binance Futures paper dashboard)
Documentation=https://github.com/${REPO_OWNER}/${REPO_NAME}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
EnvironmentFile=-/etc/default/${SERVICE_NAME}
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/${BINARY_PATH}
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

# Hardening (adjust if the binary needs write outside INSTALL_DIR)
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${INSTALL_DIR}
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
  chmod 0644 "${unit}"
  log "Wrote ${unit}"
}

configure_firewall_hint() {
  local port="${HTTP_ADDR##*:}"
  if [[ "${HTTP_ADDR}" == "${port}" ]]; then
    port="8080"
  fi
  log "Dashboard listens on ${HTTP_ADDR} (port ${port}). Open in firewall if needed, e.g.:"
  log "  ufw allow ${port}/tcp"
}

enable_service() {
  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}.service"
  systemctl restart "${SERVICE_NAME}.service"
  sleep 1
  if systemctl is-active --quiet "${SERVICE_NAME}.service"; then
    log "Service ${SERVICE_NAME} is running."
  else
    log "Service failed to start. Check: journalctl -u ${SERVICE_NAME} -n 50 --no-pager"
    systemctl status "${SERVICE_NAME}.service" || true
    exit 1
  fi
}

main() {
  require_root
  install_deps
  create_user
  download_binary
  write_env_file
  install_systemd_unit
  configure_firewall_hint
  enable_service

  local port="${HTTP_ADDR##*:}"
  [[ "${HTTP_ADDR}" == "${port}" ]] && port="8080"

  cat <<EOF

Install complete.

  Binary:  ${INSTALL_DIR}/${BINARY_PATH}
  Service: systemctl status ${SERVICE_NAME}
  Logs:    journalctl -u ${SERVICE_NAME} -f
  Config:  /etc/default/${SERVICE_NAME}
  UI:      http://<server-ip>:${port}/

Commands:
  sudo systemctl restart ${SERVICE_NAME}
  sudo systemctl stop ${SERVICE_NAME}
  sudo $0   # re-run to upgrade binary from GitHub

EOF
}

main "$@"
