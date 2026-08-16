#!/usr/bin/env bash
# Run devtools-mcp as a single shared local service (MCP over HTTP + dashboard).
#
# The macOS/Linux twin of devtools-service.ps1, same verb vocabulary:
#
#   ./scripts/devtools-service.sh start       # launch detached (idempotent)
#   ./scripts/devtools-service.sh status      # is it up? what URLs?
#   ./scripts/devtools-service.sh stop
#   ./scripts/devtools-service.sh restart
#   ./scripts/devtools-service.sh install     # register as a launchd service
#   ./scripts/devtools-service.sh uninstall
#   ./scripts/devtools-service.sh logs        # tail the server log
#
# One instance serves every project: point Claude Code / Cursor at
# http://127.0.0.1:$PORT/mcp and open the dashboard at http://127.0.0.1:$DASH.
#
# WHY A LaunchAgent AND NOT A LaunchDaemon: the server owns ~/.devtools-mcp
# (tracker.db, runs.db, stored profiles) and shells out to the user's uv and
# toolchain. A LaunchDaemon runs as root before login and would create those
# files root-owned, breaking every subsequent user-run invocation. The Agent
# starts at login instead of at boot — on a single-user desktop that is the
# same thing in practice, and it is the difference between a service that
# works and one that silently corrupts its own state directory.

set -euo pipefail

PORT="${DEVTOOLS_MCP_PORT:-8010}"
DASH="${DEVTOOLS_MCP_DASHBOARD_PORT:-8765}"
LABEL="com.devtools-mcp"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="${HOME}/.devtools-mcp"
LOG="${STATE}/server.log"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
UV="$(command -v uv || true)"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# A plain GET to a streamable-http MCP endpoint is 406 Not Acceptable BY
# DESIGN, so any status < 500 means alive. Matching the .ps1's note.
mcp_up() {
    local code
    code="$(curl -s -m 2 -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT}/mcp" || echo 000)"
    [ "$code" != "000" ] && [ "$code" -lt 500 ]
}
dash_up() {
    [ "$(curl -s -m 2 -o /dev/null -w '%{http_code}' "http://127.0.0.1:${DASH}/health" || echo 000)" = "200" ]
}

wait_up() {
    local n=0
    while [ "$n" -lt 30 ]; do
        if dash_up && mcp_up; then return 0; fi
        sleep 1; n=$((n + 1))
    done
    return 1
}

cmd_status() {
    local m d
    m=$(mcp_up && echo up || echo DOWN)
    d=$(dash_up && echo up || echo DOWN)
    printf 'mcp        %-4s  http://127.0.0.1:%s/mcp\n' "$m" "$PORT"
    printf 'dashboard  %-4s  http://127.0.0.1:%s  (tracker: /tracker)\n' "$d" "$DASH"
    if [ -f "$PLIST" ]; then
        if launchctl print "gui/$(id -u)/${LABEL}" >/dev/null 2>&1; then
            printf 'launchd    loaded (%s) — starts at login\n' "$LABEL"
        else
            printf 'launchd    plist present but NOT loaded — run: %s install\n' "$0"
        fi
    else
        printf 'launchd    not installed — run: %s install\n' "$0"
    fi
    [ "$m" = up ] && [ "$d" = up ]
}

cmd_start() {
    if mcp_up && dash_up; then echo "already running"; cmd_status; return 0; fi
    [ -n "$UV" ] || die "uv not found on PATH"
    mkdir -p "$STATE"
    nohup "$UV" run --directory "$REPO" devtools-mcp \
        --transport http --host 127.0.0.1 --port "$PORT" \
        --dashboard --dashboard-port "$DASH" >>"$LOG" 2>&1 &
    wait_up || die "did not come up within 30s; see $LOG"
    echo "started"; cmd_status
}

cmd_stop() {
    # Only ever kill OUR listener: match the full argv, not a bare 'devtools'.
    pkill -f "devtools-mcp --transport http --host 127.0.0.1 --port ${PORT}" 2>/dev/null || true
    sleep 1
    if mcp_up; then die "still up — a launchd job may be restarting it; try: $0 uninstall"; fi
    echo "stopped"
}

cmd_install() {
    [ -n "$UV" ] || die "uv not found on PATH"
    mkdir -p "$STATE" "$(dirname "$PLIST")"
    cat >"$PLIST" <<PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${UV}</string>
    <string>run</string>
    <string>--directory</string><string>${REPO}</string>
    <string>devtools-mcp</string>
    <string>--transport</string><string>http</string>
    <string>--host</string><string>127.0.0.1</string>
    <string>--port</string><string>${PORT}</string>
    <string>--dashboard</string>
    <string>--dashboard-port</string><string>${DASH}</string>
  </array>
  <key>WorkingDirectory</key><string>${REPO}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>${LOG}</string>
  <key>StandardErrorPath</key><string>${LOG}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>$(dirname "$UV"):/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>HOME</key><string>${HOME}</string>
  </dict>
</dict>
</plist>
PLIST_EOF
    plutil -lint "$PLIST" >/dev/null || die "generated plist is malformed"
    # Stop any hand-started instance first, or launchd's copy loses the port.
    pkill -f "devtools-mcp --transport http --host 127.0.0.1 --port ${PORT}" 2>/dev/null || true
    sleep 1
    launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    wait_up || die "launchd job did not come up within 30s; see $LOG"
    echo "installed and running: $PLIST"
    cmd_status
}

cmd_uninstall() {
    launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    rm -f "$PLIST"
    echo "uninstalled (plist removed, job unloaded)"
}

cmd_restart() {
    if [ -f "$PLIST" ] && launchctl print "gui/$(id -u)/${LABEL}" >/dev/null 2>&1; then
        launchctl kickstart -k "gui/$(id -u)/${LABEL}"
        wait_up || die "did not come back within 30s; see $LOG"
        echo "restarted (launchd)"; cmd_status
    else
        cmd_stop || true
        cmd_start
    fi
}

case "${1:-start}" in
    start)     cmd_start ;;
    stop)      cmd_stop ;;
    restart)   cmd_restart ;;
    status)    cmd_status ;;
    install)   cmd_install ;;
    uninstall) cmd_uninstall ;;
    logs)      tail -f "$LOG" ;;
    *) die "unknown action '${1}'; one of: start stop restart status install uninstall logs" ;;
esac
