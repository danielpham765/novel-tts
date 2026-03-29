# On-Premise Redis Port Forward

Forwards `127.0.0.1:6379` to `on-premise:6379` (resolved via `/etc/hosts` to `192.168.2.1`).

## Files

| File | Purpose |
|------|---------|
| `scripts/on_premise_portforward.py` | Forwarder process |
| `scripts/com.codex.portforward.redis.onprem.plist` | LaunchAgent definition (source of truth) |
| `~/Library/LaunchAgents/com.codex.portforward.redis.onprem.plist` | Installed copy macOS reads |
| `/etc/hosts` | Maps `on-premise` → `192.168.2.1` |

## Usage

Any tool connecting to `127.0.0.1:6379` is automatically forwarded:

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
uv run novel-tts queue ...
```

## Managing the Service

```bash
# Status (PID + last exit code)
launchctl list | grep portforward

# Stop
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.codex.portforward.redis.onprem.plist

# Start / Restart
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.codex.portforward.redis.onprem.plist

# Logs
tail -f /tmp/com.codex.portforward.redis.onprem.err.log
```

The service starts automatically at login and restarts on crash.

## Updating

**If the on-premise IP changes:**
1. Edit `/etc/hosts` (needs `sudo`) — update the `on-premise` entry
2. Restart the service (bootout + bootstrap above)

**If the script or plist changes:**
```bash
cp scripts/com.codex.portforward.redis.onprem.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.codex.portforward.redis.onprem.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.codex.portforward.redis.onprem.plist
```
