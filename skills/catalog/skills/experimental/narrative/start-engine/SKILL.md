---
name: start-engine
description: Start the Story Engine. Handles port conflicts, Java detection via SDKMAN, and database setup automatically.
user_invocable: true
---

# Start Story Engine

Start the Story Engine with automatic conflict resolution. `start.sh` is the single entry point.

## Steps

1. Check if the backend is already running:
   ```bash
   curl -s http://localhost:9876/health 2>/dev/null && echo "Already running" || echo "Not running"
   ```

2. If not running, start it:
   ```bash
   # Full stack (Quarkus + Vite via Quinoa) — default
   cd /Users/bengamble/Story-engine-3 && ./start.sh

   # Backend only (REST API + SSE MCP)
   cd /Users/bengamble/Story-engine-3 && ./start.sh backend
   ```

## Port Override

If ports conflict, set environment variables or pass flags:
```bash
QUARKUS_HTTP_PORT=9877 ./start.sh
./start.sh --backend-port 9877 --frontend-port 3003
```

## Verify

After startup, confirm services are healthy:
```bash
curl -s http://localhost:9876/health | python3 -m json.tool
```
