---
name: cx-start
description: "Start the CX Intelligence Platform server"
allowed-tools: [Bash]
---

# Start CX Platform

Start the Nexus CX Intelligence Platform server.

## Steps

1. **Check if already running**:
   ```bash
   lsof -t -i:3143
   ```
   If a PID is returned, the server is already running. Report this and skip startup.

2. **Start the server**:
   ```bash
   cd ~/omnichannel-cx-platform && node server.js &
   ```

3. **Wait for startup** (2 seconds), then verify:
   ```bash
   sleep 2
   curl -s http://localhost:3143/api/health | jq .
   ```

4. **Display startup confirmation**:

```
  +====================================================+
  |  NEXUS CX INTELLIGENCE PLATFORM               |
  +====================================================+
  |  Status:    ONLINE                                   |
  |  Port:      3143                                     |
  |  Dashboard: http://localhost:3143/                    |
  |  API:       http://localhost:3143/api                 |
  |  WebSocket: ws://localhost:3143/                      |
  +----------------------------------------------------+
  |  Layers:                                             |
  |    1. Event Fabric      -- Kinesis (local)           |
  |    2. Classifier        -- TF-IDF/Cosine             |
  |    3. Journey Engine    -- State Machine             |
  |    4. NBA Engine        -- Rules + ML Scoring        |
  |    5. Analytics         -- Real-time Metrics         |
  +====================================================+
```

If startup fails, check for:
- Port 3143 already in use by another process
- Missing node_modules (run `npm install` in ~/omnichannel-cx-platform)
- Node.js version issues
