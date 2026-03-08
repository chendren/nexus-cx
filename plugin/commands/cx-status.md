---
name: cx-status
description: "Show CX Intelligence Platform status"
allowed-tools: [Bash]
---

# CX Platform Status

Check and display the Nexus CX Intelligence Platform status.

## Steps

1. **Check if server is running**:
   ```bash
   lsof -t -i:3143
   ```

2. **If running, fetch full status**:
   ```bash
   curl -s http://localhost:3143/api/status | jq .
   ```

3. **Also fetch health and dashboard**:
   ```bash
   curl -s http://localhost:3143/api/health | jq .
   curl -s http://localhost:3143/api/dashboard | jq .
   ```

## Output Format

Present as a platform systems readout:

```
  +====================================================+
  |  NEXUS CX INTELLIGENCE PLATFORM               |
  +====================================================+
  |  Status:      [ONLINE/OFFLINE]                       |
  |  Port:        3143                                   |
  |  Uptime:      [uptime from health]                   |
  |  Dashboard:   http://localhost:3143/                  |
  +----------------------------------------------------+
  |  LAYER STATUS                                        |
  |  1. Event Fabric:    [status]                        |
  |  2. Classifier:      [status]                        |
  |  3. Journey Engine:  [status]                        |
  |  4. NBA Engine:      [status]                        |
  |  5. Analytics:       [status]                        |
  +----------------------------------------------------+
  |  METRICS                                             |
  |  Events Processed:  [count]                          |
  |  Active Journeys:   [count]                          |
  |  NBA Actions:       [count]                          |
  |  Customers:         [count]                          |
  +----------------------------------------------------+
  |  CHANNELS                                            |
  |  voice: [n]  chat: [n]  web: [n]  mobile: [n]       |
  |  email: [n]  sms: [n]   whatsapp: [n]               |
  +====================================================+
```

If the server is not running, show OFFLINE status and suggest running `/cx-platform:cx-start` to start it.
