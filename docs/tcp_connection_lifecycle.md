# TCP Connection Lifecycle: handle() Ownership Pattern

This document describes the complete TCP connection lifecycle in MetalBond, focusing on the handle() ownership pattern and coordinated shutdown mechanisms.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Goroutine Coordination Mechanism](#goroutine-coordination-mechanism)
3. [OUTGOING Connection Flow (Client Mode)](#outgoing-connection-flow-client-mode)
4. [INCOMING Connection Flow (Server Mode)](#incoming-connection-flow-server-mode)
5. [Shutdown Coordination](#shutdown-coordination)
6. [Error Scenarios and Recovery](#error-scenarios-and-recovery)

---

## Architecture Overview

### Coordinator Pattern

MetalBond uses a coordinator pattern where `handle()` manages the lifecycle of TCP connections and coordinates worker goroutines.

```
              ┌──────────────────┐
              │   handle()       │  ← COORDINATOR (owns connection)
              │  (Coordinator)   │
              │                  │
              │ Responsibilities:│
              │ • Create conn    │
              │ • Start workers  │
              │ • Process msgs   │
              │ • Close conn     │
              └────────┬─────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
         v             v             v
   ┌─────────┐  ┌─────────┐  ┌──────────────┐
   │ rxLoop  │  │ txLoop  │  │ keepaliveLoop│
   │(Worker) │  │(Worker) │  │  (Worker)    │
   │         │  │         │  │              │
   │ • Reads │  │ • Writes│  │ • Keepalives │
   │ • Parses│  │ • Sends │  │ • Timeouts   │
   └─────────┘  └─────────┘  └──────────────┘
```

**Key Principle:** The creator owns the lifecycle
- `handle()` creates connection → `handle()` closes connection
- Workers use connection → Workers signal when done

---

## Goroutine Coordination Mechanism

### Two-Phase Shutdown Pattern

When `Reset()` or `unsafeRemovePeer()` is called, the system uses a two-phase coordination to prevent channel deadlocks:

**Phase 1: Signal rxLoop specifically**
1. Set `stopRxLoop = true` - Flag for rxLoop's nested loops to exit
2. Sleep 1 second - Allow rxLoop to exit before handle() stops receiving

**Phase 2: Signal all workers**
3. Send channel signals - `shutdown`, `txChanClose`, `keepaliveStop`
4. Workers exit cleanly, handle() defer closes connection

```
Reset() or unsafeRemovePeer() called
    │
    ├─→ PHASE 1: stopRxLoop = true
    │             (Signal rxLoop's nested loops)
    v
Sleep 1 second  ← rxLoop exits during this time
    │             (Critical: prevents channel deadlock)
    v
    ├─→ PHASE 2: Send channel signals
    │   ├─→ shutdown ← true          (handle())
    │   ├─→ txChanClose ← true       (txLoop)
    │   └─→ keepaliveStop ← true     (keepaliveLoop)
    │
    v
All workers exit → handle() defer closes connection
```

**Rationale:** rxLoop contains a nested inner loop that processes buffered packets and sends to channels (`rxHello`, `rxKeepalive`, `rxUpdate`) that handle() receives from. If `shutdown` is sent before rxLoop exits, handle() stops receiving while rxLoop may be blocked on a channel send, creating a deadlock. The 1-second delay ensures rxLoop exits cleanly before handle() stops draining channels.

### Worker Response to Signals

Each worker responds to shutdown signals:

**rxLoop** (Lines 481, 527):
```go
// Outer loop (reading)
for {
    if p.stopRxLoop {
        return  // Exit
    }
    // Read from socket
}

// Inner loop (processing)
for {
    if p.stopRxLoop {
        return  // Exit
    }
    // Process packets
}
```

**txLoop** (Line 997):
```go
for {
    select {
    case msg := <-p.txChan:
        // Write message
    case <-p.txChanClose:
        return  // Exit
    }
}
```

**keepaliveLoop** (Line 883):
```go
for {
    select {
    case <-tckr.C:
        // Send keepalive
    case <-p.keepaliveTimer.C:
        // Timeout
    case <-p.keepaliveStop:
        return  // Exit
    }
}
```

### Safety Guarantee

**Timeline:**
- `T+0ms`: Phase 1 - stopRxLoop = true, sleep starts
- `T+0-1000ms`: rxLoop checks flag at loop iterations → exits cleanly
- `T+1000ms`: Phase 2 - channel signals sent (shutdown, txChanClose, keepaliveStop)
- `T+1000-1001ms`: txLoop, keepaliveLoop, handle() receive signals → exit
- `T+1001ms+`: handle() defer closes connection

**Result:** rxLoop exits before handle() stops receiving from channels, preventing deadlock. All workers exit cleanly, connection closed by handle() defer.

---

## OUTGOING Connection Flow (Client Mode)

### Phase 1: Creation

```
metalbond.AddPeer(addr) called
    │
    v
newMetalBondPeer(nil, addr, ..., OUTGOING, m)
    │
    ├─→ peer.conn = nil  (no connection yet)
    ├─→ peer.direction = OUTGOING
    │
    v
go peer.handle()  ← START COORDINATOR
```

### Phase 2: Connection Establishment

```
handle() starts
    │
    v
defer func() {
    // Will close connection when handle() exits
    if p.conn != nil {
        (*p.conn).Close()  ← OWNER CLEANUP
    }
}()
    │
    v
Create channels (shutdown, txChan, etc.)
    │
    v
for p.conn == nil {  ← ESTABLISH CONNECTION LOOP
    Try to connect: net.DialTCP()

    If success:
        p.conn = &conn
        break

    If failure:
        Sleep random interval
        Retry
}
    │
    v
Connection established ✓
```

### Phase 3: Normal Operation

```
Start worker goroutines:
    ├─→ go p.rxLoop()
    ├─→ go p.txLoop()
    └─→ (keepaliveLoop started after HELLO)

Send HELLO message
    │
    v
Main message processing loop:
    for {
        select {
        case msg := <-p.rxHello:
            p.processRxHello(msg)  → Start keepaliveLoop

        case msg := <-p.rxKeepalive:
            p.processRxKeepalive(msg)  → Connection ESTABLISHED

        case msg := <-p.rxUpdate:
            // Process messages

        case <-done:  ← SHUTDOWN SIGNAL
            p.cleanup()
            return  → defer closes connection
        }
    }
```

### Phase 4: Error Detection

```
Error occurs in rxLoop (read timeout, peer disconnect, etc.)
    │
    v
rxLoop: go p.Reset()  ← INITIATE RESET
    │
    v
rxLoop: return  (exits)
```

### Phase 5: Coordinated Shutdown (Reset for OUTGOING)

```
Reset() executes:
    │
    v
STEP 1: PHASE 1 - SIGNAL RXLOOP
    mtxReset.Lock()
    p.stopRxLoop = true
    time.Sleep(1 * time.Second)  ← rxLoop exits during this time
    mtxReset.Unlock()
    │
    v
STEP 2: PHASE 2 - SIGNAL OTHER WORKERS
    p.setState(RETRY)
    p.txChanClose ← true
    p.shutdown ← true
    p.keepaliveStop ← true
    │
    v
STEP 3: WAIT FOR WORKERS
    p.wg.Wait()  ← WAIT FOR ALL GOROUTINES
    (All workers exit, handle() defer closes connection)
    │
    v
STEP 4: PREPARE RECONNECT
    p.conn = nil
    p.wg = &sync.WaitGroup{}  (new instance)
    Sleep retry interval
    │
    v
STEP 5: RECONNECT
    p.setState(CONNECTING)
    go p.handle()  ← START NEW SESSION
```

### handle() defer Execution

When `handle()` exits:

```
handle() receives shutdown signal
    │
    v
handle() cleanup()
    │
    v
handle() return (exits main loop)
    │
    v
defer func() {
    if p.conn != nil {
        p.log().Debug("handle: closing TCP connection")
        err := (*p.conn).Close()  ← PRIMARY CLOSE
        if err != nil {
            p.log().Debugf("already closed: %v")
        }
    }
    p.log().Infof("handle done")
    p.wg.Done()
}()
```

---

## INCOMING Connection Flow (Server Mode)

### Phase 1: Accept Connection

```
Server listening (metalbond.StartServer)
    │
    v
for {
    conn, err := lis.Accept()  ← NEW CONNECTION
    │
    v
    p := newMetalBondPeer(
        &conn,       ← CONNECTION ALREADY EXISTS
        conn.RemoteAddr().String(),
        ...,
        INCOMING,    ← SERVER MODE
        m,
    )
    │
    v
    m.peers[addr] = p
}
```

### Phase 2: Start Coordinator

```
newMetalBondPeer(pconn, ...) called
    │
    ├─→ peer.conn = pconn  ← ALREADY SET (not nil!)
    ├─→ peer.direction = INCOMING
    │
    v
go peer.handle()  ← START COORDINATOR
```

### Phase 3: handle() Skips Connection Establishment

```
handle() starts
    │
    v
defer func() {
    if p.conn != nil {
        (*p.conn).Close()  ← OWNER CLEANUP
    }
}()
    │
    v
Create channels
    │
    v
for p.conn == nil {  ← p.conn != nil, SKIP THIS LOOP
    // Connection establishment
}
    │
    v (skipped immediately)
Start workers:
    ├─→ go p.rxLoop()
    └─→ go p.txLoop()

Note: No HELLO sent (server waits for client HELLO)
```

### Phase 4: Normal Operation

Same as OUTGOING mode:
- Wait for client HELLO → Send HELLO response → Start keepaliveLoop
- Process messages...

### Phase 5: Error Detection

```
Error occurs (client disconnect, read error, etc.)
    │
    v
rxLoop: go p.Reset()
    │
    v
rxLoop: return (exits)
```

### Phase 6: Coordinated Shutdown (Reset for INCOMING)

```
Reset() executes:
    │
    v
STEP 1: PHASE 1 - SIGNAL RXLOOP
    mtxReset.Lock()
    p.stopRxLoop = true
    time.Sleep(1 * time.Second)  ← rxLoop exits during this time
    mtxReset.Unlock()
    │
    v
STEP 2: PHASE 2 - CALL Close()
    p.Close()  ← For INCOMING
        ├→ setState(CLOSED)
        ├→ txChanClose ← true
        ├→ shutdown ← true
        └→ keepaliveStop ← true
    │
    v
STEP 3: REMOVE PEER
    m.RemovePeer(p.remoteAddr)
        → Removes from peer list

handle() receives shutdown signal → exits
handle() defer closes connection
```

### handle() defer Execution

```
handle() cleanup()
    │
    v
handle() return
    │
    v
defer func() {
    if p.conn != nil {
        (*p.conn).Close()  ← PRIMARY CLOSE
        // Closes connection when handle() exits
    }
    p.wg.Done()
}()

Peer fully cleaned up ✓
```

---

## Shutdown Coordination

### Timeline View

```
T+0ms
======
Error detected → Reset() or unsafeRemovePeer() called

PHASE 1: Signal rxLoop specifically
    p.stopRxLoop = true
    time.Sleep(1 * time.Second)  ← Critical: prevents channel deadlock

During sleep (T+0-1000ms):
    ┌──────────────────────────────────────────────────────────┐
    │  rxLoop Processing                                       │
    │  ─────────────────                                       │
    │  Outer loop iteration:                                   │
    │    - Checks stopRxLoop → sees true → returns ✓           │
    │                                                          │
    │  OR                                                      │
    │                                                          │
    │  Inner loop iteration (processing buffered packets):     │
    │    - Sends to rxUpdate/rxHello/etc. channels             │
    │    - handle() STILL receiving (no shutdown yet!)         │
    │    - Send completes successfully                         │
    │    - Checks stopRxLoop → sees true → returns ✓           │
    │                                                          │
    │  Result: rxLoop exits cleanly while handle() drains      │
    │          channels, preventing deadlock                   │
    └──────────────────────────────────────────────────────────┘

T+1000ms
=========
PHASE 2: Signal all other workers

For OUTGOING:
    p.setState(RETRY)
    p.txChanClose ← true
    p.shutdown ← true
    p.keepaliveStop ← true

For INCOMING:
    p.Close() → sends same signals

Worker responses (almost immediate):
    ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐
    │  txLoop      │  │  handle()    │  │  keepaliveLoop     │
    │              │  │              │  │                    │
    │ Sees signal  │  │ Sees signal  │  │ Sees signal        │
    │ → returns    │  │ → cleanup()  │  │ → returns          │
    │              │  │ → returns    │  │                    │
    └──────────────┘  └──────────────┘  └────────────────────┘

T+1001ms
=========
All workers exiting:
    • rxLoop already exited (during Phase 1 sleep)
    • txLoop calls wg.Done()
    • keepaliveLoop calls wg.Done()
    • handle() defer: closes connection, calls wg.Done()

T+1002ms (OUTGOING only)
==========================
Reset() wg.Wait() completes (all workers done)
Reset() prepares reconnect:
    • p.conn = nil
    • Create new WaitGroup
    • Sleep retry interval
    • go p.handle()  → New session begins

T+1001ms+ (INCOMING only)
==========================
Reset() calls m.RemovePeer()
Peer removed from list
All cleanup complete ✓
```

### Worker Exit Guarantees

| Worker | Exit Time | Guarantee |
|--------|-----------|-----------|
| **rxLoop** | < 1000ms | ✅ Exits during Phase 1 sleep by checking stopRxLoop flag |
| **keepaliveLoop** | < 1ms after Phase 2 | ✅ Exits immediately on keepaliveStop signal |
| **txLoop** | < 1ms after Phase 2 | ✅ Exits immediately on txChanClose signal |
| **handle()** | < 1ms after Phase 2 | ✅ Exits on shutdown signal, defer closes connection |

---

## Error Scenarios and Recovery

### Scenario 1: Remote Peer Disconnects

```
Remote peer closes connection
    ↓
rxLoop: Read() returns io.EOF
    ↓
rxLoop: go p.Reset()
    ↓
rxLoop: return
    ↓
Reset() executes (coordinated shutdown)
    ↓
OUTGOING: Reconnect after retry interval
INCOMING: Remove peer, close connection

Result: ✅ Clean recovery with guaranteed resource cleanup
```

### Scenario 2: Keepalive Timeout

```
No keepalive received for (keepaliveInterval * 5/2) seconds
    ↓
keepaliveTimer fires
    ↓
keepaliveLoop: go p.Reset()
    ↓
Reset() executes (coordinated shutdown)
    ↓
OUTGOING: Reconnect
INCOMING: Remove peer

Result: ✅ Stale connections detected and cleaned up
```

### Scenario 3: Write Error in txLoop

```
txLoop: Write() fails
    ↓
txLoop: go p.Reset()
    ↓
txLoop continues (doesn't exit immediately)
    ↓
Reset() signals txChanClose
    ↓
txLoop receives signal, exits
    ↓
Coordinated shutdown completes

Result: ✅ Write errors trigger clean shutdown
```

---

## Summary

The handle() ownership pattern with two-phase shutdown coordination provides:

- **Clear ownership:** handle() creates connection → handle() closes connection (via defer)
- **Deadlock prevention:** Two-phase shutdown ensures rxLoop exits before handle() stops receiving
- **Phase 1 (rxLoop-specific):** Set stopRxLoop flag, sleep 1 second for rxLoop to exit nested loops
- **Phase 2 (all workers):** Send channel signals for immediate worker shutdown
- **Safety guarantees:** No goroutine leaks, no connection leaks, no channel deadlocks
- **Works for both modes:** OUTGOING and INCOMING connections handled correctly

The two-phase coordination is critical for correctness: rxLoop's nested packet-processing loop sends to buffered channels that handle() receives from. If shutdown is signaled before rxLoop exits, handle() stops receiving while rxLoop may be blocked on a channel send, creating a deadlock. The 1-second sleep in Phase 1 ensures rxLoop exits cleanly before Phase 2 signals shut down the other workers.
