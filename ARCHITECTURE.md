# NeuDB Subsystem Pipeline & Error Handling Topology

---

## 1. System Initialization Phase (begin() / init())

This routine executes exactly once during the ESP32 hardware bootstrap sequence to guarantee cold-crash recovery and virtual file system (VFS) mount abstraction stability across Flash or MicroSD media:

```text
[STORAGE_INIT()] ──(FAIL)──> [Return FALSE (System Halts)]
       │
    (SUCCESS)
       ▼
[loadAllSST()] ───> Scans /lsm directory, filters "lvX_" patterns (Levels 0-4), and builds active RAM Indexes.
       │            *Error Handling: Corrupted or unopenable physical SST files are safely skipped.*
       ▼
[replayWAL()] ────> Performs sequential binary extraction of wal.log directly into active RAM.
       │            *Error Handling: Enforces hardware-backed CRC32 validation. Failed or truncated entries are discarded.*
       ▼
[STORAGE_OPEN] ───(FAIL)──> [Return FALSE] (Aborts if active WAL append handle cannot be mounted)
       │
    (SUCCESS)
       ▼
[xTaskCreate] ────> Spawns asynchronous "LSM_Task" daemon pinned exclusively to CPU Core 1.
```

---

## 2. Data Ingestion Phase (put())

The low-latency operational write-path executed concurrently by the main application thread loop, guarded by a centralized ingestion gate:

```text
[Acquire Mutex] ──(Timeout > 1000ms)──> Logs "[ERROR] Lock timeout" ──> [Return FALSE]
       │
    (SUCCESS)
       ▼
[Smart Stall] ────> Is _compactState == MERGE_STREAM?
       │                   ├── (YES) ──> Releases Mutex ──> Forces 4ms Dynamic Brake ──> Re-acquire Mutex
       │                   └── (NO) ───> Proceed to Ingestion Guards
       ▼
[Range Guard] ────(key >= KEY_SPACE_LIMIT)──> [Return FALSE (Hard Abort)]
       │
    (SUCCESS)
       ▼
[Full Guard] ─────(_flashFullGuard OR _totalEntryCount >= MAX_TOTAL_ENTRIES)──> Is Eviction Policy Active?
       │                                                                       ├── (YES) ──> [evictOldestData()]
       │                                                                       └── (NO) ───> [Return FALSE]
       ▼
[appendWAL()] ────(Adaptive 10x Retry / 2ms Backoff Fail)─────────> [Return FALSE]
       │
    (SUCCESS to Append-Only Log)
       ▼
[Update MemTable] ─> Queries target Key inside volatile RAM std::map.
       │             ├── Found     : Overwrites data payload, tracking size metrics and updated timestamp.
       │             └── New Entry : Allocates record descriptor, increments _memCount & _totalEntryCount (Atomic).
       ▼
[Release Mutex] ──> [Return TRUE] (Transaction committed instantly via low-latency RAM layer)
```

---

## 3. Background Synchronization Phase (Asynchronous tick() Daemon)

The FreeRTOS Core 1 scheduler slices execution loops to monitor structural flush boundaries without stalling the primary Core 0 write pathway:

### A. Write-Ahead Log Hard-Serialization (flushWAL)

```text
[Interval Check] ──(Delta < 200ms)──> Bypasses routine, proceeds to memory capacity audit.
       │
    (Delta >= 200ms)
       ▼
[_walFile.flush()] ─> Forces Virtual File System (VFS) to serialize RAM WAL blocks into physical storage sectors.
                    *Transaction persistence state is mathematically guaranteed past this critical boundary.*
```

### B. Volatile Memory Serialization (flush)

```text
[Boundary Check] ──(_memCount < Max AND _memBytes < Active Limit)──> Bypasses routine, proceeds to Compaction.
       │
    (Threshold Breached)
       ▼
[Acquire Mutex] ──(Lock Contention / Busy)──> Skips block gracefully, retries on subsequent tick() invocation.
       │
    (SUCCESS)
       ▼
[writeSST()] ───(Physical Block I/O Error)─> Logs "[FLUSH] ERROR" ──> Releases Mutex ──> Aborts routine.
       │
    (SUCCESS: Level 0 physical .sst generated)
       ▼
[RAM Reset] ────> Clears std::map heap blocks, resets _memBytes metric, sets _memCount = 0 (Atomic).
       │
[clearWAL()] ───> Truncates old log handle ──> STORAGE_REMOVE() ──> Remounts a fresh append WAL file descriptor.
       │
[Release Mutex] ──> Cycle complete.
```

---

## 4. Layer Consolidation Phase (tickCompact())

When Level 0 triggers structural file limits, the compaction engine executes incremental streaming merges down to Level 4 to strictly bound heap allocation ceilings:

```text
[State == MERGE_STREAM] ───> Initializes Min-Heap (Priority Queue) with base stream records from active readers.
            │
            ▼
[Bounded Block Loop] ──────> Extracts Top Heap element (Lowest Key) ──> Isolates optimal version (Highest空ts).
            │                *Error Handling: If readValue() hits I/O payload corruption, readers advance and skip.*
            │
            ▼
[Write to .tmp File] ──────> Streams deduplicated winning record metrics progressively down to a temporary VFS asset.
            │
            ▼
[Are All Stream Readers EOF?]
            ├── (NO)  ───> Yields tickCompact() context block, deferring to the next FreeRTOS cycle (Saves CPU time slice).
            └── (YES) ───> COMPACTION FINALIZATION:
                           1. Disposes and closes all active file stream reader resources.
                           2. Invokes STORAGE_RENAME(.tmp asset transformed into authentic permanent .sst).
                           3. *Error Handling: On rename fault, purges .tmp residue, aborts transaction level shifting.*
                           4. Executes sequential binary scan on new SST block to build active RAM Index & Bloom Filter.
                           5. Invokes [deleteSSTFiles()] ──> Physically unlinks stale parent SST targets from storage.
                           6. Reverts compaction machine state = IDLE.
```

---

## Architectural Error Resilience Conclusions

1. **Concurrency Isolation & Deadlock Prevention**: If the primary application thread (`put()`) initiates high-speed data ingestion while the background daemon thread (`flush()`/`compaction`) is busy constraining physical VFS boundaries, the write-path utilizes an **Adaptive Ingestion Ingestion Brake**. If the system is merging streams, it applies a 4ms dynamic brake, and caps lock contention at 1000ms. This dual-layer architecture completely averts thread starvation and resource exhaustion [1, 2].

2. **Crash & Blackout Immunity**: If a sudden power loss occurs mid-compaction, the unindexed `.tmp` block remains completely isolated on the storage device and is safely ignored by `begin()` during boot verification. Legacy `.sst` parent records are exclusively unlinked and deleted from the VFS directory table _only after_ the new combined SST block is fully verified, successfully reindexed, and registered into the active level topology layout. This guarantees ACID-like atomic durability boundaries on bare-metal systems.
