# NeuDB v2.0.0 Subsystem Pipeline & Error Handling Topology

This document details the low-level execution architecture, thread isolation boundaries, multi-core scheduling maps, and transactional fault-tolerance protocols engineered into the NeuDB storage engine.

---

## 1. System Initialization Phase (init())

This routine executes exactly once during the ESP32 hardware bootstrap sequence to guarantee cold-crash recovery and virtual file system (VFS) mount abstraction stability across Flash (LittleFS) or MicroSD media partitions:

```text
[STORAGE_INIT()] ──(FAIL)──> [Return FALSE (System Halts)]
       │
    (SUCCESS)
       ▼
[loadAllSST()] ───> Scans /lsm directory, filters regular table structures, and builds active RAM Indexes.
       │            *Resilience: Corrupted or unopenable physical standard SST files are safely bypassed.*
       ▼
[loadAllSSTLog()] ─> Scans log partition tracks, filters "log_lvX_" high-address patterns, and builds RAM log tables.
       │            *Resilience: Enforces isolated storage track verification at the high-address register boundary.*
       ▼
[replayWAL()] ────> Performs sequential binary extraction of wal.log data frames directly into the active MemTable.
       │            *Resilience: Enforces hardware CRC32 validation. Failed, trailing, or truncated entries are discarded.*
       ▼
[STORAGE_OPEN] ───(FAIL)──> [Return FALSE] (Aborts if the operational active WAL append file handle cannot be mounted)
       │
    (SUCCESS)
       ▼
[xTaskCreate] ────> Spawns the asynchronous "LSM_Task" daemon pinned exclusively to physical CPU Core 1.
```

---

## 2. Data Ingestion Pathways (put() / putLog())

NeuDB v2.0.0 features a strict **Twin-Engine Storage Pipeline** separating regular point-in-time transactions from massive sequential telemetric log tracking. Both ingest paths are completely non-blocking, thread-safe, and reentrant across FreeRTOS task contexts [1, 2].

### A. Regular Key-Value Ingestion Pipeline (`put`)

```text
[Acquire Mutex] ──(Timeout > 1000ms)──> Emits Lock Timeout Diagnostic ──> [Return FALSE]
       │
    (SUCCESS)
       ▼
[Smart Stall] ────> Is _compactState == MERGE_STREAM?
       │                   ├── (YES) ──> Temporarily Releases Mutex ──> Forces 4ms Dynamic Brake ──> Re-acquire Mutex
       │                   └── (NO) ───> Proceed to Ingestion Constraints Boundary Checks
       ▼
[Range Guard] ────(key >= NEU_KEY_SPACE_LIMIT)──> [Return FALSE (Hard Ingestion Abort)]
       │
    (SUCCESS)
       ▼
[Full Guard] ─────(_flashFullGuard OR _totalEntryCount >= MAX_TOTAL_ENTRIES)──> Is Overwrite Policy Active?
       │                                                                       ├── (YES) ──> Invoke [evictOldestData()]
       │                                                                       └── (NO) ───> [Return FALSE]
       ▼
[appendWAL()] ────(Adaptive 10x Retry / 2ms Backoff Fail Sequence)────────> [Return FALSE]
       │
    (SUCCESS to Append-Only Log)
       ▼
[Update MemTable] ─> Queries target 16-bit key inside volatile RAM std::map.
       │             ├── Found     : Overwrites data payload, balancing size metrics and dynamic updates timestamp.
       │             └── New Entry : Allocates entry descriptor node, increments counters atomically (__ATOMIC_SEQ_CST).
       ▼
[Release Mutex] ──> [Return TRUE] (Transaction committed instantly via low-latency volatile RAM layer)
```

### B. Twin-Engine Historical Log Ingestion Pipeline (`putLog`)

```text
[Acquire Mutex] ──(Timeout > 1000ms)──> Emits Lock Timeout Diagnostic ──> [Return FALSE]
       │
    (SUCCESS)
       ▼
[Log Stall] ──────> Is _compactLogState == MERGE_STREAM?
       │                   ├── (YES) ──> Temporarily Releases Mutex ──> Forces 4ms Dynamic Brake ──> Re-acquire Mutex
       │                   └── (NO) ───> Proceed to Boundary Checks
       ▼
[Boundary Guard] ─(id >= NEU_LOG_MAX_ID_LIMIT OR size > 65535)──> [Return FALSE (Hard Ingestion Abort)]
       │
    (SUCCESS)
       ▼
[Full Guard] ─────(_flashFullGuard OR _totalEntryCount >= MAX_TOTAL_ENTRIES)──> Is Overwrite Policy Active?
       │                                                                       ├── (YES) ──> Invoke [evictOldestData()]
       │                                                                       └── (NO) ───> [Return FALSE]
       ▼
[Index Resolution] ─> Invokes [findLatestLogIndex()] to resolve the latest active rolling index position.
       │             ├── Match Found : Computes next index coordinate: nextIndex = (lastIndex + 1) % NEU_LOG_MAX_INDEX.
       │             └── Key Miss    : Enforces baseline initialization parameter coordinate position: nextIndex = 0.
       ▼
[Bitwise Packing] ──> Maps ID and 14-bit index using arithmetic addition anchored at the 32-bit ceiling register offset:
       │             virtualKey = NEU_LOG_KEY_OFFSET + ((uint32_t)id << NEU_LOG_INDEX_BITS) + (uint32_t)nextIndex.
       ▼
[appendWAL()] ────(Adaptive 10x Retry / 2ms Backoff Fail Sequence)────────> [Return FALSE]
       │
    (SUCCESS to Append-Only Log)
       ▼
[RAM Injection] ──> Injects packed virtual key frame seamlessly into the active shared RAM MemTable map.
       │            *Zero-Collision Tracking: Elements scale safely inside the isolated high-address virtual pool.*
       ▼
[Release Mutex] ──> [Return TRUE] (Historical log track entry committed instantly to low-latency cache)
```

---

## 3. Background Synchronization Phase (Asynchronous tick() Daemon)

The FreeRTOS Core 1 scheduler slices execution loops to monitor structural flush boundaries without stalling the primary CPU Core 0 transaction pipeline pathways [1, 2]:

### A. Write-Ahead Log Hard-Serialization (flushWAL)

```text
[Interval Check] ──(Delta < 200ms)──> Bypasses routine, proceeds to dynamic threshold memory cluster audit.
       │
    (Delta >= 200ms)
       ▼
[_walFile.flush()] ─> Forces Virtual File System (VFS) to hard-serialize volatile RAM WAL blocks into physical flash sectors.
                    *Transaction persistence and ACID durability parameters are guaranteed past this critical boundary.*
```

### B. Volatile Memory Serialization & Data Splitting (flush)

```text
[Boundary Check] ──(_memCount < Max AND _memBytes < Active Adaptive Limit)──> Bypasses routine, proceeds to Compaction.
       │
    (Threshold Breached / Absolute Flush Invoked)
       ▼
[Acquire Mutex] ──(Lock Contention / Core 0 Active Ingestion)──> Skips block gracefully, defers to the next tick() tick cycle.
       │
    (SUCCESS)
       ▼
[Data Splitter] ──> Traverses active MemTable, parsing coordinates against NEU_LOG_KEY_OFFSET to build split pipelines.
       │            *Zero-Copy Move Protocol: Utilizes std::move pointers to cleanly bifurcate regular maps from log drops.*
       ▼
[Dual Persistence] ─> Commits split chunks to non-volatile disk concurrently via dedicated low-level block writers:
       │             ├── Regular Array Track ──> Invoke [writeSST(level=0, regularEntries)]
       │             └── Isolated Log Track  ──> Invoke [writeSSTLog(level=0, logEntries)]
       │
    (SUCCESS: Level 0 physical files generated successfully on storage disk)
       ▼
[RAM Map Reset] ──> Clears map tree nodes, clears volatile heap footprints, sets metrics counters = 0.
       │
[clearWAL()] ───> Truncates old transactional WAL handle ──> STORAGE_REMOVE() ──> Remounts a fresh write WAL file descriptor.
       │
[Release Mutex] ──> Ingestion pipeline pipeline synchronization loop complete.
```

---

## 4. Layer Consolidation & Rolling Pruning Phase (tickCompact() / tickCompactLog())

When Level 0 triggers structural density parameters, the background compaction worker thread on CPU Core 1 handles K-way merge-sort consolidations progressively down to deep tiers to bound memory allocations [1, 2]:

```text
[State == MERGE_STREAM] ───> Initializes Min-Heap (Priority Queue) with base stream index metrics from active file readers.
            │
            ▼
[Bounded Block Loop] ──────> Extracts Top Heap element (Lowest Key coordinate array pointer matching transaction tree).
            │                *Deduplication Path: Intercepts multi-version duplicates, electing highest validation timestamps.*
            │
            ▼
[Log Rolling Pruner] ───> Is the active target key an isolated log entity structure?
            │             ├── (YES) ──> Evaluates historical track limits against NEU_MAX_LOG_HISTORY makro rules.
            │             │             *Pruning: Trailing slots that breach history bounds are discarded from disk permanently.*
            │             └── (NO) ───> Proceeds directly to non-volatile serialization layers.
            │
            ▼
[Write to .tmp File] ──────> Streams deduplicated winning record metrics progressively down to a temporary VFS asset.
            │
            ▼
[Are All Stream Readers EOF?]
            ├── (NO)  ───> Yields tickCompact() context block, deferring execution to the next FreeRTOS scheduler cycle (Saves CPU time).
            └── (YES) ───> COMPACTION FINALIZATION RELEASE ROUTINE:
                           1. Disposes and closes all active file stream reader resources securely.
                           2. Invokes STORAGE_RENAME(.tmp asset transformed into authentic permanent .sst table blocks).
                           3. *Resilience: On rename fault, purges .tmp residue instantly and aborts transaction level shifting.*
                           4. Executes sequential binary scan on new SST block to build active RAM Index & hydration Bloom Filter.
                           5. Invokes [internalDeleteSST()] ──> Physically unlinks stale parent file targets from storage clusters.
                           6. Reverts compaction machine state = IDLE.
```

## Architectural Error Resilience Conclusions

1. **Multi-Thread Isolation & Deadlock Defasement**: If the primary application transaction loops (put() / putLog()) trigger explosive throughput on CPU Core 0 while the background worker thread (flush() / compactions) is busy altering VFS metadata blocks on CPU Core 1, thread starvation and race conditions are averted via Smart Ingestion Write Stalls. The pipeline applies a 4ms adaptive CPU yield delay and limits maximum lock contention to 1000ms. This keeps runtime operations completely lock-free and crash-resistant.

2. **Power Failure & Blackout Immunity**: If a sudden power cutout strikes mid-compaction, un-indexed .tmp asset blocks remain completely isolated on the disk and are bypassed by init() during subsequent boot sequences. Legacy .sst parent data blocks are unlinked from the VFS directory table only after the new consolidated block table is fully verified, successfully re-indexed, and populated into the active level array. This delivers true atomic ACID durability boundaries on bare-metal architectures.
