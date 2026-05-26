# NeuDB Subsystem Pipeline & Error Handling Topology

---

## 1. System Initialization Phase (begin() / init())

This routine executes exactly once during the ESP32 hardware bootstrap sequence:

```text
[LittleFS.begin] ──(FAIL)──> [Return FALSE (System Halts)]
       │
    (SUCCESS)
       ▼
[loadAllSST()] ───> Scans /lsm directory, filters "lvX_" patterns, reconstructs RAM Index & Bloom Filters.
       │            *Error Handling: Corrupted or unopenable physical SST files are safely skipped.*
       ▼
[replayWAL()] ────> Performs sequential binary extraction of wal.log directly into active RAM.
       │            *Error Handling: Enforces CRC32 validation. Failed or truncated entries are discarded.*
       ▼
[LittleFS.open] ──(FAIL)──> [Return FALSE] (Aborts if active WAL append handle cannot be mounted)
       │
    (SUCCESS)
       ▼
[xTaskCreate] ────> Spawns asynchronous "LSM_Task" daemon pinned exclusively to CPU Core 1.
```

---

## 2. Data Ingestion Phase (put())

The operational write-path executed concurrently by the main application thread loop:

```text
[Acquire Mutex] ──(Timeout > 1000ms)──> Logs "[ERROR] Lock timeout" ──> [Return FALSE]
       │
    (SUCCESS)
       ▼
[Capacity Check] ──(>= 2048 Entries)──> Is Eviction Override Policy Active?
       │                                       ├── (YES) ──> Executes [evictOldestData()] -> Commits Tombstone
       │                                       └── (NO) ───> Logs "[SYSTEM] Rejected" ───> [Return FALSE]
       ▼
[appendWAL()] ────(Buffer Write Fail)─────────> [Return FALSE]
       │
    (SUCCESS to RAM Buffer Chunk)
       ▼
[Update MemTable] ─> Queries target Key inside volatile RAM std::map.
       │             ├── Found     : Overwrites data payload, tracking size metrics and updated timestamp.
       │             └── New Entry : Allocates record descriptor, increments _memCount & _totalEntryCount (Atomic).
       ▼
[Release Mutex] ──> [Return TRUE] (Transaction committed instantly via low-latency RAM layer)
```

---

## 3. Background Synchronization Phase (Asynchronous tick() Daemon)

FreeRTOS Core 1 scheduler slices execution loops to monitor structural flush boundaries:

### A. Write-Ahead Log Hard-Serialization (flushWAL)

```text
[Interval Check] ──(Delta < 200ms)──> Bypasses routine, proceeds to memory capacity audit.
       │
    (Delta >= 200ms)
       ▼
[_walFile.flush()] ─> Forces Virtual File System (VFS) to serialize RAM WAL blocks into physical SPI Flash sectors.
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
[writeSST()] ───(Flash Physical I/O Error)─> Logs "[FLUSH] ERROR" ──> Releases Mutex ──> Aborts routine.
       │
    (SUCCESS: Level 0 physical .sst generated)
       ▼
[RAM Reset] ────> Clears std::map heap blocks, resets _memBytes metric, sets _memCount = 0 (Atomic).
       │
[clearWAL()] ───> Truncates old log handle ──> LittleFS.remove() ──> Remounts a fresh append WAL file descriptor.
       │
[Release Mutex] ──> Cycle complete.
```

---

## 4. Layer Consolidation Phase (tickCompact())

When Level 0 triggers structural file limits, the compaction engine executes incremental streaming to bound RAM overhead:

```text
[State == MERGE_STREAM] ───> Initializes Min-Heap (Priority Queue) with base stream records from active readers.
            │
            ▼
[Bounded Block Loop (16KB)] ─> Extracts Top Heap element (Lowest Key) ──> Isolates optimal version (Highest Timestamp).
            │                  *Error Handling: If readValue() hits I/O payload corruption, readers advance and skip.*
            │
            ▼
[Write to .tmp File] ──────> Streams deduplicated winning record metrics progressively down to a temporary Flash file.
            │
            ▼
[Are All Stream Readers EOF?]
            ├── (NO)  ───> Yields tickCompact() context block, deferring to the next FreeRTOS cycle (Saves CPU time slice).
            └── (YES) ───> COMPACTION FINALIZATION:
                           1. Disposes and closes all active file stream reader resources.
                           2. Invokes LittleFS.rename(.tmp asset transformed into authentic permanent .sst).
                           3. *Error Handling: On rename fault, purges .tmp residue, aborts transaction level shifting.*
                           4. Executes sequential binary scan on new SST block to build active RAM Index & Bloom Filter.
                           5. Invokes [deleteSSTFiles()] ──> Physically unlinks stale parent SST targets from Flash.
                           6. Reverts compaction machine state = IDLE.
```

---

## Architectural Error Resilience Conclusions

1. Concurrency Isolation: If the primary application thread (`put()`) initiates data ingestion while the background daemon thread (`flush()`/`compaction`) is busy constraining Flash boundaries, the write-path waits for a maximum safety buffer of 1000ms before returning a structural `Lock Timeout`, completely averting deadlocks.

2. Crash & Blackout Immunity: If a sudden power loss occurs mid-compaction, the unindexed `.tmp` block remains completely isolated and is safely ignored by `begin()` during boot verification. Legacy `.sst` parent records are only unlinked and deleted _after_ the new combined SST block is verified, successfully reindexed, and registered into the active topology layout.
