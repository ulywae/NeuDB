# NeuDB

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform: Arduino](https://img.shields.io/badge/Platform-Arduino-00878F?logo=arduino&logoColor=white)](https://arduino.cc)
[![Platform: ESP32](https://img.shields.io/badge/Platform-ESP32-blue?logo=espressif&logoColor=white)](https://espressif.com)

> An Industrial-Grade, Ultra-Lean 16-Bit LSM-Tree Storage Engine with Power-Fail Safety for ESP32.

NeuDB (**NeuLSMDB_FS**) is a high-performance, asynchronous Log-Structured Merge-tree (LSM-Tree) storage engine built from scratch for the **ESP32** architecture using the Arduino framework and FreeRTOS. It is meticulously engineered to handle write-heavy logging workloads (e.g., continuous telemetry or sensor logs) while aggressively reducing flash write amplification, enforcing strict capacity guards, and extending Silicon lifespan under LittleFS.

---

## Key Architectural Features

- **True 16-Bit LSM-Tree Architecture:** Implements a structured data pipeline consisting of an active RAM MemTable and multi-level SSTables (up to 4 levels) in Flash storage, indexing up to `65,536` unique keys for ultra-fast write ingestions.
- **Asynchronous Incremental Compaction:** Driven by a FreeRTOS background scheduler thread pinned to CPU Core 1. It utilizes a Min-Heap (`std::priority_queue`) to stream-merge, deduplicate stale entries, and push finalized sorted data to deeper levels without triggering CPU starvation or Watchdog (WDT) resets.
- **Microsecond Read Path Performance:** Harnesses the combined power of hardware-backed **Dynamic Bloom Filters** (64 bytes with 4 hash counts) to intercept cache misses early, alongside **Binary Search (`std::lower_bound`)** on sorted in-memory index arrays to slash physical read latencies.
- **RAM Block Cache Layer:** Caches hot data record offsets dynamically into RAM memory blocks, bypassing heavy file-seek operations and `LittleFS.open()` overhead on subsequent reads.
- **Industrial Power-Fail Safe Recovery:** Features robust Write-Ahead Logging (WAL) packed with hardware-backed CRC32-LE checksums. Automatically detects dirty shutdowns and executes an atomic _WAL Replay_ during boot initialization to rescue uncommitted data safely.
- **Hard Storage Partition Guard:** Implements an automated resource monitoring system that flags a global `_flashFullGuard` when LittleFS usage touches 90%. Safely switches the write path into an adaptive cache eviction mode to prevent file corruption from disk saturation.
- **Adaptive Memory Profiling:** Implements an intelligent `tuneMemtable()` routine that monitors runtime Heap ratios, write pressure, and Level 0 file stack metrics to adaptively throttle MemTable size limits between 1KB and 8KB.
- **Clean Facade Interface:** Completely hides complex internal STL containers (`std::map`, `std::vector`, `std::list`), abstract structs, and FreeRTOS primitive objects using a Pimpl-based `void*` wrapper interface. Zero header pollution.

---

## Quick Start

### 1. Developer-Friendly API (No `&` or `sizeof` required)

NeuDB comes equipped with template helper wrappers, allowing you to read and write variables or packed structures without passing references and memory sizes manually using full 16-bit key ranges.

```cpp
#include <NeuDB.h>

// Struct packed ensures dense storage alignment without compiler padding bytes
struct __attribute__((packed)) TelemetryData {
    uint32_t counter;
    float temperature;
    float humidity;
};

void setup() {
    Serial.begin(115200);

    // Mounts LittleFS, loads topological metadata, and runs WAL recovery if crashed
    if (db.begin()) {
        Serial.println("NeuDB Core Subsystem Initialized successfully.");
    }
}

void loop() {
    static uint32_t txCount = 0;
    txCount++;

    // 1. Write Data Instantly (Zero heap churn template write into 16-bit key space)
    TelemetryData logNode = { txCount, 26.5f, 74.2f };
    db.putVar(txCount % 100 + 1, logNode);

    // 2. Read Data Directly
    TelemetryData dataBuffer;
    if (db.getVar(txCount % 100 + 1, dataBuffer)) {
        Serial.printf("Recovered Data -> Count: %d | Temp: %.2f\n", dataBuffer.counter, dataBuffer.temperature);
    }

    // 3. Native String Storage Support on High-Address Keys
    db.putString(1500, "NeuDB Mode PC Badas!");
    String message = db.getString(1500);

    vTaskDelay(pdMS_TO_TICKS(5)); // Yield CPU time slice to background compaction daemon
}
```

---

## Top-Level API Reference

### Core Methods

- `bool begin()` / `init()`: Triggers the structural bootstrap pipeline.
- `bool putVar(uint16_t key, const T &value)`: Template utility to ingest any primitive variable or custom struct directly into the MemTable.
- `bool getVar(uint16_t key, T &out)`: Template utility to fetch data records safely into a target variable scope.
- `bool putString(uint16_t key, const String &str)`: Dynamically handles Arduino String type serialization.
- `String getString(uint16_t key)`: Extracts a stored string record. Returns an empty string if the key profile is missing.
- `void flush()`: Forces immediate serialization of the volatile RAM MemTable down to a Level 0 SST physical block.
- `bool format()`: Performs a hard wipe of the entire data partition path, cleanly resetting the state machine.
- `void auditLevels()`: Generates a topological report detailing live records, file count, storage footprints, and tombstones across all active levels.

---

## Tested Stability

NeuDB has been rigorously put through defensive, industrial-grade stress scenarios ("Pengamplasan Ekstrem Matrix") to guarantee production durability:

1.  **High-Speed Write Pounding:** Executed **1,000 / 1,000 updates** across randomized 16-bit keys wrapped within **1242 ms (~1.2ms average write response duration)**, fully verifying the concurrent transaction context queue under heavy flush workloads.
2.  **Memory Saturation Strain:** Sequential ingestion of 2,000 dense entries to pressure background task context-switching and atomic boundary scaling.
3.  **Power-Fail Interruption:** Hard physical power cutouts triggered during continuous multi-threaded write bombardment loops. Validated **100% cold crash recovery and data consistency** via automated WAL replay and CRC32-LE verification post-boot sequence.

---

## License & Attribution

Developed and maintained by **Ulywae** (2026). Released under the MIT License. Contributions to the **Neu Embedded Ecosystem** framework are welcome.
