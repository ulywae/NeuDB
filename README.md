# NeuDB

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform: Arduino](https://img.shields.io/badge/Platform-Arduino-00878F?logo=arduino&logoColor=white)](https://arduino.cc)
[![Platform: ESP32](https://img.shields.io/badge/Platform-ESP32-blue?logo=espressif&logoColor=white)](https://espressif.com)

> Ultra-Lean 16-Bit LSM-Tree Storage Engine with Power-Fail Safety and Multi-Media VFS Support for ESP32.

NeuDB (**NeuLSMDB_FS**) is a high-performance, asynchronous Log-Structured Merge-tree (LSM-Tree) storage engine built from scratch for the **ESP32** architecture using the Arduino framework and FreeRTOS. It is meticulously engineered to handle write-heavy logging workloads (e.g., continuous telemetry or sensor logs) while aggressively reducing flash write amplification, enforcing strict capacity guards, and extending Silicon lifespan across internal Flash (LittleFS) or external MicroSD hardware layers via an adaptive Virtual File System (VFS) architecture.

---

## Key Architectural Features

- **Adaptive Multi-Media Storage Framework:** Supports seamless runtime switching between resource-constrained internal Flash partitions (LittleFS) and high-capacity external MicroSD layouts via compile-time profile switches.
- **Deep 16-Bit LSM-Tree Array Hierarchy:** Implements an optimized data pipelining topology consisting of an active RAM MemTable and scalable multi-level SSTables (up to 5 levels on external storage), cleanly partitioning dynamic key space limits from physical log records.
- **Asynchronous Incremental Compaction Scheduler:** Driven by an isolated FreeRTOS background task pinned exclusively to CPU Core 1. It utilizes a Min-Heap (`std::priority_queue`) to stream-merge, deduplicate stale entries, and push finalized sorted data to deeper levels without triggering CPU starvation or Watchdog (WDT) resets.
- **Microsecond Read Path Optimization:** Harnesses the combined power of hardware-backed **Dynamic Bloom Filters** (up to 128 bytes with 5 hash counts) to intercept storage misses early, alongside **Binary Search (`std::lower_bound`)** on sorted in-memory index arrays to slash physical disk read latencies.
- **RAM Block Cache Layer:** Caches hot data record offsets dynamically into RAM memory blocks, bypassing heavy file-seek operations and directory traversal overhead on subsequent reads.
- **Industrial Power-Fail Safe Recovery:** Features robust Write-Ahead Logging (WAL) packed with hardware-backed CRC32-LE checksums. Automatically detects dirty shutdowns, skips trailing truncated sectors, and executes an atomic _WAL Replay_ during boot initialization to rescue uncommitted data safely.
- **Defensive Boundary Partition Guards:** Implements an automated resource monitoring system that flags a global `_flashFullGuard` when storage usage touches 90% or physical entry ceilings breach constraints, smoothly transitioning the write path or triggering reactive cache evictions.
- **Clean Facade Interface (Pimpl Pattern):** Completely hides complex internal STL containers (`std::map`, `std::vector`, `std::list`), abstract structs, and FreeRTOS primitive handles using an opaque `void*` wrapper interface. Zero header pollution.

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

- `bool begin()` / `init()`: Triggers the structural bootstrap pipeline and mounts virtual filesystems.
- `bool putVar(uint16_t key, const T &value)`: Template utility to ingest any primitive variable or custom struct directly into the MemTable.
- `bool getVar(uint16_t key, T &out)`: Template utility to fetch data records safely into a target variable scope.
- `bool putString(uint16_t key, const String &str)`: Dynamically handles Arduino String type serialization with embedded null terminators.
- `String getString(uint16_t key)`: Extracts a stored string record into the stack frame. Returns an empty string if the key profile is missing.
- `void flush()`: Forces immediate serialization of the volatile RAM MemTable down to a Level 0 SST physical block.
- `bool format()`: Performs a hard wipe of the entire active partition path, cleanly resetting the state machine from zero.
- `void auditLevels()`: Generates a topological report detailing live records, file count, storage footprints, and active tombstones across all levels.

---

## Performance Tuning & Hardware Configuration

NeuDB v1.2.1 features an **Adaptive Compile-Time Configuration Engine** managed entirely through the external `NeuDB_Config.h` header. The core system architecture automatically shifts its internal topology, dynamic memory budgets, tree depths, and pre-filtering matrices based on your active storage pipeline selector.

To modify the database profile, open `NeuDB_Config.h` and toggle the static preprocessor macro definition:

```cpp
// ==================================================================================
// STORAGE SETTINGS: CHOOSE ONE (Static Profile Selector)
// ==================================================================================
#define USE_LITTLEFS ///< Mount Built-In Internal Flash Partition Topology (Default)
// #define USE_SDCARD  ///< Mount External MicroSD SPI Hardware Peripheral Bus Layer
```

### Adaptive Compiler Profile Comparison Matrix

When a profile is locked, the internal engine automatically scales the following hardware-aware constraints at compile time:

| Architectural Constant         | Profile 1: `USE_LITTLEFS` (Internal Flash) | Profile 2: `USE_SDCARD` (MicroSD Card) | Purpose / Technical Impact                                               |
| :----------------------------- | :----------------------------------------: | :------------------------------------: | :----------------------------------------------------------------------- |
| **`NEU_MAX_LEVEL`**            |            **4 Levels** (L0-L3)            |          **5 Levels** (L0-L4)          | Controls tree depth to avoid SST file compaction bottlenecks [2].        |
| **`NEU_KEY_SPACE_LIMIT`**      |                 `2048` IDs                 |               `2048` IDs               | Bounds the maximum legal 16-bit Key address to optimize index RAM.       |
| **`NEU_MAX_TOTAL_ENTRIES`**    |                `2048` Rows                 |              `32768` Rows              | Unlocks physical storage scale up to 32k historical records on disk.     |
| **`NEU_MEMTABLE_MAX_ENTRIES`** |               `512` Entries                |             `2048` Entries             | Batches transactions in fast RAM to minimize flash wear amplification.   |
| **`NEU_SST_TARGET_SIZE`**      |                4 KB Blocks                 |              32 KB Chunks              | Aligns chunk serialization perfectly with physical SD sector clusters.   |
| **`NEU_CACHE_SIZE_BYTES`**     |                 1024 Bytes                 |               4096 Bytes               | Allocates dynamic lookahead memory for faster SPI disk search [2].       |
| **`NEU_BLOOM_FILTER_SIZE`**    |            64 Bytes (512 bits)             |         128 Bytes (1024 bits)          | Tightens the dense bitmask array to suppress lookup false positives [2]. |
| **`NEU_BLOOM_HASH_COUNT`**     |                4 Functions                 |              5 Functions               | Optimizes the mathematical collision dampener for target block density.  |

### Custom SPI Pin Mapping (MicroSD Mode Only)

If `USE_SDCARD` is uncommented, the unified `STORAGE_INIT()` gate automatically configures the hardware SPI peripheral controllers prior to mounting the filesystem. Modify the GPIO pins directly inside the configuration boundary to match your hardware layout:

```cpp
#define SD_CS     5   ///< SPI Chip Select Active-Low Controller Line
#define SD_MOSI   23  ///< Master Out Slave In Peripheral Data Line
#define SD_MISO   19  ///< Master In Slave Out Peripheral Data Line
#define SD_SCK    18  ///< Serial Clock Synchronizer Signal Line
#define SD_SPEED  4000000 ///< SPI Transmission Bus Clock Velocity (4MHz)
```

---

## Battle-Tested Radical Stability

NeuDB has been rigorously put through defensive, industrial-grade stress scenarios ("Pengamplasan Ekstrem Matrix") to guarantee production durability under extreme deployment:

1. **High-Speed Write Pounding:** Executed **1,000 / 1,000 updates** across randomized 16-bit keys wrapped within **2858 ms (~2.85ms average write response duration)**, fully verifying the concurrent transaction context queue under heavy asynchronous flush workloads.
2. **Over-Capacity Memory Saturation Strain:** Bombardment of **4,000 dense updates** via hot-key rolling loops to pressure background task context-switching, validating the **Heuristic Eviction Matrix** and keeping the layout perfectly bounded below target capacity limits.
3. **Power-Fail Sabotage Interruption:** Hard physical power cutouts triggered during continuous multi-threaded write loops with aggressive forced flushes. Validated **100% cold crash recovery and data consistency** via automated WAL parsing and hardware-assisted CRC32 verification post-boot sequence.

---

## License & Attribution

Developed and maintained by **Ulywae** (2026). Released under the MIT License. Contributions to the **Neu Embedded Ecosystem** framework are welcome.
