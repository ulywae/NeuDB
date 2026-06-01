/**
 * @file NeuLSMDB.h
 * @brief High-Performance, Hybrid LSM-Tree & Circular Log Embedded Storage Engine
 * @version 2.1.0
 * @date 2026
 * @author ulywae / NeuDB Core Team
 *
 * @copyright Copyright (c) 2026. Licensed under the MIT License.
 *
 * ==================================================================================
 *                              ARCHITECTURE OVERVIEW
 * ==================================================================================
 * NeuLSMDB is a lock-free input validated, crash-resilient Hybrid storage engine
 * combining the raw write-throughput of Log-Structured Merge (LSM) Trees with the
 * structured record tracking of relational engines (SQLite-inspired circular log topology).
 * Optimized for memory-constrained MCU architectures (ESP32) running over Virtual File
 * System (VFS) layers abstraction (Internal LittleFS & External SPI MicroSD Cards).
 *
 * Key Features Implemented & Battle-Tested:
 *  - 16-Bit Dynamic Key Space: Dual-partition topology mapping standard single-key mutations
 *    concurrently with a dedicated upper-boundary log snapshot memory bank.
 *  - Relational Circular Log Extension: Built-in hardware-saving rolling history logs.
 *    Bypasses write amplification via an compile-time automated offset calculator.
 *  - Compile-Time Topography Automation: Zero-hardcoded boundary protection leveraging
 *    mathematical compile-time calculations: (0xFFFF - (MAX_ID_LIMIT * MAX_INDEX_LIMIT)).
 *  - Lock-Free Stateful Streaming API: Ultra-lean, pointer-free facade loop iterator
 *    designed to keep Arduino sketches safe from volatile memory crashes.
 *  - Concurrent FreeRTOS Tasking: Multi-threaded background compaction task executing on
 *    isolated CPU cores with dynamic yield-and-retry thread synchronization handlers.
 *  - Hard Power-Failure Resilience: Transaction log recovery subsystem leveraging hardware-
 *    accelerated CRC32 validation matrices to guarantee cold-crash protection.
 *  - Double-Mapped LRU Cache & Bloom Prefilter: O(1) cache block address resolution
 *    with a 16-bit probabilistic filter matrix optimizing lookups under structural stress.
 *
 * ==================================================================================
 *                             DATA INTERACTION FLOWCHART
 * ==================================================================================
 *
 *      [ REGULAR DATA: put() ]                  [ AUTOMATIC DATA LOG: putLog() ]
 *                 │                                            │
 *                 ▼                                            ▼
 *    ┌──────────────────────────┐                ┌──────────────────────────┐
 *    │  Lockless Range Guard    │                │ Boundary Guard (ID Check)│
 *    │  (KEY_SPACE_LIMIT Chk)   │                ├──────────────────────────┤
 *    └────────────┬─────────────┘                │ DYNAMIC BIT-PACKING MASK │
 *                 │                              │ (OFFSET + ID<<BITS | IDX)│
 *                 │                              └─────────────┬────────────┘
 *                 ▼                                            ▼
 *    ┌──────────────────────────────────────────────────────────────────────┐
 *    │  xSemaphoreTake Lock & Write Stall Policy (REM 4ms Compaction Brake) │
 *    └──────────────────────────────────┬───────────────────────────────────┘
 *                                       │
 *                                       ▼
 *                          ┌──────────────────────────┐
 *                          │ FreeRTOS WAL Serializer  │ -> Append to wal.log
 *                          └────────────┬─────────────┘
 *                                       │
 *                                       ▼
 *                          ┌──────────────────────────┐
 *                          │ Commit to MemTable (RAM) │ -> Shared map volatile cache
 *                          └────────────┬─────────────┘
 *                                       │
 *                                       ▼
 *                          [ Background Flush (tick) ]
 *                                       │
 *                 ┌─────────────────────┴─────────────────────┐
 *                 ▼ [ DATA SPLITTER ROUTINE ]                 ▼
 *       (Key < NEU_LOG_KEY_OFFSET)                 (Key >= NEU_LOG_KEY_OFFSET)
 *                 │                                           │
 *                 ▼                                           ▼
 *    ┌──────────────────────────┐                ┌──────────────────────────┐
 *    │ Serialize Regular SST    │                │ Serialize Log Snapshot   │
 *    │  (/lsm/lvX_Y.sst)        │                │  (/lsm/log_lvX_Y.sst)    │
 *    └────────────┬─────────────┘                └────────────┬─────────────┘
 *                 │                                           │
 *                 ▼                                           ▼
 *    ┌──────────────────────────┐                ┌──────────────────────────┐
 *    │ runCompactionScheduler() │                │runLogCompactionScheduler()│
 *    │ (Merge Cascades L0->L4)  │                │(ROLLING HISTORY FILTER)  │
 *    └──────────────────────────┘                │(Trims -> MAX_LOG_HISTORY)│
 *                                                └──────────────────────────┘
 *
 * ==================================================================================
 *                             READ PIPELINE HIGHWAYS
 * ==================================================================================
 *  - HW 1 (Regular Lookup): get() -> Scan MemTable -> Bloom Check -> Binary SST -> VFS Read.
 *  - HW 2 (Log Point-Lookup): getLog(id, idx) -> REUSE readSST() over log loteng files.
 *  - HW 3 (Range Query Stream): logIterator() -> Stateful nextLog() -> O(1) Lazy Vector.
 * ==================================================================================
 */

#if !defined(NEU_CORE_ECO_SYSTEM)
/**
 * @def NEU_SECURITY_GUARD
 * @brief SYSTEM INTEGRITY & ECOSYSTEM ENCAPSULATION FENCE
 *
 * This core compilation submodule is a restricted internal asset of the Neu framework.
 * Direct independent execution, compilation, or compilation-unit bridging is strictly PROHIBITED.
 */
#error "Illegal Access Error! [NeuLSMDB] Isolate violation detected. This subsystem must only be ingested via the unified NeuDB abstraction gate."
#endif

#ifndef NEU_LSMDB_H
#define NEU_LSMDB_H

#include <cstdint>
#include <cstddef>
#include <WString.h>
#include <vector>
#include <map>
#include <cstring>
#include <queue>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include "NeuDB_Config.h"
#include <FS.h>

#if defined(USE_LITTLEFS)
#include <LittleFS.h>
#define STORAGE_INIT() LittleFS.begin(true)
#define STORAGE_EXISTS(p) LittleFS.exists(p)
#define STORAGE_MKDIR(p) LittleFS.mkdir(p)
#define STORAGE_REMOVE(p) LittleFS.remove(p)
#define STORAGE_RMDIR(p) LittleFS.rmdir(p)
#define STORAGE_OPEN(p, m) LittleFS.open(p, m)
#define STORAGE_RENAME(o, n) LittleFS.rename(o, n)
#define STORAGE_TOTAL() LittleFS.totalBytes()
#define STORAGE_USED() LittleFS.usedBytes()

#define NEU_LOG_INDEX_BITS 11 ///< Bit allocation depth driving the absolute circular index matrix layout.

#elif defined(USE_SDCARD)
#include <SD.h>
#include <SPI.h>
// Initialize SD using the pin set above
#define STORAGE_INIT() ({                       \
    SPI.begin(SD_SCK, SD_MISO, SD_MOSI, SD_CS); \
    SD.begin(SD_CS, SPI, SD_SPEED);             \
})

#define STORAGE_EXISTS(p) SD.exists(p)
#define STORAGE_MKDIR(p) SD.mkdir(p)
#define STORAGE_REMOVE(p) SD.remove(p)
#define STORAGE_RMDIR(p) SD.rmdir(p)
#define STORAGE_OPEN(p, m) SD.open(p, m)
#define STORAGE_RENAME(o, n) SD.rename(o, n)
#define STORAGE_TOTAL() SD.cardSize()
#define STORAGE_USED() (SD.cardSize() - (SD.totalBytes() - SD.usedBytes()))

#define NEU_LOG_INDEX_BITS 14

#else
#error "Please enable one of: #define USE_LITTLEFS OR #define USE_SDCARD"
#endif

// ==================================================================================
// AUTOMATIC BITMASK & LOGICAL INDEX METRICS GENERATOR
// ==================================================================================

/**
 * @brief Dynamic bitmask generation layer to safely isolate circular sequence counters.
 * Computes the target width at compile-time to guarantee 100% hardcode-free mask evaluation.
 * Evaluates automatically to 0x07FF for 11-bit depth or 0x3FFF for 14-bit depth configuration tracks.
 */
#define NEU_LOG_INDEX_MASK ((1UL << NEU_LOG_INDEX_BITS) - 1)

/**
 * @brief Total available circular tracking slots calculated via power-of-two allocation.
 * Establishes the hard mathematical boundary ceiling required for safe rolling ring modulo logic.
 * Evaluates automatically to 2048 slots for LittleFS profiles or 16384 slots for high-capacity SD Card profiles.
 */
#define NEU_LOG_MAX_INDEX (1UL << NEU_LOG_INDEX_BITS)

/**
 * @brief 32-Bit High-Address Anchor Offset Mapping Formula.
 * Calculated dynamically to isolate the rolling log matrix path cleanly at the ceiling of the 32-bit register space.
 * This guarantees zero memory coordinate collisions with the regular index data partition underneath.
 */
#define NEU_LOG_KEY_OFFSET (0xFFFFFFFF - ((uint32_t)NEU_LOG_MAX_ID_LIMIT * NEU_LOG_MAX_INDEX))

class NeuLSMDB;

class NeuLSMDB_LogIterator
{
public:
    NeuLSMDB_LogIterator(NeuLSMDB *db, uint16_t id, uint16_t startIdx, uint16_t endIdx);
    ~NeuLSMDB_LogIterator();

    bool next();
    uint16_t getIndex() const;
    uint32_t getTimestamp() const;
    bool getValue(void *out, size_t &size);

private:
    struct LogHeapEntry
    {
        uint16_t index;
        uint32_t ts;
        uint32_t offset;
        size_t sourceIdx;
        bool tombstone;
    };

    NeuLSMDB *_db;
    uint16_t _id;
    uint16_t _startIdx;
    uint16_t _endIdx;
    bool _valid;

    // Internal streaming buffers & state counters for FreeRTOS isolation
    void *_readersVector;
    void *_priorityQueue;
    size_t _currentIdx;
    uint32_t _currentTs;
    bool _currentTombstone;
};

#pragma pack(push, 1)
struct SSTHeader
{
    uint32_t key;
    uint32_t size;
    uint32_t ts;
    uint8_t tombstone;
};
#pragma pack(pop)

// ==========================================
// MAIN CLASS DECLARATION
// ==========================================
class NeuLSMDB
{
public:
    NeuLSMDB();
    ~NeuLSMDB();

    // ==========================================
    // CORE API FUNCTIONS
    // ==========================================

    bool init();
    bool put(uint16_t key, const void *data, size_t size);
    bool get(uint16_t key, void *out, size_t &size);
    void flush();
    void auditLevels();
    bool format();
    bool del(uint16_t key);

    // =================================================================
    // AUTOMATIC LOG INCREMENT & SNAPSHOT PIPELINE API
    // =================================================================

    /// Write log data for an ID. The engine automatically handles rolling index increment.
    bool putLog(uint16_t id, const void *data, size_t size);

    /// Skenario 1: Get the latest active snapshot for a specific ID based on the newest timestamp.
    bool getLog(uint16_t id, void *out, size_t &size);

    /// Skenario 2: Get a historical snapshot for a specific ID at an exact Index location.
    bool getLog(uint16_t id, uint16_t index, void *out, size_t &size);

    /// Get the total number of active (non-tombstone) log snapshots currently stored for an ID.
    size_t getTotalLog(uint16_t id);

    /// Clear all log history for a specific ID instantly using the tombstone mechanism.
    bool deleteLog(uint16_t id);

    // Give friendship status to the log range iterator class
    friend class NeuLSMDB_LogIterator;

    // ==========================================
    // CONFIGURATION API
    // ==========================================

    void setOverrideWhenFull(bool enable);
    bool getOverrideWhenFull() const;

    // ==========================================
    // EXPORT
    // ==========================================

    typedef void (*NeuDatasetCallback)(uint32_t rawKey, const uint8_t *data, size_t size, void *arg);

    /**
     * @brief High-speed cascading sweep scan across Regular MemTable and SSTables to extract Key-Value pairs.
     * @param callback The function pointer handling the ingested raw KV byte frames.
     * @param arg Optional user argument pointer passed cleanly through the execution pipeline.
     */
    void exportKVDataset(NeuDatasetCallback callback, void *arg);

    /**
     * @brief High-speed cascading sweep scan across MemTable and SSTables to extract log records.
     * @param callback The function pointer handling the ingested raw byte frames.
     * @param arg Optional user argument pointer passed cleanly through the execution pipeline.
     */
    void exportLogDataset(NeuDatasetCallback callback, void *arg);

private:
    // ==========================================
    // INTERNAL STRUCTURE DECLARATIONS
    // ==========================================

    struct MemEntry;
    struct SSTIndex;
    struct SSTFile;
    struct CacheBlock;
    struct SourceReader;
    struct HeapEntry;
    struct CompactJob;

    // ==========================================
    // COMPACTION STATE ENUM
    // ==========================================

    enum CompactState
    {
        IDLE,         ///< No compaction in progress
        MERGE_STREAM, ///< Merging overlapping SSTables
        FINALIZE      ///< Writing merged result and cleaning up old files
    };

    // ==========================================
    // SYSTEM ARCHITECTURE CONSTANTS
    // ==========================================

    static constexpr uint8_t MAX_LEVEL = NEU_MAX_LEVEL;                      ///< Maximum depth of LSM‑Tree levels (3–5 recommended)
    static constexpr size_t MEMTABLE_MAX_ENTRIES = NEU_MEMTABLE_MAX_ENTRIES; ///< Max entries before memtable is flushed to disk
    static constexpr size_t CACHE_SIZE_BYTES = NEU_CACHE_SIZE_BYTES;         ///< Total memory allocated for block cache (bytes)
    static constexpr uint8_t COMPACT_BUDGET_KB = NEU_COMPACT_BUDGET_KB;      ///< Max dynamic memory used during compaction (KB)
    static constexpr size_t SST_TARGET_SIZE = NEU_SST_TARGET_SIZE;           ///< Target size per SSTable file (bytes)
    static constexpr size_t MAX_TOTAL_ENTRIES = NEU_MAX_TOTAL_ENTRIES;       ///< Absolute maximum entries stored in whole database
    static constexpr size_t BLOOM_FILTER_SIZE = NEU_BLOOM_FILTER_SIZE;       ///< Bloom filter size in bytes (max 128)
    static constexpr uint8_t BLOOM_HASH_COUNT = NEU_BLOOM_HASH_COUNT;        ///< Number of hash functions per bloom filter

    // ==========================================
    // SYSTEM STATE VARIABLES & HANDLES
    // ==========================================

    bool _overrideWhenFull;              ///< Policy: evict old entries when full or reject writes
    volatile size_t _totalEntryCount;    ///< Total active (non‑tombstone) entries in database
    volatile size_t _memCount;           ///< Current entries in active memtable
    volatile uint32_t _lastFlush;        ///< Timestamp of last successful memtable flush
    volatile uint32_t _lastTune;         ///< Timestamp of last adaptive parameter adjustment
    volatile CompactState _compactState; ///< Current compaction phase
    QueueHandle_t _mutex;                ///< Mutex for thread‑safe access

    TaskHandle_t _taskHandle = NULL; ///< Handle to background maintenance task

    size_t _memBytes;      ///< Current memory used by memtable (bytes)
    size_t _adaptiveLimit; ///< Dynamically adjusted memtable size limit
    uint32_t _nextFileId;  ///< Next unique ID to assign to new SSTable file
    uint64_t _cacheUsed;   ///< Current bytes used in block cache

    void *_mem;        ///< Opaque pointer to active memtable structure
    void *_levels;     ///< Opaque pointer to multi‑level metadata
    void *_cacheList;  ///< Opaque pointer to LRU cache list
    void *_cacheMap;   ///< Opaque pointer to cache lookup map
    CompactJob *_job;  ///< Current active compaction job data
    fs::File _walFile; ///< Open handle to write‑ahead log file

    // ==========================================
    // PRIVATE INTERNAL UTILITY METHODS
    // ==========================================

    void tick();

    uint64_t makeCacheKey(uint32_t fileId, uint32_t offset);
    void cachePut(uint32_t fileId, uint32_t offset, const uint8_t *data, size_t len);
    bool cacheGet(uint32_t fileId, uint32_t offset, std::vector<uint8_t> &out);
    void cacheEvict();

    bool appendWAL(uint16_t key, const void *data, size_t size, bool tombstone);
    void replayWAL();
    void clearWAL();
    void flushWAL();

    String makeFilename(uint8_t level, uint32_t seq);
    uint32_t getFileSeq();

    bool writeSST(uint8_t level, const std::map<uint32_t, MemEntry> &entries, const String &dstFile = "");
    bool readSST(const SSTFile &sst, const SSTIndex &idxEntry, void *out, size_t &size);
    void loadAllSST();
    void deleteSSTFiles(const std::vector<String> &files);

    void bloomAdd(uint8_t *filter, uint16_t key);
    bool bloomCheck(const uint8_t *filter, uint16_t key);
    uint32_t bloomHash(uint16_t key, uint8_t seed);

    void triggerCompaction(uint8_t level);
    void tickCompact();
    void runCompactionScheduler();

    uint32_t crc32(uint32_t crc, const uint8_t *data, size_t len);

    void tuneMemtable();
    void evictOldestData();

    volatile bool _systemReady;            ///< True when engine is fully initialized and ready
    volatile bool _stopTaskRequested;      ///< Signal to background task to exit gracefully
    volatile bool _flashFullGuard = false; ///< Flag to prevent repeated eviction attempts when flash is near full

    std::priority_queue<HeapEntry> _compactHeap; ///< Priority queue used during merging
    bool _compactInitialized = false;            ///< Flag: compaction structures allocated
    std::vector<uint8_t> _compactValBuf;         ///< Temporary buffer for values during merge

    // =================================================================
    // LOG PIPELINE ISOLATION METADATA & ENGINE STATES
    // =================================================================

    void *_levelsLog;                       ///< Opaque pointer to multi‑level metadata for Log SSTables
    CompactJob *_jobLog;                    ///< Current active compaction job data for Log Pipeline
    volatile CompactState _compactLogState; ///< Current compaction phase of the Log Pipeline

    bool _compactLogInitialized = false;    ///< Flag: compaction structures for log allocated
    std::vector<uint8_t> _compactLogValBuf; ///< Temporary buffer for log values during merge compaction

    // ==========================================
    // INTERNAL STRUCTURE DEFINITIONS
    // ==========================================

    struct MemEntry
    {
        std::unique_ptr<uint8_t[]> value;
        uint16_t size;
        uint32_t ts;
        bool tombstone;
    };

    struct __attribute__((packed)) SSTIndex
    {
        uint32_t key;
        uint32_t offset;
        uint32_t size;
        uint32_t ts;
        bool tombstone;
        bool operator<(const SSTIndex &o) const { return key < o.key; }
    };

    struct SSTFile
    {
        String filename;
        std::vector<SSTIndex> index;
        uint32_t fileId;
        uint8_t bloom[BLOOM_FILTER_SIZE];
    };

    struct SourceReader
    {
        fs::File file;
        String filename;
        SSTIndex current;
        bool eof;
        uint32_t valueOffset;
        uint32_t version;

        bool open(const String &fname)
        {
            file = STORAGE_OPEN(fname, "r");
            eof = !file;
            version = 0;
            return next();
        }
        void close()
        {
            if (file)
                file.close();
        }

        bool next();
        bool readValue(uint8_t *buf, size_t &outSize);
    };

    struct CompactJob
    {
        uint8_t srcLevel;
        std::vector<String> srcFiles;
        std::vector<SourceReader> readers;
        String dstTemp;
        String dstFinal;
        bool active;
    };

    struct HeapEntry
    {
        uint32_t key;
        uint32_t ts;
        size_t readerIdx;
        uint32_t offset;
        uint32_t version;
        bool operator<(const HeapEntry &other) const { return key > other.key; }
    };

    // =================================================================
    // LOG INTERNAL PIPELINE UTILITY METHODS
    // =================================================================

    __attribute__((always_inline)) inline void internalDeleteSST(
        const std::vector<String> &files,
        std::vector<SSTFile> *levelsTarget);

    void loadAllSSTLog();
    bool writeSSTLog(uint8_t level, const std::map<uint32_t, MemEntry> &entries, const String &dstFile = "");
    void deleteSSTLogFiles(const std::vector<String> &files);

    void runLogCompactionScheduler();
    void tickCompactLog();

    /// Helper to find the latest active index and its timestamp for an ID across RAM and Flash index.
    bool findLatestLogIndex(uint16_t id, uint16_t &outIndex, uint32_t &outTs);

    /// The Reusable Core Execution Engine for unified database scanning.
    void sweepDatasetEngine(bool isLogPipeline, NeuDatasetCallback callback, void *arg);
};

#endif // NEU_LSMDB_H