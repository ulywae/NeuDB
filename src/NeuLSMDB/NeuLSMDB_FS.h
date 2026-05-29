/**
 * @file NeuLSMDB_FS.h
 * @brief High-Performance, 16-Bit LSM-Tree Embedded Storage Engine
 * @version 1.2.1
 * @date 2026
 * @author ulywae / NeuDB Core Team
 *
 * @copyright Copyright (c) 2026. Licensed under the MIT License.
 *
 * ==================================================================================
 *                              ARCHITECTURE OVERVIEW
 * ==================================================================================
 * NeuLSMDB_FS is a lock-free input validated, crash-resilient Log-Structured Merge
 * (LSM) Tree storage engine optimized for memory-constrained MCU topologies (ESP32)
 * running over an abstraction layer of Virtual File System (VFS) storage media
 * (Supports Internal Flash partitions and External MicroSD Hardware).
 *
 * Key Features Implemented & Battle-Tested:
 *  - 16-Bit Key Space: Lexicographically sorted maps indexing up to 65,536 unique keys.
 *  - Adaptive In-Memory Mutation: High-frequency writes bypass physical flash constraints
 *    via atomic state registers, reducing flash write amplification.
 *  - Concurrent FreeRTOS Tasking: Multi-threaded background compaction task executing on
 *    isolated CPU cores with dynamic yield-and-retry thread synchronization handlers.
 *  - Hard Power-Failure Resilience: Transaction log recovery subsystem leveraging hardware-
 *    accelerated CRC32 validation matrices to guarantee cold-crash protection.
 *  - Double-Mapped LRU Cache: O(1) cache address resolution using stable list iterators.
 *  - Probabilistic Pre-Filtering: Dynamic 16-bit Bloom Filters optimizing read latencies.
 *
 * ==================================================================================
 *                             DATA INTERACTION FLOWCHART
 * ==================================================================================
 *
 *      [ WRITE PATH: put() ]                    [ READ PATH: get() ]
 *               │                                         │
 *               ▼                                         ▼
 *    ┌─────────────────────┐                   ┌─────────────────────┐
 *    │ Lockless Range Guard│                   │ Lockless Range Guard│
 *    │(KEY_SPACE_LIMIT Chk)│                   │(KEY_SPACE_LIMIT Chk)│
 *    └──────────┬──────────┘                   └──────────┬──────────┘
 *               │                                         │
 *               ▼                                         ▼
 *    ┌─────────────────────┐                   ┌─────────────────────┐
 *    │ xSemaphoreTake lock ◄───────────────────► xSemaphoreTake lock │
 *    └──────────┬──────────┘                   └──────────┬──────────┘
 *               │                                         │
 *               ▼                                         ├─► [1] Read MemTable (RAM)
 *    ┌─────────────────────┐                              │   (Hit -> Instantly Return)
 *    │ _flashFullGuard(90%)│                              │
 *    └──────────┬──────────┘                              ▼
 *               │                              ┌─────────────────────┐
 *               │                              │ [2] Loop Levels 0-4 │
 *               ▼                              └──────────┬──────────┘
 *    ┌─────────────────────┐                              │
 *    │ FreeRTOS WAL Queue  │                              ▼
 *    │  (2ms Context Yield)│                   ┌─────────────────────┐
 *    └──────────┬──────────┘                   │  16-Bit Bloom Check │
 *               │                              └────┬───────────┬────┘
 *               ▼                                   │           │
 *    ┌─────────────────────┐                        │ Miss      │ Hit
 *    │  Append to wal.log  │                        ▼           ▼
 *    └──────────┬──────────┘                       [Next File] ┌────────────────────────┐
 *               │                                              │ Binary SST Index Search│
 *               ▼                                              │   (std::lower_bound)   │
 *    ┌─────────────────────┐                                   └────┬───────────────────┘
 *    │ Commit to MemTable  │                                        │
 *    └─────────────────────┘                                        ▼
 *               │                                      ┌─────────────────────┐
 *               ▼                                      │ LRU Block Cache Read│
 *    [ Background Flush (tick) ]                       └────┬───────────┬────┘
 *               │                                           │           │
 *               ▼                                           │ Miss      │ Hit
 *    ┌─────────────────────┐                                ▼           ▼
 *    │  Serialize to SST   │                           ┌─────────┐ ┌──────────┐
 *    │   (Level 0 File)    │                           │ Physical│ │ Fast RAM │
 *    └──────────┬──────────┘                           │ VFS Read│ │  Return  │
 *               │                                      └─────────┘ └──────────┘
 *               ▼
 *    ┌─────────────────────┐
 *    │ background Compact  │
 *    │(Merge Stream L0->L4)│
 *    └─────────────────────┘
 *
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
#error "Illegal Access Error! [NeuLSMDB_FS] Isolate violation detected. This subsystem must only be ingested via the unified NeuDB abstraction gate."
#endif

#ifndef NEU_LSMDB_FS_H
#define NEU_LSMDB_FS_H

#include "NeuDB_Config.h"
#include <cstdint>
#include <cstddef>
#include <WString.h>
#include <vector>
#include <map>
#include <cstring>
#include <queue>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

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

#else
#error "Please enable one of: #define USE_LITTLEFS OR #define USE_SDCARD"
#endif

// ==========================================
// MAIN CLASS DECLARATION
// ==========================================
class NeuLSMDB_FS
{
public:
    NeuLSMDB_FS();
    ~NeuLSMDB_FS();

    // ==========================================
    // CORE API FUNCTIONS
    // ==========================================

    bool init();
    bool put(uint16_t key, const void *data, size_t size);
    bool get(uint16_t key, void *out, size_t &size);
    void flush();
    void auditLevels();
    bool format();

    // ==========================================
    // CONFIGURATION API
    // ==========================================

    void setOverrideWhenFull(bool enable);
    bool getOverrideWhenFull() const;

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

    bool writeSST(uint8_t level, const std::map<uint16_t, MemEntry> &entries, const String &dstFile = "");
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
};

#endif // NEU_LSMDB_FS_H