#pragma once

#include <LittleFS.h>
#include <cstdint>
#include <cstddef>
#include <WString.h>
#include <vector>
#include <map>
#include <cstring>
#include <queue>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

typedef QueueHandle_t SemaphoreHandle_t;

// ==========================================
// MAIN CLASS DECLARATION
// ==========================================
class NeuLSMDB_FS
{
public:
    NeuLSMDB_FS();
    ~NeuLSMDB_FS();

    // ==========================================
    // CORE CORE API FUNCTIONS
    // ==========================================
    bool init();
    inline bool begin() { return init(); }
    bool put(uint8_t key, const void *data, size_t size);
    bool get(uint8_t key, void *out, size_t &size);
    void flush();
    void auditLevels();
    bool format();

    // Configuration Settings
    void setOverrideWhenFull(bool enable);
    bool getOverrideWhenFull() const;

private:
    // ==========================================
    // INTERNAL STRUCT DECLARATIONS
    // ==========================================
    struct MemEntry;
    struct SSTIndex;
    struct SSTFile;
    struct CacheBlock;
    struct SourceReader;
    struct HeapEntry;
    struct CompactJob;

    enum CompactState
    {
        IDLE,
        MERGE_STREAM,
        FINALIZE
    };

    // ==========================================
    // SYSTEM ARCHITECTURE CONSTANTS
    // ==========================================
    static constexpr uint8_t MAX_LEVEL = 4;             // Maximum depth of LSM Tree levels (Recommended: 3 - 5)
    static constexpr size_t MEMTABLE_MAX_ENTRIES = 512; // Maximum entry threshold before flushing MemTable to disk
    static constexpr size_t CACHE_SIZE_BYTES = 1024;    // Allocated block cache footprint size in bytes
    static constexpr uint8_t COMPACT_BUDGET_KB = 16;    // Dynamic memory budget constraint for compaction routines (KB)
    static constexpr size_t SST_TARGET_SIZE = 4096;     // Target maximum storage byte capacity per physical SST file
    static constexpr size_t MAX_TOTAL_ENTRIES = 2048;   // Absolute ceiling limit for aggregated database records
    static constexpr size_t BLOOM_FILTER_SIZE = 64;     // Dynamic Bloom Filter array size (MAXIMUM: 128 bytes)
    static constexpr uint8_t BLOOM_HASH_COUNT = 4;      // Total number of unique hash function seeds for Bloom Filter

    // ==========================================
    // SYSTEM STATE VARIABLES & HANDLES
    // ==========================================
    bool _overrideWhenFull;              // Enforces eviction policy over strict rejection when database is full
    volatile size_t _totalEntryCount;    // Global tracker for live non-tombstone entries across the engine
    volatile size_t _memCount;           // Current atomic volatile counter of records held in active MemTable
    volatile uint32_t _lastFlush;        // Timestamp tracking the last committed MemTable serialization
    volatile uint32_t _lastTune;         // Timestamp marking the final boundary of adaptive scaling routine
    volatile CompactState _compactState; // Current active state machine phase of the compaction worker
    QueueHandle_t _mutex;

    TaskHandle_t _taskHandle = NULL;

    size_t _memBytes;
    size_t _adaptiveLimit;
    uint32_t _nextFileId;
    uint64_t _cacheUsed;

    void *_mem;
    void *_levels;
    void *_cacheList;
    void *_cacheMap;
    CompactJob *_job;
    fs::File _walFile;

    // ==========================================
    // PRIVATE INTERNAL UTILITY METHODS
    // ==========================================
    void tick();

    uint64_t makeCacheKey(uint32_t fileId, uint32_t offset);
    void cachePut(uint32_t fileId, uint32_t offset, const uint8_t *data, size_t len);
    bool cacheGet(uint32_t fileId, uint32_t offset, std::vector<uint8_t> &out);
    void cacheEvict();

    bool appendWAL(uint8_t key, const void *data, size_t size, bool tombstone);
    void replayWAL();
    void clearWAL();
    void flushWAL();

    String makeFilename(uint8_t level, uint32_t seq);
    uint32_t getFileSeq();
    bool writeSST(uint8_t level, const std::map<uint8_t, MemEntry> &entries, const String &dstFile = "");
    bool readSST(const SSTFile &sst, const SSTIndex &idxEntry, void *out, size_t &size);
    void loadAllSST();
    void deleteSSTFiles(const std::vector<String> &files);

    void bloomAdd(uint8_t *filter, uint8_t key);
    bool bloomCheck(const uint8_t *filter, uint8_t key);
    uint32_t bloomHash(uint8_t key, uint8_t seed);

    void triggerCompaction(uint8_t level);
    void tickCompact();
    void runCompactionScheduler();

    uint32_t crc32(uint32_t crc, const uint8_t *data, size_t len);
    void tuneMemtable();
    void evictOldestData();

    volatile bool _systemReady;
    volatile bool _stopTaskRequested; // Worker task soft-shutdown flag orchestration identifier

    std::priority_queue<HeapEntry> _compactHeap;
    bool _compactInitialized = false;
    std::vector<uint8_t> _compactValBuf;
};
