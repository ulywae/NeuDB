/**
 * @file NeuLSMDB_FS.h
 * @brief Core LSM‑Tree storage engine implementation using LittleFS for ESP32/Arduino platforms.
 *
 * This class implements a full Log‑Structured Merge‑Tree (LSM‑Tree) database engine,
 * designed specifically for embedded systems. It uses LittleFS as the underlying
 * persistent storage layer, supports write‑ahead logging, memtable, SSTables,
 * background compaction, bloom filters, and block caching.
 * All keys are 8‑bit values (0–255) for simplicity and efficiency.
 * Thread‑safe access is provided via FreeRTOS synchronization primitives.
 */

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

/**
 * @def SemaphoreHandle_t
 * @brief Alias for queue handle used as mutex/semaphore type.
 */
typedef QueueHandle_t SemaphoreHandle_t;

// ==========================================
// MAIN CLASS DECLARATION
// ==========================================

/**
 * @class NeuLSMDB_FS
 * @brief Core LSM‑Tree database engine operating directly on LittleFS.
 *
 * Implements the internal storage logic: memtable, SSTable files, multi‑level
 * structure, write‑ahead log, caching, bloom filters, and automatic/manual compaction.
 * This is the underlying engine used by the higher‑level `NeuDB` wrapper class.
 */
class NeuLSMDB_FS
{
public:
    /**
     * @brief Constructor — Initializes internal pointers and state to default values.
     */
    NeuLSMDB_FS();

    /**
     * @brief Destructor — Frees all allocated memory, closes open files, stops background tasks.
     */
    ~NeuLSMDB_FS();

    // ==========================================
    // CORE API FUNCTIONS
    // ==========================================

    /**
     * @brief Initializes the engine, mounts LittleFS, recovers state from WAL, and loads existing SSTables.
     *
     * Must be called before any other operations. Starts background maintenance task.
     *
     * @return true if initialization completed successfully; false on failure (filesystem error, memory error).
     */
    bool init();

    /**
     * @brief Alias for `init()` — for API compatibility with Arduino style libraries.
     *
     * @return true on success, false on failure.
     */
    inline bool begin() { return init(); }

    /**
     * @brief Stores or updates a key/value pair in the database.
     *
     * Writes first to write‑ahead log (for durability) and active memtable.
     * Triggers flush to SSTable when memtable size limit is reached.
     *
     * @param key 8‑bit unique identifier (0–255).
     * @param data Pointer to the binary data to store.
     * @param size Size of data in bytes.
     * @return true if written successfully; false on invalid key, full DB, or I/O error.
     */
    bool put(uint8_t key, const void *data, size_t size);

    /**
     * @brief Retrieves value associated with the given key.
     *
     * Searches first in active memtable, then in cache, then in SSTables from newest to oldest level.
     * Uses bloom filters to skip files that definitely do not contain the key.
     *
     * @param key 8‑bit unique identifier to look up.
     * @param out Pointer to buffer where result will be stored.
     * @param size Reference: input = buffer capacity, output = actual bytes read.
     * @return true if key found and data retrieved; false if not found or read error.
     */
    bool get(uint8_t key, void *out, size_t &size);

    /**
     * @brief Forces immediate flush of active memtable to disk and runs pending compaction jobs.
     *
     * Ensures all data is persisted to LittleFS and optimizes file layout.
     */
    void flush();

    /**
     * @brief Scans all levels and files, calculates entry count, fragmentation, and prints status to output.
     *
     * Useful for debugging and monitoring storage health.
     */
    void auditLevels();

    /**
     * @brief Deletes all database files from LittleFS, resets all internal state to empty.
     *
     * @return true if format completed without errors; false if file deletion failed.
     */
    bool format();

    // ==========================================
    // CONFIGURATION API
    // ==========================================

    /**
     * @brief Sets behavior when maximum entry limit is reached.
     *
     * @param enable If true: evict oldest entries to make space for new writes.
     *               If false: reject new writes until space is freed manually.
     */
    void setOverrideWhenFull(bool enable);

    /**
     * @brief Gets current policy for handling full database state.
     *
     * @return true if auto‑eviction is enabled; false if writes are rejected when full.
     */
    bool getOverrideWhenFull() const;

private:
    // ==========================================
    // INTERNAL STRUCTURE DECLARATIONS
    // ==========================================

    /**
     * @struct MemEntry
     * @brief Represents a single key/value record stored in memory table.
     */
    struct MemEntry;

    /**
     * @struct SSTIndex
     * @brief Index entry inside an SSTable file: maps key to data offset/length.
     */
    struct SSTIndex;

    /**
     * @struct SSTFile
     * @brief Metadata for a sorted‑string table file stored on LittleFS.
     */
    struct SSTFile;

    /**
     * @struct CacheBlock
     * @brief Cached block of data from an SSTable file, used to speed up reads.
     */
    struct CacheBlock;

    /**
     * @struct SourceReader
     * @brief Helper for iterating over entries from multiple sorted sources during compaction.
     */
    struct SourceReader;

    /**
     * @struct HeapEntry
     * @brief Entry used in priority queue during merging of sorted runs.
     */
    struct HeapEntry;

    /**
     * @struct CompactJob
     * @brief Describes a compaction task: source levels/files, target level, progress state.
     */
    struct CompactJob;

    // ==========================================
    // COMPACTION STATE ENUM
    // ==========================================

    /**
     * @enum CompactState
     * @brief Current phase of the background compaction state machine.
     */
    enum CompactState
    {
        IDLE,         ///< No compaction in progress
        MERGE_STREAM, ///< Merging overlapping SSTables
        FINALIZE      ///< Writing merged result and cleaning up old files
    };

    // ==========================================
    // SYSTEM ARCHITECTURE CONSTANTS
    // ==========================================

    static constexpr uint8_t MAX_LEVEL = 4;             ///< Maximum depth of LSM‑Tree levels (3–5 recommended)
    static constexpr size_t MEMTABLE_MAX_ENTRIES = 512; ///< Max entries before memtable is flushed to disk
    static constexpr size_t CACHE_SIZE_BYTES = 1024;    ///< Total memory allocated for block cache (bytes)
    static constexpr uint8_t COMPACT_BUDGET_KB = 16;    ///< Max dynamic memory used during compaction (KB)
    static constexpr size_t SST_TARGET_SIZE = 4096;     ///< Target size per SSTable file (bytes)
    static constexpr size_t MAX_TOTAL_ENTRIES = 2048;   ///< Absolute maximum entries stored in whole database
    static constexpr size_t BLOOM_FILTER_SIZE = 64;     ///< Bloom filter size in bytes (max 128)
    static constexpr uint8_t BLOOM_HASH_COUNT = 4;      ///< Number of hash functions per bloom filter

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

    /**
     * @brief Background periodic update: handles flushing, compaction, and tuning.
     */
    void tick();

    /**
     * @brief Generates unique key for cache entries from file ID and offset.
     * @param fileId ID of the SSTable file
     * @param offset Byte offset inside the file
     * @return Combined 64‑bit key
     */
    uint64_t makeCacheKey(uint32_t fileId, uint32_t offset);

    /**
     * @brief Inserts a block of data into the cache.
     * @param fileId Source file ID
     * @param offset Source offset
     * @param data Pointer to data block
     * @param len Length of data block
     */
    void cachePut(uint32_t fileId, uint32_t offset, const uint8_t *data, size_t len);

    /**
     * @brief Retrieves a block from cache if present.
     * @param fileId Source file ID
     * @param offset Source offset
     * @param out Vector to receive data
     * @return true if found in cache; false otherwise
     */
    bool cacheGet(uint32_t fileId, uint32_t offset, std::vector<uint8_t> &out);

    /**
     * @brief Removes least recently used block when cache is full.
     */
    void cacheEvict();

    /**
     * @brief Appends an entry to the write‑ahead log for crash recovery.
     * @param key Entry key
     * @param data Entry data (nullable for tombstones)
     * @param size Data length
     * @param tombstone True if this entry marks a deletion
     * @return true if written successfully
     */
    bool appendWAL(uint8_t key, const void *data, size_t size, bool tombstone);

    /**
     * @brief Reads and applies all entries from WAL after startup to recover state.
     */
    void replayWAL();

    /**
     * @brief Truncates and clears the write‑ahead log file.
     */
    void clearWAL();

    /**
     * @brief Forces WAL file contents to be physically written to storage.
     */
    void flushWAL();

    /**
     * @brief Generates standard filename for an SSTable file.
     * @param level Level number (0 to MAX_LEVEL‑1)
     * @param seq Unique sequence/file ID
     * @return Formatted filename string
     */
    String makeFilename(uint8_t level, uint32_t seq);

    /**
     * @brief Allocates and returns next unique file sequence ID.
     * @return Next available file ID
     */
    uint32_t getFileSeq();

    /**
     * @brief Writes a sorted map of entries into a new SSTable file at given level.
     * @param level Target level
     * @param entries Sorted key/value entries to write
     * @param dstFile Optional custom filename; auto‑generated if empty
     * @return true if file created and written successfully
     */
    bool writeSST(uint8_t level, const std::map<uint8_t, MemEntry> &entries, const String &dstFile = "");

    /**
     * @brief Reads a single value from an SSTable file using its index entry.
     * @param sst SSTable metadata
     * @param idxEntry Index entry pointing to the record
     * @param out Output buffer
     * @param size Input: buffer size, output: actual data length
     * @return true if read successfully
     */
    bool readSST(const SSTFile &sst, const SSTIndex &idxEntry, void *out, size_t &size);

    /**
     * @brief Scans LittleFS directory and loads metadata for all existing SSTable files.
     */
    void loadAllSST();

    /**
     * @brief Deletes specified SSTable files from LittleFS after compaction.
     * @param files List of filenames to remove
     */
    void deleteSSTFiles(const std::vector<String> &files);

    /**
     * @brief Adds a key to a bloom filter.
     * @param filter Pointer to filter byte array
     * @param key Key to add
     */
    void bloomAdd(uint8_t *filter, uint8_t key);

    /**
     * @brief Checks if key might exist in set (may return false positive, never false negative).
     * @param filter Pointer to filter byte array
     * @param key Key to check
     * @return false → definitely not present; true → possibly present
     */
    bool bloomCheck(const uint8_t *filter, uint8_t key);

    /**
     * @brief Generates a hash value for bloom filter calculation.
     * @param key Key to hash
     * @param seed Unique seed for different hash functions
     * @return 32‑bit hash value
     */
    uint32_t bloomHash(uint8_t key, uint8_t seed);

    /**
     * @brief Starts compaction process for given level if conditions are met.
     * @param level Level to compact
     */
    void triggerCompaction(uint8_t level);

    /**
     * @brief Executes next step in ongoing compaction process.
     */
    void tickCompact();

    /**
     * @brief Decides which levels/files to compact and schedules jobs.
     */
    void runCompactionScheduler();

    /**
     * @brief Computes CRC32 checksum for data integrity verification.
     * @param crc Initial CRC value
     * @param data Input data
     * @param len Data length
     * @return Updated CRC value
     */
    uint32_t crc32(uint32_t crc, const uint8_t *data, size_t len);

    /**
     * @brief Adjusts memtable size and thresholds dynamically based on usage.
     */
    void tuneMemtable();

    /**
     * @brief Removes oldest entries when database reaches maximum capacity.
     */
    void evictOldestData();

    volatile bool _systemReady;       ///< True when engine is fully initialized and ready
    volatile bool _stopTaskRequested; ///< Signal to background task to exit gracefully

    std::priority_queue<HeapEntry> _compactHeap; ///< Priority queue used during merging
    bool _compactInitialized = false;            ///< Flag: compaction structures allocated
    std::vector<uint8_t> _compactValBuf;         ///< Temporary buffer for values during merge
};