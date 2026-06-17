/**
 * @file NeuDB.h
 * @brief High‑Level Facade Interface Wrapper for the NeuLSMDB Core Storage Engine.
 * @version 2.1.2
 * @date 2026
 * @author Ulywae / Neu Embedded Ecosystem Framework
 *
 * This header exposes an ultra‑lean, developer‑friendly API designed for seamless
 * integration into standard Arduino environment sketches. It completely encapsulates the
 * low‑level complexities of the underlying Log‑Structured Merge (LSM) Tree engine—hiding
 * internal vectors, primitive mutices, priority queues, and raw file handles behind an
 * opaque object mask (Pimpl pattern) to guarantee compile‑time isolation and zero header pollution.
 */

#ifndef NEU_DB_H
#define NEU_DB_H

#include <cstdint>
#include <cstddef>
#include <Arduino.h>

/**
 * @note SYSTEM REBOOT RELEASE NOTES (v2.0.0):
 * - Deployed high-address anchor 32-bit register architecture to prevent key collision track.
 * - Engineered twin-engine storage pipeline (isolated regular and log snapshot layers).
 * - Expanded circular rolling history tracks up to 16,384 non-volatile dynamic slots per ID.
 * - Embedded hyper-drive write ingestion pipeline (100 sequential commits in 12ms).
 * - Standardized corporate-grade Doxygen technical English documentation architecture.
 */

// ==================================================================================
// NEUDB COMPILATION FRAMEWORK INTERFACE LAYER
// ==================================================================================
#define NEU_DB_VERSION_MAJOR 2
#define NEU_DB_VERSION_MINOR 1
#define NEU_DB_VERSION_PATCH 2
#define NEU_DB_VERSION_STR "2.1.2"

// =================================================================
// FORWARD DECLARATION LAYER: ZERO‑HEADER POLLUTION GUARANTEE
// =================================================================
class Stream; ///< Abstract framework I/O channel forward‑declaration

/**
 * @class NeuDB
 * @brief Top‑Level Transaction Handler Facade for Embedded LSM‑Tree Storage.
 *
 * This class serves as the primary data ingestion gate for standard application sketches.
 * It coordinates write‑path pipelining, read‑path lookups, dynamic template mapping,
 * and diagnostic routines. Keys are restricted to 16‑bit numeric addresses bounded within
 * physical memory array thresholds. Fully thread‑safe and reentrant across FreeRTOS task contexts.
 */

class NeuDB
{
public:
    /**
     * @brief Context Constructor — Initializes wrapper instance metrics.
     */
    NeuDB();

    /**
     * @brief Context Destructor — Executes safety flush checkpoints and releases runtime resource allocations.
     */
    ~NeuDB();

    /**
     * @brief Boots the storage pipeline, mounts VFS partitions, and verifies transaction logs.
     *
     * Inline abstraction wrapper mapped directly to the internal core initialization sequence.
     * Must be explicitly invoked inside the baseline setup() routine.
     *
     * @return true if virtual filesystem mounting and crash recovery succeed; false on system failure.
     */
    inline bool begin()
    {
        return this->init();
    }

    /**
     * @brief Triggers the concrete structural bootstrap pipeline sequencing.
     *
     * @return true if initialization, validation, and crash recovery replay succeed; false on hardware fault.
     */
    bool init();

    /**
     * @brief Commits raw byte strings directly into the write‑path ingestion pipeline.
     *
     * Transactions pass lockless range checks, acquire a core mutex handle, and serialize
     * sequentially into the append‑only WAL ring buffer before updating the volatile memory tree layout.
     *
     * @param key 16‑bit distinct structural address, strictly bound within capacity metrics (0 to MAX_TOTAL_ENTRIES - 1).
     * @param data Constant pointer targeting the source data payload buffer.
     * @param size Payload volume measured in bytes (Max constraint 65,535 bytes).
     * @return true if transaction is durably logged and mapped; false if rejected by range check or storage saturation guards.
     */
    bool put(uint16_t key, const void *data, size_t size);

    /**
     * @brief Dispatches a deterministic point‑lookup query to extract record payloads.
     *
     * The read pathway executes a tiered traversal strategy: checks volatile RAM MemTable, searches the LRU Block Cache,
     * evaluates probabilistic Bloom Filters, and falls back to binary index searching on physical disk SST files.
     *
     * @param key 16‑bit distinct structural address to search for.
     * @param out Destination pointer targeting the output allocation buffer space.
     * @param size Reference descriptor: input specifies output buffer threshold capacity, output yields actual payload bytes extracted.
     * @return true if key vector exists and payload correlates perfectly; false on miss, capacity overflow, or data drift.
     */
    bool get(uint16_t key, void *out, size_t &size);

    /**
     * @brief Forces immediate checkpoint serialization, dropping volatile memtables into physical Level 0 storage blocks.
     */
    void flush();

    /**
     * @brief Interrogates active structural levels to compile and print topological footprint reports.
     */
    void auditLevels();

    /**
     * @brief Wipes the active storage directory, truncates logging queues, and formats file systems.
     *
     * @return true if partition formatting executes successfully; false on VFS locking failures.
     */
    bool format();

    /**
     * @brief Removes a key vector from the active storage footprint.
     *
     * @param key 16-bit distinct structural address to delete.
     */
    bool del(uint16_t key);

    /**
     * @brief Modifies engine structural reactive policies when storage footprints cross target ceilings.
     *
     * @param enable If true, reactive eviction drops stale data via proactive tombstone injection.
     *               If false, incoming write operations are forcefully rejected at the ingestion gate.
     */
    void setOverrideWhenFull(bool enable);

    /**
     * @brief Retrieves the active operational constraint strategy for full storage conditions.
     *
     * @return true if proactive cache eviction matrix is armed; false if hard validation rejections are enforced.
     */
    bool getOverrideWhenFull() const;

    // =================================================================
    // TYPE-SAFE TEMPLATE ACCELERATORS: ZERO-OVERHEAD VOLATILE EXTENSIONS
    // =================================================================

    /**
     * @brief Ingests arbitrary variables or custom structural packed matrices directly into RAM.
     *
     * Automatically extracts payload address coordinates and data type size bounds at compile time,
     * removing the need for manual pointer casts or sizeof arithmetic in consumer sketches.
     *
     * @tparam T Arbitrary data type, primitive variable, or custom packed telemetry structure.
     * @param key 16‑bit distinct destination identifier.
     * @param value Constant reference to the source variable to map.
     * @return true on successful ingestion commitment; false otherwise.
     */
    template <typename T>
    bool putVar(uint16_t key, const T &value)
    {
        return this->put(key, &value, sizeof(T));
    }

    /**
     * @brief Recovers arbitrary structures securely from the storage architecture.
     *
     * Compiles exact target size boundaries matching type T to enforce memory safety bounds
     * during extraction.
     *
     * @tparam T Target data structural variable layout profile.
     * @param key 16‑bit distinct origin query identifier.
     * @param out Target reference scope where the recovered payload data is directly cast and populated.
     * @return true if lookup succeeds and type footprint matches; false on data miss or capacity overflow.
     */
    template <typename T>
    bool getVar(uint16_t key, T &out)
    {
        size_t size = sizeof(T);
        return this->get(key, &out, size);
    }

    /**
     * @brief Serializes dynamic heap‑allocated Arduino String components down to persistent blocks.
     *
     * Safely traverses heap pointers to capture true character array boundaries and length metrics.
     *
     * @param key 16‑bit distinct destination identifier.
     * @param str Constant reference to the dynamic source String object.
     * @return true on successful string commitment; false on failure bounds.
     */
    bool putString(uint16_t key, const String &str);

    // =================================================================
    // CONFIGURATION BUFFER MUTATORS
    // =================================================================
    /**
     * @brief Sets the maximum internal buffer size for dynamic string retrieval.
     * @param maxLen The new maximum byte length limit for getString operations.
     */
    void setMaxStringLength(size_t maxLen)
    {
        if (maxLen > 0 && maxLen <= 1024)
            this->_maxStrLen = maxLen;
    }

    /**
     * @brief Gets the current maximum internal buffer size for string retrieval.
     */
    size_t getMaxStringLength() const { return this->_maxStrLen; }

    /**
     * @brief Materializes dynamic heap-allocated text entries into standard Arduino String objects.
     *
     * @param key 16‑bit distinct query identifier.
     * @return String containing the active verified record text; returns an empty object on data miss.
     */
    String getString(uint16_t key);

    /**
     * @brief Retrieves the absolute framework release version string at runtime.
     * @note Guarantees const-safety alignment to provide read-only diagnostic metadata.
     * @return Constant char pointer targeting the immutable "2.0.0" semantic version string literal.
     */
    const char *getVersion() const { return NEU_DB_VERSION_STR; }

    // =================================================================
    // AUTOMATIC LOG INCREMENT & SNAPSHOT FACADE API
    // =================================================================

    /**
     * @brief Ingests an atomic log payload under a specific object identifier.
     *
     * The underlying storage engine automatically resolves the active circular sequence
     * boundaries mapped to NEU_LOG_MAX_INDEX and serializes a fresh snapshot record.
     *
     * @param id Distinct identifier for the target log track variable (0 to NEU_LOG_MAX_ID_LIMIT - 1).
     * @param data Constant raw pointer targeting the source log payload buffer.
     * @param size Payload volume measured in bytes.
     * @return true if the transactional transaction frame is safely committed to WAL and indexed; false otherwise.
     */
    bool putLog(uint16_t id, const void *data, size_t size);

    /**
     * @brief Extracts the latest active log snapshot state for a target identifier based on the highest timestamp.
     *
     * Performs a fast point-lookup across volatile caches and non-volatile index cascades
     * to intercept and retrieve the single newest historical mutation block.
     *
     * @param id Target log tracking identifier to query.
     * @param out Destination raw pointer targeting the pre-allocated user output buffer space.
     * @param size Reference to enforce memory capacity boundaries and receive actual retrieved bytes.
     * @return true if a valid non-tombstone record exists and is successfully extracted; false on logical miss.
     */
    bool getLog(uint16_t id, void *out, size_t &size);

    /**
     * @brief Extracts a targeted, historical log snapshot record at an exact circular slot position.
     *
     * Resolves absolute multi-dimensional coordinates dynamically to sweep the exact historical tier
     * without executing expansive full-table disk scan iterations.
     *
     * @param id Target log tracking identifier to query.
     * @param index Absolute targeted circular ring buffer slot coordinate.
     * @param out Destination raw pointer targeting the pre-allocated user output buffer space.
     * @param size Reference to enforce memory capacity boundaries and receive actual retrieved bytes.
     * @return true if the index slot maps to a valid transaction node; false on miss, capacity mismatch, or tombstone interception.
     */
    bool getLog(uint16_t id, uint16_t index, void *out, size_t &size);

    /**
     * @brief Compiles the true cumulative metric count of active, non-tombstone historical entries for an ID.
     *
     * Sweeps transaction trackers concurrently and performs stack-allocated deduplication
     * to skip over multi-version obsolete records or cleared slots.
     *
     * @param id Target log tracking identifier to calculate metrics.
     * @return Total integer count of active historical snapshot records currently retained in storage clusters.
     */
    size_t getTotalLog(uint16_t id);

    /**
     * @brief Purges the absolute operational history track belonging to an identifier via reactive tombstone injection.
     *
     * Rather than triggering immediate blocking physical sector unlinking, this enqueues low-overhead
     * cancellation markers to let the background compaction task safely clear physical space later.
     *
     * @param id Target log tracking identifier to wipe from the system.
     * @return true if the atomic purge sequence successfully commits transaction markers; false on lock timeouts.
     */
    bool deleteLog(uint16_t id);

    // =================================================================
    // TEMPLATE LOG ACCELERATORS: TYPE-SAFE COMPLIANT EXTENSIONS
    // =================================================================

    /**
     * @brief Type-safe template wrapper to seamlessly ingest fixed-size data structures without pointer casting.
     * @tparam T Inferred type descriptor of the incoming source object.
     */
    template <typename T>
    bool putLogVar(uint16_t id, const T &value)
    {
        return this->putLog(id, &value, sizeof(T));
    }

    /**
     * @brief Type-safe template wrapper to retrieve the latest historical state directly into a matching struct.
     * @tparam T Inferred type descriptor of the target destination object.
     */
    template <typename T>
    bool getLogVar(uint16_t id, T &out)
    {
        size_t size = sizeof(T);
        return this->getLog(id, &out, size);
    }

    /**
     * @brief Type-safe template wrapper to query a targeted historical slot index directly into a matching struct.
     * @tparam T Inferred type descriptor of the target destination object.
     */
    template <typename T>
    bool getLogVar(uint16_t id, uint16_t index, T &out) 
    {
        size_t size = sizeof(T);
        return this->getLog(id, index, &out, size);
    }

    // =================================================================
    // HIGH-LEVEL ARCHITECTURE LOG RANGE ITERATOR PIPELINE
    // =================================================================

    /**
     * @brief Spawns a stateful lookahead cursor context to stream historical log boundaries.
     *
     * Establishes dynamic range constraints to support sequential tracking sweeps. Memory optimization matrices
     * are evaluated lazily on the first iteration step to guarantee zero upfront heap overhead allocation.
     *
     * @param id Target log tracking identifier to sweep.
     * @param startIdx Baseline lower coordinate slot boundary to open the streaming track.
     * @param endIdx Ceiling upper coordinate slot boundary to close the streaming track.
     * @return true if the tracking parameters are authenticated and the iteration layer is armed; false on range violations.
     */
    bool logIterator(uint16_t id, uint16_t startIdx, uint16_t endIdx);

    /**
     * @brief Advances the high-address range cursor position forward to the next historical element sequence track.
     *
     * Intended as the core evaluation expression inside clean application while() blocks.
     * Automatically filters out query boundary overflows and intercepts tombstones in-flight.
     *
     * @return true if a fresh historical state node is successfully captured; false upon hitting true EOF parameters.
     */
    bool nextLog();

    /**
     * @brief Extracts the raw data payload belonging to the record currently locked by the iterator cursor.
     *
     * @param out Destination raw pointer targeting the pre-allocated user output buffer space.
     * @param size Reference to specify capacity limits and receive actual extracted payload size bounds.
     * @return true if physical block data streaming completes successfully; false on descriptor or size errors.
     */
    bool getLogValue(void *out, size_t &size);

    /**
     * @brief Type-safe template accelerator to stream active cursor record fields directly into destination struct frames.
     * @tparam T Inferred type descriptor of the user target structure array.
     */
    template <typename T>
    bool getLogValueVar(T &out)
    {
        size_t size = sizeof(T);
        return this->getLogValue(&out, size);
    }

    /**
     * @brief Retrieves the decoded circular slot index position currently pointed to by the active stream cursor.
     * @return The native slot coordinate stripped of high-address registration offsets.
     */
    uint16_t getLogIndex();

    /**
     * @brief Retrieves the native hardware millisecond timestamp tracking exactly when the current cursor entry was recorded.
     * @return Absolute chronological timestamp value of the active transaction record.
     */
    uint32_t getLogTimestamp();

    /**
     * @brief Destroys the stateful tracking cursor context and deallocates all stack/heap vector memory partitions.
     * Must be explicitly invoked by the implementation routine to secure volatile memory bounds against heap fragmentation.
     */
    void closeLog();

    // =================================================================
    // BULK DATA EXPORT / DUMP UTILITY GATEWAY APIs
    // =================================================================

    /**
     * @brief Streams the entire active regular Key‑Value dataset out to an external channel.
     * @param targetStream Pointer to an active Arduino Stream instance (e.g., &Serial, &backupFile).
     * @return true if entries were successfully extracted and serialized; false otherwise.
     */
    bool exportKeyValuesToStream(Stream *targetStream);

    /**
     * @brief Streams the entire multi‑version circular log dataset out to an external channel.
     * @param targetStream Pointer to an active Arduino Stream instance (e.g., &Serial, &telnetClient).
     * @return true if records were successfully extracted and serialized; false otherwise.
     */
    bool exportLogsToStream(Stream *targetStream);

    // =================================================================
    // SYSTEM STORAGE METRICS INTERFACE (ZERO-IO RAM CACHED)
    // =================================================================
    /**
     * @brief Retrieves the total flash storage partition scale in bytes.
     */
    size_t getTotalBytes() const;

    /**
     * @brief Retrieves the currently occupied storage footprint in bytes.
     */
    size_t getUsedBytes() const;

    /**
     * @brief Computes the remaining free non-volatile storage space in bytes.
     */
    inline size_t getFreeBytes() const { return this->getTotalBytes() - this->getUsedBytes(); }

private:
    // =================================================================
    // PRIVATE INTERNAL ARCHITECTURAL GUARD
    // =================================================================
    void *_engine;                      ///< Opaque Pimpl pointer concealing the active NeuLSMDB engine instance profile.
    void *_activeLogIterator = nullptr; ///< Private heap-allocated context tracking state variations for the operational range iterator.
    size_t _maxStrLen = 128;            ///< Internal template accelerator parameter to set maximum string length bounds for safe stack allocation during getString operations.
};

/**
 * @var db
 * @brief Global singleton instance pre‑instantiated for direct call injection within Arduino loop structures.
 */
extern NeuDB db;

#endif
