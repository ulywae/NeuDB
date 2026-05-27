/**
 * @file NeuDB.h
 * @brief High‑Level Facade Interface Wrapper for the NeuLSMDB_FS Core Storage Engine.
 * @version 1.1.0
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
#include <Arduino.h> // Required for native String serialization profiles

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

    /**
     * @brief Materializes dynamic heap-allocated text entries into standard Arduino String objects.
     *
     * @param key 16‑bit distinct query identifier.
     * @return String containing the active verified record text; returns an empty object on data miss.
     */
    String getString(uint16_t key);

    /**
     * @brief Fetches the current operational library version tag.
     * @return SemVer compliant constant character array pointer ("MAJOR.MINOR.PATCH").
     */
    const char *getVersion() const { return "1.1.0"; }

private:
    void *_engine; ///< Opaque Pimpl pointer concealing the active NeuLSMDB_FS engine instance profile.
};

/**
 * @var db
 * @brief Global singleton instance pre‑instantiated for direct call injection within Arduino loop structures.
 */
extern NeuDB db;

#endif
