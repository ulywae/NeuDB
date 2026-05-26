/**
 * @file NeuDB.h
 * @brief High‑level wrapper library for LSM‑Tree database engine using LittleFS.
 *
 * This class provides a simple, user‑friendly API designed for Arduino projects,
 * wrapping the full functionality of the underlying LSM‑Tree storage engine
 * implemented on top of LittleFS. It supports storing and retrieving raw binary
 * data, arbitrary variables, and Arduino String objects, while handling all
 * internal complexity like memtables, SSTables, compaction, and caching automatically.
 */

#ifndef NEU_DB_H
#define NEU_DB_H

#include <cstdint>
#include <cstddef>
#include <Arduino.h> // Required for Arduino's built‑in String type

/**
 * @class NeuDB
 * @brief Simple interface to access the LSM‑Tree database running on LittleFS.
 *
 * This is the main class intended for use in `.ino` sketches. It delegates all
 * operations to an internal LSM‑Tree engine instance. Keys are limited to 8‑bit
 * values (0–255) for efficiency. Thread‑safe and optimized for embedded systems.
 */
class NeuDB
{
public:
    /**
     * @brief Constructor — Creates a NeuDB instance and initializes internal state.
     */
    NeuDB();

    /**
     * @brief Destructor — Cleans up resources and safely shuts down the storage engine.
     */
    ~NeuDB();

    /**
     * @brief Initializes the database engine, mounts LittleFS, and prepares storage.
     *
     * Must be called once in `setup()` before using any other methods.
     *
     * @return true if initialization succeeded; false on filesystem or memory errors.
     */
    bool begin();

    /**
     * @brief Stores raw binary data under a specified key.
     *
     * Data is written first to a write‑ahead log and memory table, then persisted
     * to LittleFS following LSM‑Tree principles.
     *
     * @param key 8‑bit unique identifier (0–255) for the entry.
     * @param data Pointer to the data buffer to store.
     * @param size Size of the data in bytes.
     * @return true if stored successfully; false on invalid parameters or storage full.
     */
    bool put(uint8_t key, const void *data, size_t size);

    /**
     * @brief Retrieves raw binary data associated with a given key.
     *
     * Searches memory table, cache, and disk files in order; uses bloom filters
     * internally to speed up lookups.
     *
     * @param key 8‑bit unique identifier of the entry to read.
     * @param out Pointer to buffer where retrieved data will be stored.
     * @param size Reference: input = buffer capacity, output = actual bytes read.
     * @return true if key exists and data was read; false if not found or error.
     */
    bool get(uint8_t key, void *out, size_t &size);

    /**
     * @brief Forces all pending data to be written permanently to LittleFS and triggers compaction.
     *
     * Useful before power‑off to ensure no data is lost.
     */
    void flush();

    /**
     * @brief Runs a health check, scans all storage levels, and prints usage statistics.
     *
     * Shows entry count, file structure, and fragmentation status.
     */
    void auditLevels();

    /**
     * @brief Erases all stored data, deletes database files from LittleFS, and resets the engine.
     *
     * @return true if format completed successfully; false on file operation failure.
     */
    bool format();

    /**
     * @brief Configures behavior when the database reaches maximum capacity.
     *
     * @param enable If true: automatically evict oldest entries to make space for new writes.
     *               If false: reject new writes until space is freed manually.
     */
    void setOverrideWhenFull(bool enable);

    /**
     * @brief Gets current configuration policy for full storage handling.
     *
     * @return true if auto‑eviction is enabled; false if writes are rejected when full.
     */
    bool getOverrideWhenFull() const;

    // =================================================================
    // TEMPLATE HELPER: Simplifies usage — no manual pointers or sizeof needed in sketches!
    // =================================================================

    /**
     * @brief Stores a variable of any type directly under a key.
     *
     * Automatically takes the address and size of the variable. Works with primitive
     * types, structs, and fixed‑size data.
     *
     * @tparam T Data type of the value to store.
     * @param key 8‑bit unique identifier.
     * @param value Constant reference to the variable to save.
     * @return true on success; false otherwise.
     */
    template <typename T>
    bool putVar(uint8_t key, const T &value)
    {
        return this->put(key, &value, sizeof(T));
    }

    /**
     * @brief Retrieves a variable of any type from the database.
     *
     * Automatically sets expected size based on type T.
     *
     * @tparam T Data type of the value to retrieve.
     * @param key 8‑bit unique identifier.
     * @param out Reference where the retrieved value will be stored.
     * @return true on success; false if key missing or type mismatch.
     */
    template <typename T>
    bool getVar(uint8_t key, T &out)
    {
        size_t size = sizeof(T);
        return this->get(key, &out, size);
    }

    /**
     * @brief Stores an Arduino String (dynamic length string) under a key.
     *
     * Correctly handles data stored on the heap, ensuring length and content are saved.
     *
     * @param key 8‑bit unique identifier.
     * @param str Constant reference to the Arduino String to save.
     * @return true if stored successfully; false otherwise.
     */
    bool putString(uint8_t key, const String &str);

    /**
     * @brief Retrieves an Arduino String stored under the given key.
     *
     * @param key 8‑bit unique identifier.
     * @return String containing the stored text; empty string if key not found or error.
     */
    String getString(uint8_t key);

private:
    void *_engine; ///< Opaque pointer to the underlying NeuLSMDB_FS engine instance.
};

/**
 * @var db
 * @brief Global pre‑created instance of NeuDB ready for direct use in Arduino sketches.
 */
extern NeuDB db;

#endif