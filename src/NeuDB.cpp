/**
 * @file NeuDB.cpp
 * @brief Implementation of the high‑level NeuDB wrapper class.
 *
 * This file implements a simple, user‑friendly interface that wraps the full
 * functionality of `NeuLSMDB_FS` — an LSM‑Tree database engine built on LittleFS.
 * It handles memory management of the engine instance and forwards all method
 * calls to the underlying implementation. Helper methods for Arduino `String`
 * type are also provided here for convenience.
 */

#include "NeuDB.h"
#include "NeuLSMDB_FS.h"

/**
 * @brief Constructor — Allocates and initializes the underlying LSM‑Tree engine on the heap.
 *
 * The internal engine is stored as a generic `void*` pointer to hide implementation
 * details from the user.
 */
NeuDB::NeuDB()
{
    // Allocate the actual database engine in heap memory
    _engine = static_cast<void *>(new NeuLSMDB_FS());
}

/**
 * @brief Destructor — Safely destroys the underlying engine and frees allocated memory.
 *
 * Ensures all data is properly flushed and resources (files, tasks, mutexes)
 * are released before shutdown.
 */
NeuDB::~NeuDB()
{
    // Clean up engine memory when the object is destroyed
    delete static_cast<NeuLSMDB_FS *>(_engine);
}

/**
 * @brief Initializes the database engine, mounts LittleFS, and prepares storage.
 *
 * Must be called once in `setup()` before using any other functions.
 *
 * @return true if initialization succeeded; false on filesystem or memory errors.
 */
bool NeuDB::begin()
{
    return static_cast<NeuLSMDB_FS *>(_engine)->begin();
}

/**
 * @brief Stores raw binary data under a specified key.
 *
 * Delegates directly to the engine’s `put()` method. Data is written first to
 * write‑ahead log and memtable, then persisted to LittleFS.
 *
 * @param key 8‑bit unique identifier (0–255) for the entry.
 * @param data Pointer to the data buffer to store.
 * @param size Size of the data in bytes.
 * @return true if stored successfully; false on invalid parameters or storage full.
 */
bool NeuDB::put(uint8_t key, const void *data, size_t size)
{
    return static_cast<NeuLSMDB_FS *>(_engine)->put(key, data, size);
}

/**
 * @brief Retrieves raw binary data associated with a given key.
 *
 * Delegates directly to the engine’s `get()` method. Searches memtable, cache,
 * and disk files; uses bloom filters internally for speed.
 *
 * @param key 8‑bit unique identifier of the entry to read.
 * @param out Pointer to buffer where retrieved data will be stored.
 * @param size Reference: input = buffer capacity, output = actual bytes read.
 * @return true if key exists and data was read; false if not found or error.
 */
bool NeuDB::get(uint8_t key, void *out, size_t &size)
{
    return static_cast<NeuLSMDB_FS *>(_engine)->get(key, out, size);
}

/**
 * @brief Forces all pending data to be written permanently to LittleFS and triggers compaction.
 *
 * Useful before power‑off to ensure no data is lost. Delegates to engine’s `flush()`.
 */
void NeuDB::flush()
{
    static_cast<NeuLSMDB_FS *>(_engine)->flush();
}

/**
 * @brief Runs a health check, scans all storage levels, and prints usage statistics.
 *
 * Shows entry count, file structure, and fragmentation status. Delegates to engine’s `auditLevels()`.
 */
void NeuDB::auditLevels()
{
    static_cast<NeuLSMDB_FS *>(_engine)->auditLevels();
}

/**
 * @brief Erases all stored data, deletes database files from LittleFS, and resets the engine.
 *
 * @return true if format completed successfully; false on file operation failure.
 */
bool NeuDB::format()
{
    return static_cast<NeuLSMDB_FS *>(_engine)->format();
}

/**
 * @brief Configures behavior when the database reaches maximum capacity.
 *
 * @param enable If true: automatically evict oldest entries to make space for new writes.
 *               If false: reject new writes until space is freed manually.
 */
void NeuDB::setOverrideWhenFull(bool enable)
{
    static_cast<NeuLSMDB_FS *>(_engine)->setOverrideWhenFull(enable);
}

/**
 * @brief Gets current configuration policy for full storage handling.
 *
 * @return true if auto‑eviction is enabled; false if writes are rejected when full.
 */
bool NeuDB::getOverrideWhenFull() const
{
    return static_cast<NeuLSMDB_FS *>(_engine)->getOverrideWhenFull();
}

/**
 * @brief Stores an Arduino String (dynamic length string) under a key.
 *
 * Internally stores the string content plus the null terminator, so it can be
 * reconstructed correctly when retrieved.
 *
 * @param key 8‑bit unique identifier.
 * @param str Constant reference to the Arduino String to save.
 * @return true if stored successfully; false otherwise.
 */
bool NeuDB::putString(uint8_t key, const String &str)
{
    // Write string length + character data including null terminator
    return this->put(key, str.c_str(), str.length() + 1);
}

/**
 * @brief Retrieves an Arduino String stored under the given key.
 *
 * Uses a fixed‑size stack buffer (128 bytes here — adjust as needed for your use case).
 * If the key is found, constructs and returns a String object from the stored data.
 *
 * @param key 8‑bit unique identifier.
 * @return String containing the stored text; empty string if key not found or error.
 */
String NeuDB::getString(uint8_t key)
{
    char buffer[128]; // Adjust this value to set maximum safe string length
    size_t size = sizeof(buffer);
    memset(buffer, 0, size);

    if (this->get(key, buffer, size))
    {
        return String(buffer);
    }
    return String(""); // Return empty string if key does not exist
}

/**
 * @var db
 * @brief Global pre‑created instance of NeuDB ready for direct use in Arduino sketches.
 *
 * This global object allows users to immediately call methods like `db.begin()`,
 * `db.putVar()`, etc., without manually creating an instance.
 */
NeuDB db;