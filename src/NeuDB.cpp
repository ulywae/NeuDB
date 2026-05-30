/**
 * @file NeuDB.cpp
 * @brief Implementation Layer for the Top‑Level NeuDB Facade Wrapper.
 * @version 1.2.2
 * @date 2026
 * @author Ulywae / Neu Embedded Ecosystem Framework
 *
 * This file provides the concrete implementation for the ultra-lean NeuDB facade,
 * passing all transactional execution pathways down to the core NeuLSMDB_FS
 * storage pipeline. It handles dynamic pointer translation, heap-safety wrappers,
 * and provides specialized buffer tracking mechanisms for dynamic Arduino String types.
 */

#include "NeuLSMDB/DB_Token.h"
#include "NeuDB.h"
#include "NeuLSMDB/NeuLSMDB_FS.h"

/**
 * @brief Constructor — Allocates the physical core storage pipeline on the heap.
 *
 * Employs the Pimpl (Pointer-to-Implementation) idiom to conceal low-level storage
 * definitions behind an opaque `void*` context mask to guarantee absolute compilation isolation.
 */
NeuDB::NeuDB()
{
    // Allocate the actual database engine in heap memory
    _engine = static_cast<void *>(new NeuLSMDB_FS());
}

/**
 * @brief Destructor — Triggers full memory reclamation of the underlying core instance.
 *
 * Ensures all volatile memory spaces are durably committed, WAL handles unmounted,
 * and primitive RTOS handles released before terminating the wrapper context scope.
 */
NeuDB::~NeuDB()
{
    // Clean up engine memory when the object is destroyed
    delete static_cast<NeuLSMDB_FS *>(_engine);
}

/**
 * @brief Initializes the core storage pipeline, mounts VFS partitions, and loads topological data.
 *
 * Implements the concrete database bootstrap sequence by forwarding configuration metrics.
 *
 * @return true if initialization, validation, and crash recovery replay succeed; false on hardware or memory fault.
 */
bool NeuDB::init()
{
    return static_cast<NeuLSMDB_FS *>(_engine)->init();
}

/**
 * @brief Direct write pathway forwarding mechanism.
 *
 * Maps execution pathways directly to the underlying core `put()` pipeline. Records are
 * sequentially committed via append-only WAL logs before mutating volatile memory tree spaces.
 *
 * @param key 16‑bit distinct destination tracking index.
 * @param data Constant pointer targeting the incoming transaction data buffer.
 * @param size Data payload scale measured in bytes.
 * @return true if write operation successfully commits; false if rejected by range or capacity guards.
 */
bool NeuDB::put(uint16_t key, const void *data, size_t size)
{
    return static_cast<NeuLSMDB_FS *>(_engine)->put(key, data, size);
}

/**
 * @brief Direct point-lookup read pathway forwarding mechanism.
 *
 * Maps execution pathways directly to the underlying core `get()` pipeline, executing tiered
 * traversals from memory trees down to binary-searched physical disk storage levels.
 *
 * @param key 16‑bit distinct source tracking query index.
 * @param out Destination pointer targeting the output allocation buffer space.
 * @param size Reference descriptor managing structural buffer thresholds and real payload sizes.
 * @return true if point-lookup hits a valid data record matching the index coordinate; false otherwise.
 */
bool NeuDB::get(uint16_t key, void *out, size_t &size)
{
    return static_cast<NeuLSMDB_FS *>(_engine)->get(key, out, size);
}

/**
 * @brief Forces immediate volatile serialization, dumping pending memory states down to permanent file sectors.
 */
void NeuDB::flush()
{
    static_cast<NeuLSMDB_FS *>(_engine)->flush();
}

/**
 * @brief Interrogates active structural levels to compile and print topological footprint reports.
 */
void NeuDB::auditLevels()
{
    static_cast<NeuLSMDB_FS *>(_engine)->auditLevels();
}

/**
 * @brief Wipes the active storage directory, truncates logging queues, and formats file systems.
 *
 * @return true if partition formatting executes successfully; false on VFS locking failures.
 */
bool NeuDB::format()
{
    return static_cast<NeuLSMDB_FS *>(_engine)->format();
}

/**
 * @brief Configures memory tree reactive constraints when total records breach capacity limits.
 *
 * @param enable True arms proactive cache evictions via tombstone injection; false enforces rejection faults.
 */
void NeuDB::setOverrideWhenFull(bool enable)
{
    static_cast<NeuLSMDB_FS *>(_engine)->setOverrideWhenFull(enable);
}

/**
 * @brief Retrieves the active operational constraint strategy for full storage conditions.
 *
 * @return true if proactive cache eviction matrix is armed; false if hard validation rejections are enforced.
 */
bool NeuDB::getOverrideWhenFull() const
{
    return static_cast<NeuLSMDB_FS *>(_engine)->getOverrideWhenFull();
}

/**
 * @brief Serializes dynamic heap‑allocated Arduino String components down to persistent blocks.
 *
 * Extracts internal character arrays and serializes raw sequences including the null-terminator
 * byte to ensure deterministic re-materialization during downstream lookup extraction.
 *
 * @param key 16‑bit distinct destination tracking index.
 * @param str Constant reference to the source text string object.
 * @return true on successful string commitment; false on failure bounds.
 */
bool NeuDB::putString(uint16_t key, const String &str)
{
    // Write string length + character data including null terminator
    return this->put(key, str.c_str(), str.length() + 1);
}

/**
 * @brief Materializes dynamic heap-allocated text entries into standard Arduino String objects.
 *
 * Leverages a strict stack allocation buffer space to secure safe, overflow-proof memory bounds.
 * Automatically handles object creation if a valid storage record hits the index block.
 *
 * @param key 16‑bit distinct source tracking query index.
 * @return String object containing the active verified record text; returns an empty instance on data miss.
 */
String NeuDB::getString(uint16_t key)
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
 * Pre-instantiated singleton exposed globally to eliminate runtime allocation overhead,
 * allowing application sketches to call core CRUD methods instantly out of the box.
 */
NeuDB db;
