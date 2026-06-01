/**
 * @file NeuDB.cpp
 * @brief Implementation Layer for the Top‑Level NeuDB Facade Wrapper.
 * @version 2.1.0
 * @date 2026
 * @author Ulywae / Neu Embedded Ecosystem Framework
 *
 * This file provides the concrete implementation for the ultra-lean NeuDB facade,
 * passing all transactional execution pathways down to the core NeuLSMDB
 * storage pipeline. It handles dynamic pointer translation, heap-safety wrappers,
 * and provides specialized buffer tracking mechanisms for dynamic Arduino String types.
 */

#include "NeuLSMDB/DB_Token.h"
#include "NeuDB.h"
#include "NeuLSMDB/NeuLSMDB.h"

/**
 * @brief Constructor — Allocates the physical core storage pipeline on the heap.
 *
 * Employs the Pimpl (Pointer-to-Implementation) idiom to conceal low-level storage
 * definitions behind an opaque `void*` context mask to guarantee absolute compilation isolation.
 */
NeuDB::NeuDB()
{
    // Allocate the actual database engine in heap memory
    _engine = static_cast<void *>(new NeuLSMDB());
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
    delete static_cast<NeuLSMDB *>(_engine);
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
    return static_cast<NeuLSMDB *>(_engine)->init();
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
    return static_cast<NeuLSMDB *>(_engine)->put(key, data, size);
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
    return static_cast<NeuLSMDB *>(_engine)->get(key, out, size);
}

/**
 * @brief Forces immediate volatile serialization, dumping pending memory states down to permanent file sectors.
 */
void NeuDB::flush()
{
    static_cast<NeuLSMDB *>(_engine)->flush();
}

/**
 * @brief Interrogates active structural levels to compile and print topological footprint reports.
 */
void NeuDB::auditLevels()
{
    static_cast<NeuLSMDB *>(_engine)->auditLevels();
}

/**
 * @brief Wipes the active storage directory, truncates logging queues, and formats file systems.
 *
 * @return true if partition formatting executes successfully; false on VFS locking failures.
 */
bool NeuDB::format()
{
    return static_cast<NeuLSMDB *>(_engine)->format();
}

/**
 * @brief Direct point-lookup delete pathway forwarding mechanism.
 *
 * Maps execution pathways directly to the underlying core `del()` pipeline, executing tiered
 * traversals from memory trees down to binary-searched physical disk storage levels.
 *
 * @param key 16－bit distinct source tracking query index.
 * @return true if point-lookup hits a valid data record matching the index coordinate; false otherwise.
 */
bool NeuDB::del(uint16_t key)
{
    // Implementation Forwarding: Pass the target key registration address down to the clean core LSM engine.
    return static_cast<NeuLSMDB *>(_engine)->del(key);
}

/**
 * @brief Configures memory tree reactive constraints when total records breach capacity limits.
 *
 * @param enable True arms proactive cache evictions via tombstone injection; false enforces rejection faults.
 */
void NeuDB::setOverrideWhenFull(bool enable)
{
    static_cast<NeuLSMDB *>(_engine)->setOverrideWhenFull(enable);
}

/**
 * @brief Retrieves the active operational constraint strategy for full storage conditions.
 *
 * @return true if proactive cache eviction matrix is armed; false if hard validation rejections are enforced.
 */
bool NeuDB::getOverrideWhenFull() const
{
    return static_cast<NeuLSMDB *>(_engine)->getOverrideWhenFull();
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

// =================================================================
// OPAQUE PASS-THROUGH FOR LOGGING (PIMPL FORWARDING LIFECYCLE)
// =================================================================

bool NeuDB::putLog(uint16_t id, const void *data, size_t size)
{
    // Implementation Forwarding: Cast the opaque context pointer directly to the real
    // implementation class to invoke the 32-bit virtual layout pipeline safely.
    return static_cast<NeuLSMDB *>(_engine)->putLog(id, data, size);
}

bool NeuDB::getLog(uint16_t id, void *out, size_t &size)
{
    // Route Delegation: Forward the Point-Lookup query channel to resolve the single newest mutation frame.
    return static_cast<NeuLSMDB *>(_engine)->getLog(id, out, size);
}

bool NeuDB::getLog(uint16_t id, uint16_t index, void *out, size_t &size)
{
    // Alignment Safety: Pass the 16-bit expanded index slot coordinate downstream.
    // This bridges the clean external user API with the internal 11-bit/14-bit rolling mask.
    return static_cast<NeuLSMDB *>(_engine)->getLog(id, index, out, size);
}

size_t NeuDB::getTotalLog(uint16_t id)
{
    // Metrics Accumulation Pass: Invoke the isolated stack-allocated deduplication counter track.
    return static_cast<NeuLSMDB *>(_engine)->getTotalLog(id);
}

bool NeuDB::deleteLog(uint16_t id)
{
    // Reactive Eviction Pass: Inject transactional cancel markers at the targeted object track.
    return static_cast<NeuLSMDB *>(_engine)->deleteLog(id);
}

// =================================================================
// STATEFUL ITERATOR DATA STREAMING EXTENSIONS
// =================================================================

bool NeuDB::logIterator(uint16_t id, uint16_t startIdx, uint16_t endIdx)
{
    // Memory Protection Guard: Proactively intercept and destroy any lingering historical range cursors
    // to prevent volatile memory resource leaks on the FreeRTOS heap structure.
    if (_activeLogIterator)
    {
        delete static_cast<NeuLSMDB_LogIterator *>(_activeLogIterator);
    }

    NeuLSMDB *core = static_cast<NeuLSMDB *>(_engine);

    // Heap Allocation Phase: Instantiate the real stateful tracking context on the stack partition.
    // The concrete instance pointer is securely concealed inside the opaque _activeLogIterator storage gate.
    _activeLogIterator = new NeuLSMDB_LogIterator(core, id, startIdx, endIdx);

    // Context Evaluation: Verify whether the runtime tracker context was securely armed.
    return (_activeLogIterator != nullptr);
}

bool NeuDB::nextLog()
{
    if (!_activeLogIterator)
        return false;

    // Cursor Step Advancement: Drive the internal multi-version sorting and filtering sweep loop forward.
    return static_cast<NeuLSMDB_LogIterator *>(_activeLogIterator)->next();
}

bool NeuDB::getLogValue(void *out, size_t &size)
{
    // Safety Boundary Guard: Instantly abort runtime block streaming if the tracking context is uninitialized.
    if (!_activeLogIterator)
        return false;

    // Zero-Copy Direct Forwarding: Cast back to the operational stream iterator block
    // to extract the physical telemetry data frame from the active lookup coordinate.
    return static_cast<NeuLSMDB_LogIterator *>(_activeLogIterator)->getValue(out, size);
}

uint16_t NeuDB::getLogIndex()
{
    if (!_activeLogIterator)
        return 0;

    // Coordinate Decoding Pass: Extract the raw circular slot sequence number from the active register tracking state.
    return static_cast<NeuLSMDB_LogIterator *>(_activeLogIterator)->getIndex();
}

uint32_t NeuDB::getLogTimestamp()
{
    if (!_activeLogIterator)
        return 0;

    // Temporal Resolution Pass: Extract the cached millisecond epoch assigned during transaction registration.
    return static_cast<NeuLSMDB_LogIterator *>(_activeLogIterator)->getTimestamp();
}

void NeuDB::closeLog()
{
    if (_activeLogIterator)
    {
        // Deallocation Phase: Purge the active heap-allocated range cursor cleanly.
        delete static_cast<NeuLSMDB_LogIterator *>(_activeLogIterator);

        // Reset the pointer reference to a secure null state boundary to prevent fatal dangling reference traps.
        _activeLogIterator = nullptr;
    }
}

// =================================================================
// SYSTEM BULK DATA EXPORT / COMPACT WIRE-STREAMING EXTESIONS
// =================================================================

/**
 * @brief Streams the entire active regular Key‑Value dataset out to an external channel.
 *
 * Invokes a cascading sweep scan across volatile memory and physical storage levels.
 * Serializes live mutations into a zero-padding 4-byte packed wire header protocol.
 *
 * @param targetStream Pointer to an active Arduino Stream instance (e.g., &Serial, &backupFile).
 * @return true if entries were successfully extracted and serialized; false otherwise.
 */
bool NeuDB::exportKeyValuesToStream(Stream *targetStream)
{
    if (!_engine || !targetStream)
        return false;

    // Local structural bridge to translate concrete datasets back into a binary serialization wire
    auto kvBridge = [](uint32_t rawKey, const uint8_t *data, size_t size, void *arg)
    {
        Stream *output = static_cast<Stream *>(arg);
        if (!output)
            return;

#pragma pack(push, 1)
        struct NeuKVWireHeader
        {
            uint16_t regularKey; ///< Decoded clean 16-bit distinct dictionary index
            uint16_t dataLength; ///< Explicit payload footprint dimension bounding the trailing block
        };
#pragma pack(pop)

        NeuKVWireHeader header = {static_cast<uint16_t>(rawKey), static_cast<uint16_t>(size)};

        // Pump atomic data packets: Stream 4-byte packet header followed immediately by raw payload block
        output->write(reinterpret_cast<const uint8_t *>(&header), sizeof(NeuKVWireHeader));
        output->write(data, size);
    };

    // Route delegation down to the concrete internal unified reuse scanning engine
    size_t prevUsed = targetStream->available();
    static_cast<NeuLSMDB *>(_engine)->exportKVDataset(kvBridge, targetStream);

    return true;
}

/**
 * @brief Streams the entire multi‑version circular log dataset out to an external channel.
 *
 * Executes a fast lower-bound range traversal using space-optimized MVCC bitmasks.
 * Automatically decodes high-address register baseline offsets back into pure ring loop parameters.
 *
 * @param targetStream Pointer to an active Arduino Stream instance (e.g., &Serial, &telnetClient).
 * @return true if records were successfully extracted and serialized; false otherwise.
 */
bool NeuDB::exportLogsToStream(Stream *targetStream)
{
    if (!_engine || !targetStream)
        return false;

    // Local structural bridge to decode the packed 32-bit internal register track before wire streaming
    auto logBridge = [](uint32_t rawKey, const uint8_t *data, size_t size, void *arg)
    {
        Stream *output = static_cast<Stream *>(arg);
        if (!output)
            return;

#pragma pack(push, 1)
        struct NeuLogWireHeader
        {
            uint16_t logObjectId;   ///< Human-readable original telemetry tracking ID
            uint16_t circularIndex; ///< Re-materialized native 14-bit rolling sequence marker
            uint16_t dataLength;    ///< Explicit payload footprint dimension bounding the trailing block
        };
#pragma pack(pop)

        // Bitwise Component Unpacking: Decode the high-offset virtual coordinate space automatically
        uint32_t logSection = rawKey - NEU_LOG_KEY_OFFSET;
        uint16_t originalId = static_cast<uint16_t>(logSection >> NEU_LOG_INDEX_BITS);
        uint16_t originalIndex = static_cast<uint16_t>(logSection & NEU_LOG_INDEX_MASK);

        NeuLogWireHeader header = {originalId, originalIndex, static_cast<uint16_t>(size)};

        // Serialise packed structural packets directly onto the external non-blocking physical bus link
        output->write(reinterpret_cast<const uint8_t *>(&header), sizeof(NeuLogWireHeader));
        output->write(data, size);
    };

    // Forward the streaming execution context to the internal unified reuse scanning engine
    static_cast<NeuLSMDB *>(_engine)->exportLogDataset(logBridge, targetStream);

    return true;
}

/**
 * @var db
 * @brief Global pre‑created instance of NeuDB ready for direct use in Arduino sketches.
 *
 * Pre-instantiated singleton exposed globally to eliminate runtime allocation overhead,
 * allowing application sketches to call core CRUD methods instantly out of the box.
 */
NeuDB db;
