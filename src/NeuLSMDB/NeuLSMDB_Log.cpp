#include "NeuLSMDB/DB_Token.h"
#include "NeuLSMDB/NeuLSMDB.h"

#include <Arduino.h>
#include <list>
#include <memory>
#include <cstring>
#include <algorithm>

// =================================================================
// TYPE ABSTRACTION LAYER: LOG PIPELINE OPAQUE POINTER MAPPING
// =================================================================
#define GET_LEVELS_LOG() static_cast<std::vector<NeuLSMDB::SSTFile> *>(_levelsLog)
#define GET_MEM() static_cast<std::map<uint32_t, NeuLSMDB::MemEntry> *>(_mem)
#define GET_LEVELS() static_cast<std::vector<NeuLSMDB::SSTFile> *>(_levels)

// =================================================================
// INTERNAL HELPER: AUTOMATIC LATEST INDEX & TIMESTAMP FINDER
// =================================================================
bool NeuLSMDB::findLatestLogIndex(uint16_t id, uint16_t &outIndex, uint32_t &outTs)
{
    // Bitwise packing: Shift the 16-bit object ID into the upper register of a 32-bit virtual coordinate space.
    // This isolates each ID's sequential history boundary to prevent runtime memory address fragmentation.
    uint32_t keyStart = NEU_LOG_KEY_OFFSET + ((uint32_t)id << NEU_LOG_INDEX_BITS);
    uint32_t keyEnd = keyStart + NEU_LOG_INDEX_MASK;

    uint32_t maxTs = 0;
    uint32_t bestKey = 0;
    bool found = false;

    // Phase 1: Volatile Memory Read Path (RAM MemTable Lookup)
    // Perform a binary tree lower-bound seek to intercept un-flushed transaction records.
    auto &mapMem = *GET_MEM();
    auto itMem = mapMem.lower_bound(keyStart);
    while (itMem != mapMem.end() && itMem->first <= keyEnd)
    {
        if (itMem->second.ts > maxTs || (itMem->second.ts == maxTs && itMem->first > bestKey))
        {
            maxTs = itMem->second.ts;
            bestKey = itMem->first;
            found = true;
        }
        ++itMem;
    }

    // Phase 2: Persistent Storage Read Path (In-Memory SSTable Metadata Log Index)
    // Scan all immutable storage levels from newest to oldest file descriptors.
    for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
    {
        auto &levelLog = GET_LEVELS_LOG()[lvl];
        for (auto itSST = levelLog.rbegin(); itSST != levelLog.rend(); ++itSST)
        {
            auto &sst = *itSST;

            // Execute an array-level binary search over the packed index vector in RAM.
            // This prevents expensive, blocking physical disk I/O cycles during coordinate checks.
            auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                          SSTIndex{keyStart, 0, 0, 0, false});

            while (idxIt != sst.index.end() && idxIt->key <= keyEnd)
            {
                if (idxIt->ts > maxTs || (idxIt->ts == maxTs && idxIt->key > bestKey))
                {
                    maxTs = idxIt->ts;
                    bestKey = idxIt->key;
                    found = true;
                }
                ++idxIt;
            }
        }
    }

    if (found)
    {
        // Decode Phase: Strip the high-address baseline offset and extract the 14-bit circular index.
        // This maps the packed 32-bit internal key structure back into a standard human-readable format.
        outIndex = (bestKey - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
        outTs = maxTs;
        return true;
    }

    return false;
}

// =================================================================
// AUTOMATIC WRITE PIPELINE: putLog() WITH DYNAMIC UPPER OFFSET
// =================================================================
bool NeuLSMDB::putLog(uint16_t id, const void *data, size_t size)
{
    // Defensive Boundary Validation: Early drop for unauthorized IDs or payload truncation risks.
    if (!_systemReady || id >= NEU_LOG_MAX_ID_LIMIT || size > 65535)
        return false;

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return false;

    // Smart Write Stall Policy: Concurrency Ingestion Brake.
    // If the background worker thread is actively merging log SSTable files, back off
    // the ingestion pipeline to prevent VFS block descriptor saturation and thread starving.
    if (__atomic_load_n(&_compactLogState, __ATOMIC_SEQ_CST) == MERGE_STREAM)
    {
        xSemaphoreGive(_mutex);
        vTaskDelay(pdMS_TO_TICKS(4)); // Yield execution control context to allow the VFS link to breathe
        if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
            return false;
    }

    bool res = false;

    do
    {
        // Storage Integrity and Memory Capacity Fence: Emergency Overwrite Protocol.
        // Prevent heap memory collapse or physical flash depletion by invoking proactive eviction
        // if live entries or sector limits breach hard-coded architectural boundaries.
        if (_flashFullGuard || __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST) >= MAX_TOTAL_ENTRIES)
        {
            if (_overrideWhenFull)
                evictOldestData();
            else
                break;
        }

        uint16_t lastIndex = 0;
        uint32_t lastTs = 0;
        uint16_t nextIndex = 0;

        // Rolling Circle Computation Layer: Circular Ring Buffer Routing.
        // Intercept the latest physical state address to advance the logical sequence.
        if (findLatestLogIndex(id, lastIndex, lastTs))
        {
            // Apply a strict modulo operation over the binary index ceiling to achieve the rolling loop.
            nextIndex = (lastIndex + 1) % NEU_LOG_MAX_INDEX;
        }
        else
        {
            nextIndex = 0; // Baseline initial state assignment for a newly registered tracking ID
        }

        // Bitwise Component Packing Phase: Unified Virtual Key Serialization.
        // Construct the immutable 32-bit destination coordinate using the high-address offset baseline.
        uint32_t virtualKey = NEU_LOG_KEY_OFFSET + ((uint32_t)id << NEU_LOG_INDEX_BITS) + (uint32_t)nextIndex;

        // Core Persistence Ingestion: Fault-Tolerant Write-Ahead Log (WAL) Serialization.
        // Commit transactions to disk immediately to guarantee power-loss crash protection.
        int retryWAL = 0;
        bool walSuccess = false;
        while (retryWAL < 10)
        {
            if (appendWAL(virtualKey, data, size, false))
            {
                walSuccess = true;
                break;
            }
            xSemaphoreGive(_mutex);
            vTaskDelay(pdMS_TO_TICKS(2)); // Back off temporarily to allow outstanding flash sector writes to finish
            if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
                break;
            retryWAL++;
        }

        if (!walSuccess)
            break;

        // Volatile Memory Mapping Phase: In-Memory Ingestion Layer (RAM MemTable Injection).
        // Isolate log records cleanly by checking collision parameters inside the active transaction tree.
        uint32_t now = millis();
        auto &mapMem = *GET_MEM();

        auto it = mapMem.find(virtualKey);
        if (it != mapMem.end())
        {
            // Mutation Path: Replace preexisting memory descriptors directly at the circular slot target.
            _memBytes -= it->second.size;
            it->second.value.reset(new uint8_t[size]);
            if (data && size > 0)
                memcpy(it->second.value.get(), data, size);
            it->second.size = size;
            it->second.ts = now;
            it->second.tombstone = (size == 0);
            _memBytes += size;
        }
        else
        {
            // Allocation Path: Inject a brand new transaction record node into the logical index map.
            MemEntry e;
            if (size > 0 && data)
            {
                e.value.reset(new uint8_t[size]);
                memcpy(e.value.get(), data, size);
            }
            e.size = size;
            e.ts = now;
            e.tombstone = (size == 0);

            mapMem[virtualKey] = std::move(e);
            __atomic_add_fetch(&_memCount, 1, __ATOMIC_SEQ_CST); // Update atomic counters for resource tracking
            _memBytes += size;
            __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
        }

        res = true;
    } while (0);

    xSemaphoreGive(_mutex);
    return res;
}

// =================================================================
// READ PIPELINE: Point-Lookup Data Retrieval (Latest State)
// =================================================================
bool NeuLSMDB::getLog(uint16_t id, void *out, size_t &size)
{
    if (!_systemReady)
        return false;

    // Fast-Fail Optimization Layer: Instantly drop queries that violate configuration limits.
    // This short-circuits the pipeline before executing heavy thread-blocking operations.
    if (id >= NEU_LOG_MAX_ID_LIMIT)
    {
        size = 0;
        return false;
    }

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return false;

    bool found = false;

    do
    {
        uint16_t latestIndex = 0;
        uint32_t latestTs = 0;

        // Sequence Resolution Phase: Locate the circular buffer index holding the newest timestamp.
        // This abstracts the multi-way lookup logic out of the core structural extraction path.
        if (!findLatestLogIndex(id, latestIndex, latestTs))
            break;

        // Mutex Handover Protocol: Prevent deadlocks during overloaded functional routing.
        // Release the global lock *before* triggering the sub-query method. This bypasses
        // internal re-entrancy constraints and keeps the multi-core execution path non-blocking.
        xSemaphoreGive(_mutex);
        found = getLog(id, latestIndex, out, size);
        return found;

    } while (0);

    xSemaphoreGive(_mutex);
    return found;
}

// =================================================================
// READ PIPELINE: Targeted Index Record Retrieval
// =================================================================
bool NeuLSMDB::getLog(uint16_t id, uint16_t index, void *out, size_t &size)
{
    if (!_systemReady || id >= NEU_LOG_MAX_ID_LIMIT || index >= NEU_LOG_MAX_INDEX)
        return false;

    // ========================================================================
    // ADAPTIVE READ-STALL POLICY: LOG PIPELINE DESCRIPTOR SATURATION BRAKE
    // ========================================================================
    // If the active log SSTable density within Level 0 breaches hard-coded boundaries
    // due to write-heavy log bombardments, dynamically throttle the point-lookup
    // pipeline execution context. This yields CPU cycles to the background maintenance
    // worker thread, mitigating Virtual File System (VFS) block descriptor saturation
    // and preventing severe Multi-Core resource lock contentions over the database mutex.
    size_t logLevel0Density = GET_LEVELS_LOG()[0].size();

    if (logLevel0Density >= 16)
    {
        // Critical Saturation Boundary: Force a 4-millisecond reactive backoff window
        // to allow the background K-Way Merge worker to clear out Level 0 file shards.
        vTaskDelay(pdMS_TO_TICKS(4));
    }
    else if (logLevel0Density >= 8)
    {
        // Elevated Pressure Alert: Execute a brief 1ms cooperative multitask yield
        // to appease the ESP32 Task Watchdog Timer (TWDT) boundaries.
        vTaskDelay(pdMS_TO_TICKS(1));
    }

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return false;

    bool seek = false;

    do
    {
        // Coordinate Translation: Map the target ID and index into an absolute 32-bit virtual key.
        // This bridges standard human-readable inputs with our custom upper-offset storage topology.
        uint32_t virtualKey = NEU_LOG_KEY_OFFSET + (((uint32_t)id << NEU_LOG_INDEX_BITS) | index);

        // Stage 1: Volatile Read Path (Active In-Memory MemTable Resolution)
        // Check un-flushed transaction maps first to avoid unnecessary non-volatile layer iteration.
        auto &mapMem = *GET_MEM();
        auto it = mapMem.find(virtualKey);
        if (it != mapMem.end())
        {
            // Tombstone Interception: Instantly abort and return a logical miss if data has been deleted.
            if (it->second.tombstone)
            {
                size = 0;
                break;
            }

            // Memory Protection: Enforce hard buffer allocation boundaries before moving data blocks.
            size_t requiredSize = it->second.size;
            if (size < requiredSize)
            {
                size = requiredSize; // Communicate the missing size constraint to the interface layer
                break;               // Prevent buffer overflow by dropping the transfer
            }

            if (requiredSize > 0 && out && it->second.value)
            {
                memcpy(out, it->second.value.get(), requiredSize);
            }
            size = requiredSize;
            seek = true;
            break;
        }

        // Stage 2: Non-Volatile Storage Resolution (Multi-Level SSTable Index Sweep)
        // Query the dedicated log snapshot layers from the newest block cascade down to cold states.
        for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
        {
            auto &levelLog = GET_LEVELS_LOG()[lvl];
            for (auto itSST = levelLog.rbegin(); itSST != levelLog.rend(); ++itSST)
            {
                auto &sst = *itSST;

                // Probabilistic Pre-Filtering: Invoke the hardware-assisted Bloom Filter matrix.
                // This eliminates expensive array iteration loops if the key is guaranteed absent.
                if (!bloomCheck(sst.bloom, virtualKey))
                    continue;

                // Index-Level Resolution: Perform a binary search lookahead on the vector array in RAM.
                auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                              SSTIndex{virtualKey, 0, 0, 0, false});

                if (idxIt != sst.index.end() && idxIt->key == virtualKey)
                {
                    if (idxIt->tombstone)
                    {
                        size = 0;
                        goto end_get_log;
                    }

                    // Pre-I/O Validation: Intercept sizing discrepancies before opening file sectors.
                    if (size < idxIt->size)
                    {
                        size = idxIt->size;
                        goto end_get_log;
                    }

                    size_t tmp = size;
                    // Physical Read Path: Extract data sectors from flash disk via unified block read routing.
                    if (readSST(sst, *idxIt, out, tmp))
                    {
                        size = tmp;
                        seek = true;
                        goto end_get_log;
                    }
                }
            }
        }
    end_get_log:;
    } while (0);

    xSemaphoreGive(_mutex);
    return seek;
}

// =================================================================
// SYSTEM METRICS: getTotalLog() Per ID (DYNAMIC BOUNDARY COMPLIANT)
// =================================================================
size_t NeuLSMDB::getTotalLog(uint16_t id)
{
    if (!_systemReady || id >= NEU_LOG_MAX_ID_LIMIT)
        return 0;

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return 0;

    size_t activeCount = 0;

    do
    {
        // Coordinate Translation Layer: Map target ID to its exact virtual 32-bit register boundaries
        uint32_t keyStart = NEU_LOG_KEY_OFFSET + ((uint32_t)id << NEU_LOG_INDEX_BITS);
        uint32_t keyEnd = keyStart + NEU_LOG_INDEX_MASK;

        // Dynamic MVCC Bitmap: Space-optimized 1-bit allocation per circular tracking slot
        std::vector<bool> discoveredSlots(NEU_LOG_MAX_INDEX, false);

        // Stage 1: Volatile Cache Scan (RAM MemTable Log Iteration)
        // Execute a fast lower-bound tree traversal to count fresh un-flushed states first.
        auto &mapMem = *GET_MEM();
        auto itMem = mapMem.lower_bound(keyStart);
        while (itMem != mapMem.end() && itMem->first <= keyEnd)
        {
            // Decode the index coordinates to extract the specific circular slot position.
            uint32_t idx = (itMem->first - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;

            if (idx < NEU_LOG_MAX_INDEX && !discoveredSlots[idx])
            {
                discoveredSlots[idx] = true; // Lock slot allocation context at its freshest state

                if (!itMem->second.tombstone)
                    activeCount++;
            }
            ++itMem;
        }

        // Stage 2: Persistent Storage Lookahead (In-Memory SSTable Metadata Index Scan)
        // Sweep immutable storage descriptors chronologically backwards from newest file states to old segments.
        for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
        {
            auto &levelLog = GET_LEVELS_LOG()[lvl];
            for (auto itSST = levelLog.rbegin(); itSST != levelLog.rend(); ++itSST)
            {
                auto &sst = *itSST;

                auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                              SSTIndex{keyStart, 0, 0, 0, false});

                while (idxIt != sst.index.end() && idxIt->key <= keyEnd)
                {
                    uint32_t idx = (idxIt->key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;

                    // Historical Multi-Version Concurrency Control (MVCC) Filter Fence:
                    // If a slot is already found at the top (newer) level, skip this obsolete state.
                    if (idx < NEU_LOG_MAX_INDEX && !discoveredSlots[idx])
                    {
                        discoveredSlots[idx] = true; // Commit structural occupancy map

                        if (!idxIt->tombstone)
                            activeCount++;
                    }
                    ++idxIt;
                }
            }
        }

    } while (0);

    xSemaphoreGive(_mutex);
    return activeCount;
}

// =================================================================
// TRANSACTION PURGE: deleteLog() Per ID via Dynamic Tombstones
// =================================================================
bool NeuLSMDB::deleteLog(uint16_t id)
{
    if (!_systemReady || id >= NEU_LOG_MAX_ID_LIMIT)
        return false;

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return false;

    bool res = false;

    do
    {
        // Coordinate Translation Layer: Compute absolute 32-bit register addresses.
        // This targets the exact virtual key boundary mapped to the specific object ID.
        uint32_t keyStart = NEU_LOG_KEY_OFFSET + ((uint32_t)id << NEU_LOG_INDEX_BITS);
        uint32_t keyEnd = keyStart + NEU_LOG_INDEX_MASK;
        uint32_t now = millis();
        auto &mapMem = *GET_MEM();

        // Dynamic Tracking Allocation: Instantiate an optimized boolean tracker on the stack.
        // The array size scales dynamically with NEU_LOG_MAX_INDEX to minimize heap pressure during execution.
        std::vector<bool> killSlots(NEU_LOG_MAX_INDEX, false);
        bool anyActive = false;

        // Stage 1: Volatile Layer Sweep (RAM MemTable Invalidation Search)
        // Perform a quick tree lower-bound traversal to intercept active un-flushed states.
        auto itMem = mapMem.lower_bound(keyStart);
        while (itMem != mapMem.end() && itMem->first <= keyEnd)
        {
            if (!itMem->second.tombstone)
            {
                uint32_t idx = (itMem->first - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
                if (idx < NEU_LOG_MAX_INDEX)
                {
                    killSlots[idx] = true;
                    anyActive = true;
                }
            }
            ++itMem;
        }

        // Stage 2: Persistent Index Sweep (Immutable SSTable Metadata Log Index Search)
        // Traverse RAM-cached SSTable index blocks to locate lingering persistent history segments.
        for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
        {
            for (const auto &sst : GET_LEVELS_LOG()[lvl])
            {
                auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                              SSTIndex{keyStart, 0, 0, 0, false});
                while (idxIt != sst.index.end() && idxIt->key <= keyEnd)
                {
                    if (!idxIt->tombstone)
                    {
                        uint32_t idx = (idxIt->key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
                        if (idx < NEU_LOG_MAX_INDEX)
                        {
                            killSlots[idx] = true;
                            anyActive = true;
                        }
                    }
                    ++idxIt;
                }
            }
        }

        // Idempotency Shortcut Rule: Fast-exit if no live records exist across RAM or disk layers.
        // This avoids writing redundant cancel markers and saves non-volatile memory block endurance.
        if (!anyActive)
        {
            res = true;
            break;
        }

        // Stage 3: Tombstone Serialization Layer (Atomic Purge Execution)
        // Inject empty data frames across all identified historical slot index positions.
        for (uint32_t idx = 0; idx < NEU_LOG_MAX_INDEX; idx++)
        {
            if (killSlots[idx])
            {
                // Reconstruct the 32-bit coordinate by bitwise merging the slot index with the base address.
                uint32_t virtualKey = keyStart | idx;

                // Commit the transactional override log record directly into the physical write-ahead channel
                // to maintain strict persistence and fault tolerance across sudden hardware power cuts.
                appendWAL(virtualKey, nullptr, 0, true);

                MemEntry tombEntry;
                tombEntry.size = 0;
                tombEntry.ts = now;
                tombEntry.tombstone = true;

                auto it = mapMem.find(virtualKey);
                if (it != mapMem.end())
                {
                    _memBytes -= it->second.size;
                    mapMem[virtualKey] = std::move(tombEntry);
                }
                else
                {
                    mapMem[virtualKey] = std::move(tombEntry);
                    __atomic_add_fetch(&_memCount, 1, __ATOMIC_SEQ_CST);
                }

                // Balance atomic counters cleanly by decrementing live system entries
                size_t currentTotal = __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST);
                if (currentTotal > 0)
                    __atomic_sub_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
            }
        }

        res = true;
    } while (0);

    xSemaphoreGive(_mutex);
    return res;
}

// =================================================================
// REORGANIZATION ENGINE: ASYNCHRONOUS LOG ROLLING COMPACTION
// =================================================================

void NeuLSMDB::runLogCompactionScheduler()
{
    CompactState st = __atomic_load_n(&_compactLogState, __ATOMIC_SEQ_CST);
    if (st != IDLE)
        return;

    // Boundary Evaluation Phase: Trigger log compaction once Level 0 file density exceeds the limit.
    // This mitigates long lookup latency spikes by enforcing chronological level consolidation.
    if (GET_LEVELS_LOG()[0].size() > 6)
    {
        // Concurrency Guard: Utilize atomic Compare-And-Swap (CAS) to securely acquire the task lock.
        // This coordinates background routines safely across multi-core environments without thread collisions.
        CompactState exp = IDLE;
        if (__atomic_compare_exchange_n(&_compactLogState, &exp, MERGE_STREAM, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
        {
            // Transaction Job Allocation: Instantiate structural track parameters for the log compaction pool.
            _jobLog->srcLevel = 0;
            _jobLog->srcFiles.clear();
            _jobLog->readers.clear();
            _jobLog->active = true;

            // Enqueue all files from Level 0 and the overlapping lower Level 1 for merging.
            for (auto &sst : GET_LEVELS_LOG()[0])
                _jobLog->srcFiles.push_back(sst.filename);
            for (auto &sst : GET_LEVELS_LOG()[1])
                _jobLog->srcFiles.push_back(sst.filename);

            // Establish file stream readers for every descriptor in the targeted compaction cascade.
            for (auto &fn : _jobLog->srcFiles)
            {
                SourceReader r;
                if (r.open(fn))
                    _jobLog->readers.push_back(std::move(r));
                else
                    r.close(); // Descriptor Leak Protection: Close corrupt or missing block links instantly.
            }

            // Build temporary tracking file paths to support atomic renaming protocols during commit phases.
            uint32_t fileId = getFileSeq();
            _jobLog->dstTemp = makeFilename(1, fileId) + ".logtmp";
            _jobLog->dstFinal = "/lsm/log_lv1_" + String(fileId) + ".sst";
        }
    }
}

void NeuLSMDB::tickCompactLog()
{
    CompactState state = __atomic_load_n(&_compactLogState, __ATOMIC_SEQ_CST);
    if (state != MERGE_STREAM)
        return;

    // Stage 1: Priority Queue Ingestion (Multi-Way Merge Priority Queue Bootstrapping)
    // Build the sorted stream tree in volatile memory during the initial tick execution.
    if (!_compactLogInitialized)
    {
        while (!_compactHeap.empty())
            _compactHeap.pop();

        for (size_t i = 0; i < _jobLog->readers.size(); i++)
        {
            auto &r = _jobLog->readers[i];
            if (!r.eof)
                _compactHeap.push({r.current.key, r.current.ts, i, r.current.offset, r.version});
        }
        _compactLogInitialized = true;
    }

    File out = STORAGE_OPEN(_jobLog->dstTemp, "a");
    if (!out)
    {
        __atomic_store_n(&_compactLogState, IDLE, __ATOMIC_SEQ_CST);
        _compactLogInitialized = false;
        _jobLog->active = false;
        return;
    }

    // Resource Management Constraint: Compute byte limits based on the configured runtime budget.
    // This allows the task to slice long physical file writes into non-blocking dynamic intervals.
    size_t budget = COMPACT_BUDGET_KB * 1024;
    size_t written = 0;

    // State Tracking Register: Maintain temporal references to coordinate global circular loop metrics.
    uint32_t lastProcessedId = 0xFFFFFFFF;
    uint32_t historyCounter = 0;

    // Stage 2: Data Consolidation Loop (K-Way Merge Sort Iteration Phase)
    while (!_compactHeap.empty() && written < budget)
    {
        HeapEntry top;
        bool found = false;
        while (!_compactHeap.empty())
        {
            top = _compactHeap.top();
            _compactHeap.pop();
            auto &r = _jobLog->readers[top.readerIdx];
            if (r.version == top.version && r.current.offset == top.offset)
            {
                found = true;
                break;
            }
        }
        if (!found)
            continue;

        uint32_t key = top.key;
        uint32_t currentId = key >> NEU_LOG_INDEX_BITS; // Bitwise Unpacking: Extract the native 16-bit object ID.

        // Collect matching virtual keys from all open source streams to settle collision states.
        std::vector<size_t> group;
        for (size_t i = 0; i < _jobLog->readers.size(); i++)
        {
            auto &r = _jobLog->readers[i];
            if (!r.eof && r.current.key == key)
                group.push_back(i);
        }
        if (group.empty())
            continue;

        // Internal Deduplication Path: Evaluate and elect the entry with the highest timestamp.
        uint32_t bestTs = 0;
        size_t winnerIdx = group[0];
        bool first = true;
        for (size_t idx : group)
        {
            auto &r = _jobLog->readers[idx];
            if (first || r.current.ts > bestTs)
            {
                bestTs = r.current.ts;
                winnerIdx = idx;
                first = false;
            }
        }

        auto &winner = _jobLog->readers[winnerIdx];

        // Stage 3: Rolling Circle Analytical Filtering Layer
        // Reset metrics immediately upon encountering a new object boundary within the sequential index.
        if (currentId != lastProcessedId)
        {
            lastProcessedId = currentId;
            historyCounter = 0;
        }

        historyCounter++;

        // Enforce the dynamic historical ceiling fence tracked via NEU_MAX_LOG_HISTORY.
        // This drops expired trailing slots permanently from disk during the merge write pass.
        bool discardDueToRolling = (historyCounter > NEU_MAX_LOG_HISTORY);

        if (winner.current.size > 0 && !discardDueToRolling && !winner.current.tombstone)
        {
            if (_compactLogValBuf.size() < winner.current.size)
                _compactLogValBuf.resize(winner.current.size);

            size_t actual;
            if (winner.readValue(_compactLogValBuf.data(), actual))
            {
                SSTHeader header;
                header.key = key;
                header.size = winner.current.size;
                header.ts = bestTs;
                header.tombstone = 0;

                out.write((const uint8_t *)&header, sizeof(SSTHeader));
                out.write(_compactLogValBuf.data(), header.size);
                written += sizeof(SSTHeader) + header.size;
            }
        }
        else if (winner.current.tombstone && !discardDueToRolling)
        {
            // Retention Rule: Maintain active tombstone cancellations if they remain inside valid history frames.
            SSTHeader header;
            header.key = key;
            header.size = 0;
            header.ts = bestTs;
            header.tombstone = 1;
            out.write((const uint8_t *)&header, sizeof(SSTHeader));
            written += sizeof(SSTHeader);
        }

        // Advance all read pointer coordinates belonging to the processed duplication set.
        for (size_t idx : group)
        {
            auto &r = _jobLog->readers[idx];
            r.next();
            if (!r.eof)
                _compactHeap.push({r.current.key, r.current.ts, idx, r.current.offset, r.version});
        }
    }

    out.close();

    // Stage 4: Commit and Block Index Finalization
    bool allDone = true;
    for (auto &r : _jobLog->readers)
    {
        if (!r.eof)
        {
            allDone = false;
            break;
        }
    }

    if (allDone)
    {
        for (auto &r : _jobLog->readers)
            r.close();
        _jobLog->readers.clear();

        // Execute an atomic VFS rename transaction to safely register the consolidated SSTable on the file system.
        if (STORAGE_RENAME(_jobLog->dstTemp, _jobLog->dstFinal))
        {
            File f = STORAGE_OPEN(_jobLog->dstFinal, "r");
            if (f)
            {
                std::vector<SSTIndex> idx;
                SSTFile sst;
                memset(sst.bloom, 0, sizeof(sst.bloom));

                // SYNC FILE ID FROM dstFinal
                int lastUnderscore = _jobLog->dstFinal.lastIndexOf('_');
                int lastDot = _jobLog->dstFinal.lastIndexOf('.');
                if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore)
                {
                    sst.fileId = _jobLog->dstFinal.substring(lastUnderscore + 1, lastDot).toInt();
                }
                else
                {
                    sst.fileId = getFileSeq();
                }
                strncpy(sst.filename, _jobLog->dstFinal.c_str(), sizeof(sst.filename) - 1);
                sst.filename[sizeof(sst.filename) - 1] = '\0';

                while (f.available())
                {
                    uint32_t currentEntryOffset = f.position();

                    SSTHeader readHeader;
                    if (f.read((uint8_t *)&readHeader, sizeof(SSTHeader)) != sizeof(SSTHeader))
                        break;

                    SSTIndex entry;
                    entry.key = readHeader.key;
                    entry.offset = currentEntryOffset;
                    entry.size = readHeader.size;
                    entry.ts = readHeader.ts;
                    entry.tombstone = (readHeader.tombstone == 1);

                    bloomAdd(sst.bloom, entry.key);

                    if (entry.size > 0)
                    {
                        f.seek(entry.size, SeekCur);
                    }

                    idx.push_back(entry);

                    if (!entry.tombstone)
                        __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
                }
                f.close();

                std::sort(idx.begin(), idx.end());
                sst.index = std::move(idx);

                if (xSemaphoreTake(_mutex, portMAX_DELAY) == pdTRUE)
                {
                    GET_LEVELS_LOG()[1].push_back(std::move(sst));
                    xSemaphoreGive(_mutex);
                }
            }

            // Flush old file nodes securely from disk and clean up redundant temporary states
            deleteSSTLogFiles(_jobLog->srcFiles);
        }
        else
        {
            STORAGE_REMOVE(_jobLog->dstTemp);
        }

        __atomic_store_n(&_compactLogState, IDLE, __ATOMIC_SEQ_CST);
        _jobLog->active = false;
        _compactLogInitialized = false;
    }
}

// =================================================================
// LOG DISK HARDWARE FILE DRIVERS (I/O PIPELINE HELPER METHODS)
// =================================================================

// 1. Storage Recovery Sequence: Reconstruct operational system metadata log maps from non-volatile storage during bootstrap.
void NeuLSMDB::loadAllSSTLog()
{
    File root = STORAGE_OPEN("/lsm", "r");
    if (!root || !root.isDirectory())
        return;

    File f = root.openNextFile();
    while (f)
    {
        String nm = f.name();

        // VFS Absolute Path Sanitization: Extract the raw filename string
        // to bypass platform-specific absolute path nesting issues.
        if (nm.lastIndexOf('/') != -1)
            nm = nm.substring(nm.lastIndexOf('/') + 1);

        // Strict Filter Boundary: Isolate dedicated log tables matching the 'log_lvX_Y.sst' design template.
        if (nm.endsWith(".sst") && nm.startsWith("log_lv"))
        {
            int lvl = nm.substring(6, nm.indexOf('_')).toInt();

            if (lvl >= 0 && lvl < MAX_LEVEL)
            {
                String fullPath = "/lsm/" + nm;

                SSTFile sst;
                strncpy(sst.filename, fullPath.c_str(), sizeof(sst.filename) - 1);
                sst.filename[sizeof(sst.filename) - 1] = '\0';

                int lastUnderscore = nm.lastIndexOf('_');
                int lastDot = nm.lastIndexOf('.');
                if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore)
                    sst.fileId = nm.substring(lastUnderscore + 1, lastDot).toInt();
                else
                    sst.fileId = getFileSeq(); // Fallback protocol: Dynamically append a fresh identifier if name blocks are broken

                memset(sst.bloom, 0, sizeof(sst.bloom));

                File fd = STORAGE_OPEN(fullPath, "r");
                if (!fd)
                {
                    f = root.openNextFile();
                    continue;
                }

                // Deserialization Pipeline: Incrementally loop across physical blocks to load the index array into RAM.
                while (fd.available())
                {
                    uint32_t currentPos = fd.position();

                    SSTHeader header;
                    if (fd.read((uint8_t *)&header, sizeof(SSTHeader)) != sizeof(SSTHeader))
                        break;

                    SSTIndex idx;
                    idx.key = header.key;
                    idx.offset = currentPos;
                    idx.size = header.size;
                    idx.ts = header.ts;
                    idx.tombstone = (header.tombstone == 1);

                    // Probabilistic Hydration: Populate the active bloom bitmask matrix during indexing.
                    bloomAdd(sst.bloom, header.key);

                    // Zero-Copy Streaming Jump: Skip over the raw data payload block to rapidly decode the next header frame.
                    fd.seek(idx.size, SeekCur);

                    sst.index.push_back(idx);

                    if (!idx.tombstone)
                        __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
                }
                fd.close();

                // Sort Constraint: Arrange the metadata index lexicographically by key to allow absolute binary search operations.
                std::sort(sst.index.begin(), sst.index.end());
                GET_LEVELS_LOG()
                [lvl].push_back(std::move(sst));
            }
        }
        f = root.openNextFile();
    }
    root.close();
}

// 2. Storage Serialization Pipeline: Commit volatile RAM MemTable entries into immutable Level 0 block tables on flash.
bool NeuLSMDB::writeSSTLog(uint8_t level, const std::map<uint32_t, MemEntry> &entries, const String &dstFile)
{
    if (entries.empty())
        return true;

    uint32_t fid = 0;
    String fn;

    // File Target Resolver: Re-use an assigned destination name string (compaction path)
    // or dynamically generate a brand new Level 0 state block from our unique sequence identifier.
    if (dstFile.length() > 0)
    {
        fn = dstFile;
        int lastUnderscore = dstFile.lastIndexOf('_');
        int lastDot = dstFile.lastIndexOf('.');
        if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore)
            fid = dstFile.substring(lastUnderscore + 1, lastDot).toInt();
        else
            fid = getFileSeq();
    }
    else
    {
        fid = getFileSeq();
        // Structural Isolation Protocol: Enforce a dedicated filename prefix format.
        // This isolates log blocks from regular tree data cascades to ensure strict data track separation.
        fn = "/lsm/log_lv" + String(level) + "_" + String(fid) + ".sst";
    }

    File f = STORAGE_OPEN(fn, "w");
    if (!f)
        return false;

    std::vector<SSTIndex> idx;
    SSTFile sstOut;
    strncpy(sstOut.filename, fn.c_str(), sizeof(sstOut.filename) - 1);
    sstOut.filename[sizeof(sstOut.filename) - 1] = '\0';
    sstOut.fileId = fid;
    memset(sstOut.bloom, 0, sizeof(sstOut.bloom));

    // Transaction Conversion Pass: Linear sweep across the 32-bit map entries to serialize binary data blocks.
    for (auto &kv : entries)
    {
        uint32_t k = kv.first; // Retain the packed 32-bit virtual coordinate for accurate multi-version tracking.
        const MemEntry &e = kv.second;
        uint32_t currentPos = f.position();

        SSTHeader header;
        header.key = k;
        header.size = e.size;
        header.ts = e.ts;
        header.tombstone = e.tombstone ? 1 : 0;

        // Metadata Commit Phase: Directly stream fixed-length layout frames onto non-volatile target blocks.
        if (f.write((const uint8_t *)&header, sizeof(SSTHeader)) != sizeof(SSTHeader))
        {
            f.close();
            return false;
        }

        // Payload Commit Phase: Write raw telemetry payload chunks into flash sectors directly trailing the header.
        if (header.size > 0 && e.value)
        {
            if (f.write(e.value.get(), header.size) != header.size)
            {
                f.close();
                return false;
            }
        }

        // Memory Index Construction: Map runtime file-pointer coordinates for accelerated RAM lookup lookaheads.
        // Concurrently registers the key into the probabilistic Bloom Filter bitmask matrix to save subsequent disk I/O.
        SSTIndex si;
        si.key = k;
        si.offset = currentPos;
        si.size = header.size;
        si.ts = header.ts;
        si.tombstone = e.tombstone;
        bloomAdd(sstOut.bloom, k);

        idx.push_back(si);
    }
    f.close();

    // Index Standardization Pass: Sort the collection lexicographically by key to enforce binary search compliance
    // before appending the newly minted SSTable state into the dedicated log level array.
    std::sort(idx.begin(), idx.end());
    sstOut.index = std::move(idx);
    GET_LEVELS_LOG()
    [level].push_back(std::move(sstOut));

    return true;
}

// =================================================================
// SYSTEM EXPORT UTILITY: Regular Key-Value Streaming Scan Cascade
// =================================================================
void NeuLSMDB::sweepDatasetEngine(bool isLogPipeline, NeuDatasetCallback callback, void *arg)
{
    if (!_systemReady || !callback)
        return;

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(2000)) != pdTRUE)
        return;

    do
    {
        // Dynamic Allocation Layer: Set limits based on target partition profiles
        uint32_t maxLimit = isLogPipeline ? NEU_LOG_MAX_INDEX : NEU_KEY_SPACE_LIMIT;
        std::vector<bool> discoveredBits(maxLimit, false);

        uint16_t maxIdLoops = isLogPipeline ? NEU_LOG_MAX_ID_LIMIT : 1;

        for (uint16_t id = 0; id < maxIdLoops; id++)
        {
            // Compute range boundaries dynamically
            uint32_t keyStart = isLogPipeline ? (NEU_LOG_KEY_OFFSET + ((uint32_t)id << NEU_LOG_INDEX_BITS)) : 0;
            uint32_t keyEnd = isLogPipeline ? (keyStart + NEU_LOG_INDEX_MASK) : (NEU_KEY_SPACE_LIMIT - 1);

            std::fill(discoveredBits.begin(), discoveredBits.end(), false);

            // -------------------------------------------------------------
            // STAGE 1: Volatile Cache Pipeline (MemTable Sweep)
            // -------------------------------------------------------------
            auto &mapMem = *GET_MEM();
            auto itMem = mapMem.lower_bound(keyStart);
            while (itMem != mapMem.end() && itMem->first <= keyEnd)
            {
                uint32_t mapKey = itMem->first;
                uint32_t bitIdx = isLogPipeline ? ((mapKey - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK) : mapKey;

                if (bitIdx < maxLimit && !discoveredBits[bitIdx])
                {
                    discoveredBits[bitIdx] = true; // Lock memory context version

                    if (!itMem->second.tombstone && itMem->second.size > 0 && itMem->second.value)
                    {
                        callback(mapKey, itMem->second.value.get(), itMem->second.size, arg);
                    }
                }
                ++itMem;
            }

            // -------------------------------------------------------------
            // STAGE 2: Non-Volatile Storage Pipeline (SSTable Layers Scan)
            // -------------------------------------------------------------
            for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
            {
                // JAWABAN KUNCI: Pilih kontainer file aslinya secara dinamis!
                auto &levelVector = isLogPipeline ? GET_LEVELS_LOG()[lvl] : GET_LEVELS()[lvl];

                for (auto itSST = levelVector.rbegin(); itSST != levelVector.rend(); ++itSST)
                {
                    auto &sst = *itSST;
                    auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                                  SSTIndex{keyStart, 0, 0, 0, false});

                    while (idxIt != sst.index.end() && idxIt->key <= keyEnd)
                    {
                        uint32_t sstKey = idxIt->key;
                        uint32_t bitIdx = isLogPipeline ? ((sstKey - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK) : sstKey;

                        if (bitIdx < maxLimit && !discoveredBits[bitIdx])
                        {
                            discoveredBits[bitIdx] = true; // Shadow obsolete historical iterations

                            if (!idxIt->tombstone && idxIt->size > 0)
                            {
                                std::vector<uint8_t> readBuf(idxIt->size);
                                size_t tmpSize = idxIt->size;

                                if (readSST(sst, *idxIt, readBuf.data(), tmpSize))
                                {
                                    callback(sstKey, readBuf.data(), tmpSize, arg);
                                }
                            }
                        }
                        ++idxIt;
                    }
                }
            }
        }
    } while (0);

    xSemaphoreGive(_mutex);
}

void NeuLSMDB::exportLogDataset(NeuDatasetCallback callback, void *arg)
{
    sweepDatasetEngine(true, callback, arg);
}

void NeuLSMDB::exportKVDataset(NeuDatasetCallback callback, void *arg)
{
    sweepDatasetEngine(false, callback, arg);
}

// =================================================================
// STREAMING LAYER: RANGE ITERATOR CONSTRUCTOR
// =================================================================
NeuLSMDB_LogIterator::NeuLSMDB_LogIterator(NeuLSMDB *db, uint16_t id, uint16_t startIdx, uint16_t endIdx)
    : _db(db), _id(id), _startIdx(startIdx), _endIdx(endIdx), _valid(false), _currentIdx(0), _currentTs(0), _currentTombstone(false)
{
    _readersVector = new std::vector<NeuLSMDB::SSTIndex>();

    // Structural Pre-Validation: Immediately abort context generation if the requested range parameters
    // or object identifiers breach active configuration limit thresholds.
    if (!_db || !_db->_systemReady || _startIdx > _endIdx || _id >= NEU_LOG_MAX_ID_LIMIT)
        return;

    // Coordinate Boundaries Calculation: Compute the absolute 32-bit register coordinate range
    // using the dynamic macro baseline to prevent memory arithmetic overflows.
    uint16_t keyStart = NEU_LOG_KEY_OFFSET + ((_id << NEU_LOG_INDEX_BITS) | _startIdx);
    uint16_t keyEnd = NEU_LOG_KEY_OFFSET + ((_id << NEU_LOG_INDEX_BITS) | _endIdx);

    auto &collected = *static_cast<std::vector<NeuLSMDB::SSTIndex> *>(_readersVector);
    _valid = true;
}

NeuLSMDB_LogIterator::~NeuLSMDB_LogIterator()
{
    if (_readersVector != nullptr)
    {
        // Allocation Sanitization Phase: Explicitly deallocate heap-assigned pointer variables
        // to prevent volatile memory resource leaks on the FreeRTOS heap structure.
        auto *vecPtr = static_cast<std::vector<NeuLSMDB::SSTIndex> *>(_readersVector);
        delete vecPtr;
        _readersVector = nullptr;
    }
}

bool NeuLSMDB_LogIterator::next()
{
    if (!_valid || !_db)
        return false;

    auto &collected = *static_cast<std::vector<NeuLSMDB::SSTIndex> *>(_readersVector);

    // Initial Hydration Phase: Execute lazy-loading mechanisms exclusively during the first iteration pass.
    // This prevents runtime memory allocation overhead until the user specifically requests dataset streaming.
    if (collected.empty() && _currentIdx == 0 && _currentTs == 0)
    {
        if (xSemaphoreTake(_db->_mutex, portMAX_DELAY) != pdTRUE)
            return false;

        uint32_t keyStart = NEU_LOG_KEY_OFFSET + ((uint32_t)_id << NEU_LOG_INDEX_BITS) + _startIdx;
        uint32_t keyEnd = keyStart + (_endIdx - _startIdx); // Derive the precise absolute end marker offset

        // Stage 1: Volatile Memory Scan (Active RAM MemTable Interception)
        // Execute a fast lower-bound tree traversal to count fresh un-flushed states first.
        auto &mapMem = *(static_cast<std::map<uint32_t, NeuLSMDB::MemEntry> *>(_db->_mem));
        auto itMem = mapMem.lower_bound(keyStart);
        while (itMem != mapMem.end() && itMem->first <= keyEnd)
        {
            NeuLSMDB::SSTIndex si;
            si.key = itMem->first;
            si.offset = 0; // Fixed coordinate flag identifying a volatile RAM memory context
            si.size = itMem->second.size;
            si.ts = itMem->second.ts;
            si.tombstone = itMem->second.tombstone;
            collected.push_back(si);
            ++itMem;
        }

        // Stage 2: Persistent Metadata Scan (Immutable RAM-Cached SSTable Index Extraction)
        // Extract multi-version log blocks chronologically across all deep LSM tree hierarchy levels.
        auto *levelsLogArr = static_cast<std::vector<NeuLSMDB::SSTFile> *>(_db->_levelsLog);

        for (int lvl = 0; lvl < NEU_MAX_LEVEL; lvl++)
        {
            auto &singleLevelVector = levelsLogArr[lvl];

            for (auto &sst : singleLevelVector)
            {
                auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                              NeuLSMDB::SSTIndex{keyStart, 0, 0, 0, false});

                while (idxIt != sst.index.end() && idxIt->key <= keyEnd)
                {
                    collected.push_back(*idxIt);
                    ++idxIt;
                }
            }
        }

        // Stage 3: Chronological Sorting & Multi-Version Concurrency Control (MVCC) Deduplication
        // Arrange items by their loop index position first, then order overlapping entries by their timestamp.
        std::sort(collected.begin(), collected.end(), [](const NeuLSMDB::SSTIndex &a, const NeuLSMDB::SSTIndex &b)
                  {
                      // Decode dadi nomor slot asli 14-bit
                      uint32_t slotA = (a.key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
                      uint32_t slotB = (b.key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;

                      if (slotA != slotB)
                          return slotA < slotB; // Sort by original slot number from smallest to largest
                      return a.ts > b.ts;       // If the round slots are the same, select the data with the most recent timestamp
                  });

        // Purge obsolete historical duplicate entries, preserving only the freshest mutation block per slot position.
        auto uniqueIt = std::unique(collected.begin(), collected.end(), [](const NeuLSMDB::SSTIndex &a, const NeuLSMDB::SSTIndex &b)
                                    {
                                        uint32_t slotA = (a.key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
                                        uint32_t slotB = (b.key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
                                        return slotA == slotB; // If they really occupy the same circular slot, they just opened one!
                                    });
        collected.erase(uniqueIt, collected.end());

        xSemaphoreGive(_db->_mutex);
        _currentIdx = 0;
    }

    // ========================================================================
    // RUNTIME CURSOR STREAMING PIPELINE CHANNEL
    // ========================================================================
    while (_currentIdx < collected.size())
    {
        auto &entry = collected[_currentIdx];

        // Decode Phase: Strip baseline offsets via NEU_LOG_INDEX_MASK to isolate the raw 14-bit ring index.
        uint32_t realSlotIndex = (entry.key - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;

        // Defensive Fence Filter: Verify whether the extracted index stays inside user query bounds.
        // Obsolete or out-of-bounds keys are skipped dynamically to ensure strict data range tracking.
        if (realSlotIndex < _startIdx || realSlotIndex > _endIdx)
        {
            _currentIdx++;
            continue;
        }

        _currentIdx++;
        _currentTs = entry.ts;
        _currentTombstone = entry.tombstone;

        // Skip records marked with a tombstone cancellation vector to enforce logical data deletions.
        if (_currentTombstone)
            continue;

        return true;
    }

    return false; // Returns false once the targeted data tracking stream hits EOF
}

uint16_t NeuLSMDB_LogIterator::getIndex() const
{
    // Defensive Guard: Return a baseline index value of 0 if the internal streaming vector
    // is entirely empty or if the active iteration pointer has not yet been advanced.
    auto &collected = *static_cast<std::vector<NeuLSMDB::SSTIndex> *>(_readersVector);
    if (collected.empty() || _currentIdx == 0)
        return 0;

    // Context Capture Phase: Intercept the active virtual key metadata node
    // currently pointed to by the iteration cursor (_currentIdx - 1).
    uint16_t activeVirtualKey = collected[_currentIdx - 1].key;

    // Binary Decoding Phase: Strip high-address baseline offsets using NEU_LOG_INDEX_MASK
    // to cleanly isolate the raw circular index coordinate from the packed 32-bit internal register.
    return (activeVirtualKey - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;
}

uint32_t NeuLSMDB_LogIterator::getTimestamp() const
{
    // Return the cached hardware timestamp assigned during the active step transition.
    return _currentTs;
}

bool NeuLSMDB_LogIterator::getValue(void *out, size_t &size)
{
    if (!_valid || _currentTombstone)
        return false;

    auto &collected = *static_cast<std::vector<NeuLSMDB::SSTIndex> *>(_readersVector);

    // Defensive Bounds Check: Validate that the current cursor tracker does not cause an out-of-bounds
    // vector vector memory violation during physical dataset traversal operations.
    if (collected.empty() || _currentIdx == 0 || (_currentIdx - 1) >= collected.size())
        return false;

    // Phase 1: Context Resolution Pass. Extract the absolute 32-bit virtual key
    // corresponding to the active iteration slot index element (_currentIdx - 1).
    uint32_t activeVirtualKey = collected[_currentIdx - 1].key;

    // Phase 2: Binary Decoding Sequence. Subtract high-address anchor offsets and apply the
    // NEU_LOG_INDEX_MASK bitmask to extract the true circular sequence slot number from the virtual track.
    uint16_t realCircularSlot = (activeVirtualKey - NEU_LOG_KEY_OFFSET) & NEU_LOG_INDEX_MASK;

    // Phase 3: Route Delegation Layer. Forward the extracted native slot coordinate to the Scenario 2 point-lookup execution path.
    // Bypassing the raw cursor counter guarantees absolute data integrity and isolates multi-version transaction states.
    return _db->getLog(_id, realCircularSlot, out, size);
}
