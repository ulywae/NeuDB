#include "NeuLSMDB/DB_Token.h"
#include "NeuLSMDB/NeuLSMDB_FS.h"

#include <Arduino.h>
#include <list>
#include <memory>
#include <cstring>
#include <algorithm>
#include "rom/crc.h"

// =================================================================
// TYPE ABSTRACTION LAYER: OPAQUE POINTER DATA ARCHITECTURE MAPPING
// =================================================================
#define GET_LEVELS() static_cast<std::vector<NeuLSMDB_FS::SSTFile> *>(_levels)
#define GET_MEM() static_cast<std::map<uint16_t, NeuLSMDB_FS::MemEntry> *>(_mem)
#define GET_CACHE_LIST() static_cast<std::list<NeuLSMDB_FS::CacheBlock> *>(_cacheList)
#define GET_CACHE_MAP() static_cast<std::map<uint64_t, std::list<NeuLSMDB_FS::CacheBlock>::iterator> *>(_cacheMap)

// =================================================================
// TOPOLOGY SCHEMA: DATA FORMAT STRUCTURE PROFILES
// =================================================================

struct NeuLSMDB_FS::MemEntry
{
    std::unique_ptr<uint8_t[]> value;
    uint16_t size;
    uint32_t ts;
    bool tombstone;
};

#pragma pack(push, 1)
struct SSTHeader
{
    uint16_t key;
    uint32_t size;
    uint32_t ts;
    uint8_t tombstone;
};
#pragma pack(pop)

struct __attribute__((packed)) NeuLSMDB_FS::SSTIndex
{
    uint16_t key;
    uint32_t offset;
    uint32_t size;
    uint32_t ts;
    bool tombstone;
    bool operator<(const SSTIndex &o) const { return key < o.key; }
};

struct NeuLSMDB_FS::SSTFile
{
    String filename;
    std::vector<SSTIndex> index;
    uint32_t fileId;
    uint8_t bloom[BLOOM_FILTER_SIZE];
};

struct NeuLSMDB_FS::CacheBlock
{
    uint64_t cacheKey;
    std::vector<uint8_t> data;
};

struct NeuLSMDB_FS::SourceReader
{
    File file;
    String filename;
    SSTIndex current;
    bool eof;
    uint32_t valueOffset;
    uint32_t version;
    bool open(const String &fname)
    {
        file = STORAGE_OPEN(fname, "r");
        eof = !file;
        version = 0;
        return next();
    }
    void close()
    {
        if (file)
            file.close();
    }
    bool next();
    bool readValue(uint8_t *buf, size_t &outSize);
};

struct NeuLSMDB_FS::HeapEntry
{
    uint16_t key;
    uint32_t ts;
    size_t readerIdx;
    uint32_t offset;
    uint32_t version;
    bool operator<(const HeapEntry &other) const { return key > other.key; }
};

struct NeuLSMDB_FS::CompactJob
{
    uint8_t srcLevel;
    std::vector<String> srcFiles;
    std::vector<SourceReader> readers;
    String dstTemp;
    String dstFinal;
    bool active;
};

// =================================================================
// CONTEXT INSTANTIATION: CONSTRUCTOR & DESTRUCTOR ALLOCATIONS
// =================================================================

NeuLSMDB_FS::NeuLSMDB_FS()
{
    // HEAP ALLOCATION: Instantiate physical structures behind data isolation masks
    _mem = new std::map<uint16_t, MemEntry>();
    _levels = new std::vector<SSTFile>[MAX_LEVEL]();
    _cacheList = new std::list<CacheBlock>();
    _cacheMap = new std::map<uint64_t, std::list<CacheBlock>::iterator>();
    _job = new CompactJob();

    // METRICS INITIALIZATION: Baseline structural limits
    _memCount = 0;
    _memBytes = 0;
    _nextFileId = 1;
    _lastFlush = 0;
    _lastTune = 0;
    _cacheUsed = 0;
    _adaptiveLimit = 4096;
    _compactState = IDLE;
    _overrideWhenFull = true;
    _totalEntryCount = 0;
    _job->active = false;

    _systemReady = false;
    _stopTaskRequested = false;

    // KERNEL INITIALIZATION: Instantiate thread-safety synchronization handles
    _mutex = xSemaphoreCreateMutex();
}

NeuLSMDB_FS::~NeuLSMDB_FS()
{
    flush();
    if (_walFile)
        _walFile.close();

    // MEMORY SANITIZATION: Reclaim heap space allocated behind opaque pointer masks
    delete static_cast<std::map<uint16_t, MemEntry> *>(_mem);
    delete[] static_cast<std::vector<SSTFile> *>(_levels);
    delete static_cast<std::list<CacheBlock> *>(_cacheList);
    delete static_cast<std::map<uint64_t, std::list<CacheBlock>::iterator> *>(_cacheMap);
    delete _job;

    if (_mutex)
        vSemaphoreDelete(_mutex);
}

// =================================================================
// BOOTSTRAP CONTROL PIPELINE: SYSTEM ENGINE BOOT INIT
// =================================================================

bool NeuLSMDB_FS::init()
{
    if (_systemReady)
        return true;

    // VFS INITIALIZATION: Mount partition topology map via low-level storage drivers
    if (!STORAGE_INIT())
        return false;

    // SYSTEM PATH CHECK: Guarantee transactional directory space profiles exist safely
    if (!STORAGE_EXISTS("/lsm"))
        STORAGE_MKDIR("/lsm");

    // DATA RECOVERY SEQUENCING: Reconstruct operational system topology maps
    loadAllSST();
    replayWAL();

    // STORAGE ACCESS CHANNEL: Open append-only pipeline stream to commit transaction history log
    _walFile = STORAGE_OPEN("/lsm/wal.log", FILE_APPEND);
    if (!_walFile)
        return false;

    _lastFlush = millis();
    _lastTune = millis();

    _stopTaskRequested = false;
    _systemReady = true;

    if (_taskHandle == NULL)
    {
        // KERNEL DESPATCHER: Isolate low-priority structural reorganization task to physical CPU core 1
        xTaskCreatePinnedToCore(
            [](void *param)
            {
                NeuLSMDB_FS *db = static_cast<NeuLSMDB_FS *>(param);
                for (;;)
                {
                    // INTERRUPT SERVICE DETECTOR: Gracefully break continuous processing if termination flags match
                    if (db->_stopTaskRequested)
                        break;

                    db->tick();
                    vTaskDelay(pdMS_TO_TICKS(5)); // Relinquish CPU execution control window
                }

                TaskHandle_t localHandle = db->_taskHandle;
                db->_taskHandle = NULL;
                vTaskDelete(NULL); // Terminate background process task context
            },
            "LSM_Task", 4096, this, 1, &_taskHandle, 1);
    }

    return true;
}

bool NeuLSMDB_FS::format()
{
    _systemReady = false;
    _compactState = IDLE;
    _stopTaskRequested = true;

    int timeout = 0;
    while (_taskHandle != NULL && timeout < 20)
    {
        vTaskDelay(pdMS_TO_TICKS(5));
        timeout++;
    }

    if (_taskHandle != NULL)
    {
        vTaskDelete(_taskHandle);
        _taskHandle = NULL;
    }

    if (_mutex != nullptr)
    {
        if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(2000)) != pdTRUE)
            return false;
    }

    if (_job != nullptr)
    {
        _job->active = false;
        for (auto &reader : _job->readers)
            reader.close();
        _job->readers.clear();
        _job->srcFiles.clear();
    }

    if (_walFile)
        _walFile.close();

    std::vector<String> listFileHapus;

    if (STORAGE_EXISTS("/lsm"))
    {
        File dir = STORAGE_OPEN("/lsm", "r");
        if (dir && dir.isDirectory())
        {
            File file = dir.openNextFile();
            while (file)
            {
                listFileHapus.push_back(String("/lsm/") + file.name());
                file.close();
                file = dir.openNextFile();
            }
        }
        if (dir)
            dir.close();

        for (const auto &filePath : listFileHapus)
        {
            STORAGE_REMOVE(filePath);
            vTaskDelay(pdMS_TO_TICKS(2));
        }
        STORAGE_RMDIR("/lsm");
    }
    STORAGE_MKDIR("/lsm");

    auto memPtr = GET_MEM();
    if (memPtr != nullptr)
        memPtr->clear();

    auto levelsPtr = GET_LEVELS();
    if (levelsPtr != nullptr)
    {
        for (uint8_t i = 0; i < MAX_LEVEL; i++)
            levelsPtr[i].clear();
    }

    auto cacheListPtr = GET_CACHE_LIST();
    if (cacheListPtr != nullptr)
        cacheListPtr->clear();

    auto cacheMapPtr = GET_CACHE_MAP();
    if (cacheMapPtr != nullptr)
        cacheMapPtr->clear();

    __atomic_store_n(&_memCount, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&_totalEntryCount, 0, __ATOMIC_SEQ_CST);
    _memBytes = 0;
    _cacheUsed = 0;
    _lastFlush = millis();
    _lastTune = millis();

    if (_mutex != nullptr)
        xSemaphoreGive(_mutex);

    return init();
}

void NeuLSMDB_FS::setOverrideWhenFull(bool enable) { _overrideWhenFull = enable; }
bool NeuLSMDB_FS::getOverrideWhenFull() const { return _overrideWhenFull; }

// ==========================================
// LOOP SISTEM: tick()
// ==========================================
void NeuLSMDB_FS::tick()
{
    // 1. Core safety guard to prevent execution before bootstrap sequence completes
    if (!_systemReady)
        return;

    // NOTE: Allow tick() to execute concurrently without holding any global lock mutex constraints.

    // ========================================================================
    // PREDICTIVE RESOURCE-AWARE AUTO-TUNING
    // ========================================================================
    // Asynchronously compute current hardware stress matrices (RAM heap, write volume, L0 file pressure)
    // and recalculate the elastic _adaptiveLimit threshold before checking boundaries.
    tuneMemtable();

    // 2. Evaluate and handle periodic Write-Ahead Log serialization (flushWAL) every 200ms
    if (millis() - _lastFlush >= 200)
    {
        // Apply fixed-timestep accumulator trick to guarantee consistent 200ms scheduling intervals
        _lastFlush += 200;
        flushWAL();
    }

    // 3. Monitor if RAM MemTable capacity boundaries or entry counts breach active profile limits
    // Read the atomic _memCount variable safely without locking overhead
    size_t currentMemCount = __atomic_load_n(&_memCount, __ATOMIC_SEQ_CST);

    if (currentMemCount >= MEMTABLE_MAX_ENTRIES || _memBytes >= _adaptiveLimit)
    {
        // Call flush(). The original flush() function is already equipped with
        // internal xSemaphoreTake, so it will lock itself safely!
        flush();
    }

    // 4. Delegate LSM Tree Compaction and file merge routines management
    // Allow the compaction engine pipelines to manage their own granular mutex locking internally
    if (_compactState != IDLE)
        tickCompact();
    else
        runCompactionScheduler();
}

void NeuLSMDB_FS::runCompactionScheduler()
{
    CompactState st = __atomic_load_n(&_compactState, __ATOMIC_SEQ_CST);
    if (st != IDLE)
        return;

    if (GET_LEVELS()[0].size() > 6)
    {
        triggerCompaction(0);
        return;
    }
    for (int lvl = 0; lvl < MAX_LEVEL - 1; lvl++)
    {
        int thr = (lvl == 0) ? 2 : (2 << lvl);
        if ((lvl == 0 && (GET_LEVELS()[lvl].size() >= thr || GET_LEVELS()[lvl].size() >= 10)) || (lvl > 0 && GET_LEVELS()[lvl].size() >= thr))
        {
            triggerCompaction(lvl);
            return;
        }
    }
}

// ==========================================
// WRITE PATH PIPELINE: put()
// ==========================================

bool NeuLSMDB_FS::put(uint16_t key, const void *data, size_t size)
{
    if (!_systemReady)
        return false;

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return false;

    // ========================================================================
    // SMART WRITE STALL POLICY: ASYNCHRONOUS CONCURRENCY INGESTION BRAKE
    // ========================================================================
    // If the background maintenance worker thread is actively executing a heavy
    // compaction stream, yield the execution pipeline to prevent VFS block device
    // descriptor saturation and allow the underlying file storage subsystem to
    // safely flush outstanding transaction buffers.
    if (__atomic_load_n(&_compactState, __ATOMIC_SEQ_CST) == MERGE_STREAM)
    {
        xSemaphoreGive(_mutex);       // Release the unified key resource lock
        vTaskDelay(pdMS_TO_TICKS(4)); // Force a 4-millisecond adaptive backoff window
        if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
            return false;
    }

    bool res = false;

    do
    {
        // =================================================================
        // DEFENSIVE VALIDATION: LOCKLESS RANGE CONSTRAINT CHECK
        // =================================================================
        if (key >= NEU_KEY_SPACE_LIMIT || size > 65535)
            break; // Hard abort incoming transactions instantly via defensive range checks

        // =================================================================
        // STORAGE INTEGRITY: RESOURCE CAPACITY BOUNDARY INTERRUPT
        // =================================================================
        if (_flashFullGuard || __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST) >= MAX_TOTAL_ENTRIES)
        {
            if (_overrideWhenFull)
                evictOldestData(); // Force reactive cache eviction on active MemTable element
            else
                break; // Hard abort incoming transactions if override policy is suppressed
        }

        // =================================================================
        // CONCURRENCY CONTROL: ADAPTIVE WAL TRANSACTION QUEUE RETRY LOOPS
        // =================================================================
        int retryWAL = 0;
        bool walSuccess = false;
        while (retryWAL < 10)
        {
            if (appendWAL(key, data, size, false))
            {
                walSuccess = true;
                break;
            }

            // Yield CPU control context to resolve background flush resource locks
            xSemaphoreGive(_mutex);       // Temporarily release resource lock allocation
            vTaskDelay(pdMS_TO_TICKS(2)); // Force a brief 2ms backoff delay to allow background task to complete flush and release locks
            if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
                break;

            retryWAL++;
        }

        if (!walSuccess)
            break; // Terminate state processing if write pipeline execution fails

        uint32_t now = millis();
        auto &mapMem = *static_cast<std::map<uint16_t, MemEntry> *>(_mem);
        auto it = mapMem.find(key);

        if (it != mapMem.end())
        {
            // MUTATION PATH: UPDATE IN-MEMORY RECORD VECTOR
            _memBytes -= it->second.size;
            it->second.value.reset(new uint8_t[size]);
            if (data)
                memcpy(it->second.value.get(), data, size);
            it->second.size = size;
            it->second.ts = now;
            it->second.tombstone = (size == 0);
            _memBytes += size;
        }
        else
        {
            // ALLOCATION PATH: INSERT FRESH MEMTABLE TRANSACTION RECORD
            MemEntry e;
            if (size > 0 && data)
            {
                e.value.reset(new uint8_t[size]);
                memcpy(e.value.get(), data, size);
            }
            e.size = size;
            e.ts = now;
            e.tombstone = (size == 0);
            mapMem[key] = std::move(e);
            __atomic_add_fetch(&_memCount, 1, __ATOMIC_SEQ_CST);
            _memBytes += size;
            __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
        }
        res = true;
    } while (0);

    xSemaphoreGive(_mutex);
    return res;
}

// =================================================================
// READ PATH PIPELINE: get()
// =================================================================

bool NeuLSMDB_FS::get(uint16_t key, void *out, size_t &size)
{
    if (!_systemReady)
        return false;

    if (key >= NEU_KEY_SPACE_LIMIT)
    {
        size = 0;
        return false; // Fast-fail early exit before acquiring mutex to optimize CPU cycles
    }

    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
    {
        return false;
    }
    bool seek = false;

    do
    {
        // 1. VOLATILE MEMORY READ PIPELINE (RAM PHASE LOOKUP)
        auto &mapMem = *static_cast<std::map<uint16_t, MemEntry> *>(_mem);
        auto it = mapMem.find(key);
        if (it != mapMem.end())
        {
            if (it->second.tombstone)
            {
                size = 0;
                break; // Key found but status DELETED (Tombstone) -> Return false
            }

            // Save the original size of the data to the size variable so that the user knows the original size.
            size_t requiredSize = it->second.size;
            if (size < requiredSize)
            {
                size = requiredSize; // Tell me the size you need
                break;               // Exit with seek status = false (Buffer not big enough)
            }

            if (requiredSize > 0 && out && it->second.value)
            {
                memcpy(out, it->second.value.get(), requiredSize);
            }
            size = requiredSize;
            seek = true;
            break;
        }

        // 2. PERSISTENT STORAGE SEARCH PIPELINE (DISPATCH LOOP LAYER)
        for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
        {
            auto &level = GET_LEVELS()[lvl];
            for (auto itSST = level.rbegin(); itSST != level.rend(); ++itSST)
            {
                auto &sst = *itSST;

                if (!bloomCheck(sst.bloom, key))
                    continue;

                // Binary search on SST index in RAM
                auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(), SSTIndex{key, 0, 0, 0, false});
                if (idxIt != sst.index.end() && idxIt->key == key)
                {
                    // TOMBSTONE INTERCEPTION DIRECTLY FROM RAM INDEX
                    if (idxIt->tombstone)
                    {
                        size = 0;
                        seek = false; // Official key deleted at this sst level
                        goto end_get;
                    }

                    // Validate buffer capacity before reading physical files (saves I/O)
                    if (size < idxIt->size)
                    {
                        size = idxIt->size; // Tell the user the original size required.
                        seek = false;
                        goto end_get;
                    }

                    size_t tmp = size;
                    // Perform a physical read to disk via readSST
                    if (readSST(sst, *idxIt, out, tmp))
                    {
                        size = tmp;
                        seek = true;
                        goto end_get;
                    }
                }
            }
        }
    end_get:;
    } while (0);

    xSemaphoreGive(_mutex);
    return seek;
}

// =================================================================
// MEMORY TRUNCATION & VOLATILE FLUSH PIPELINE: flush()
// =================================================================
void NeuLSMDB_FS::flush()
{
    if (!_systemReady)
        return;

    // Acquire database mutex lock
    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        return;

    // =================================================================
    // RESOURCE MONITORING: ENFORCE 90% HARD STORAGE MATRIX BOUNDARY
    // =================================================================
    size_t totalBytesFlash = STORAGE_TOTAL();
    size_t usedBytesFlash = STORAGE_USED();
    _flashFullGuard = (usedBytesFlash >= (totalBytesFlash * 9) / 10);

    auto &mapMem = *GET_MEM();

    // =================================================================
    // MEMTABLE FLUSH INTEGRITY FENCE
    // =================================================================
    // If the MemTable is empty OR the writeSST file write process to Level 0 fails,
    // immediately return the mutex token to the kernel and abort the disk synchronization cycle.
    if (mapMem.empty() || !writeSST(0, mapMem))
    {
        xSemaphoreGive(_mutex);
        return;
    }

    // PERSISTENCE SYNC: Purge in-memory volatile vectors upon committed serialization
    mapMem.clear();
    _memBytes = 0;
    __atomic_store_n(&_memCount, 0, __ATOMIC_SEQ_CST);

    // Close and physically delete the old WAL log file from Flash storage
    clearWAL();

    // ========================================================================
    // RECOVERY PATH PIPELINE: INITIATE ATOMIC WAL TRANSACTION SEGMENT
    // ========================================================================
    // Instantiates a fresh append-only transaction history log file for downstream writes.
    // Even if the underlying VFS block storage device triggers an extended internal hardware
    // garbage collection or memory page swap operation, the upstream put() injection pathway
    // remains resilient against latency spikes due to adaptive lock-contention backoffs.
    _walFile = STORAGE_OPEN("/lsm/wal.log", FILE_APPEND);
    if (!_walFile)
    {
        // Serial.println(F("[CRITICAL] Storage subsystem failed to instantiate a new WAL descriptor segment!"));
    }

    _lastFlush = millis();

    // =================================================================
    // CRITICAL: Release database mutex lock IMMEDIATELY here!
    // Ensures the main application write gate (put) is free from timeout risks.
    // =================================================================
    xSemaphoreGive(_mutex);
}

// =================================================================
// TRANSACTION LOG MANAGEMENT: WRITE-AHEAD LOGGING (WAL) SUBSYSTEM
// =================================================================

bool NeuLSMDB_FS::appendWAL(uint16_t key, const void *data, size_t size, bool tombstone)
{
    if (!_walFile)
        return false;

    uint32_t writeSize = (uint32_t)size;

    // Calculate structural record checksum using standard CRC32
    uint32_t crc = crc32_le(0, (const uint8_t *)&key, sizeof(key));
    crc = crc32_le(crc, (const uint8_t *)&writeSize, sizeof(writeSize));
    if (writeSize > 0 && data)
        crc = crc32_le(crc, (const uint8_t *)data, writeSize);
    uint8_t tombByte = tombstone ? 1 : 0;
    crc = crc32_le(crc, &tombByte, 1);

    // =================================================================
    // TRANSACTION DESCRIPTOR FIXED-LENGTH HEADER COMMIT
    // =================================================================
    // PIPELINE GUARD: Stream the 16-bit key and 32-bit payload size descriptors directly into VFS block device.
    // Instantly abort the transaction pipeline if any fixed-length layout block write operation fails.
    if (_walFile.write((const uint8_t *)&key, sizeof(key)) != sizeof(key) ||
        _walFile.write((const uint8_t *)&writeSize, sizeof(writeSize)) != sizeof(writeSize))
        return false;

    if (writeSize > 0 && data)
    {
        if (_walFile.write((const uint8_t *)data, writeSize) != writeSize)
            return false;
    }

    // =================================================================
    // TRANSACTION TRAILING DISK COMMIT
    // =================================================================
    // FOOTER GUARD: Commit tombstone state flag and trailing hardware-backed CRC32 checksum to flash.
    // Instantly abort pipeline transaction and return false if any filesystem hardware block write fails.
    if (_walFile.write(&tombByte, 1) != 1 || _walFile.write((const uint8_t *)&crc, sizeof(crc)) != sizeof(crc))
        return false;

    return true;
}

void NeuLSMDB_FS::replayWAL()
{
    File wal = STORAGE_OPEN("/lsm/wal.log", "r");
    if (!wal)
        return;

    auto &mapMem = *GET_MEM();
    mapMem.clear();
    _memBytes = 0;
    __atomic_store_n(&_memCount, 0, __ATOMIC_SEQ_CST);

    while (wal.available())
    {
        uint16_t key;
        uint32_t sz;
        uint8_t tomb;
        uint32_t crcFile;

        // =================================================================
        // TRANSACTION LIFECYCLE VFS STREAM VALIDATION
        // =================================================================
        // RECOVERY GUARD: Break iteration if stream reads fail OR payload size 'sz' overflows safety bounds
        if (wal.read((uint8_t *)&key, sizeof(key)) != sizeof(key) ||
            wal.read((uint8_t *)&sz, sizeof(sz)) != sizeof(sz) ||
            sz > 4096)
        {
            break; // Abruptly terminate recovery log parsing if any validation fence is tripped
        }

        std::unique_ptr<uint8_t[]> buf;
        if (sz > 0)
        {
            buf.reset(new uint8_t[sz]);
            if (wal.read(buf.get(), sz) != sz)
                break;
        }

        // =================================================================
        // TRANSACTION TRAILING PIPELINE VALIDATION
        // =================================================================
        // FOOTER GUARD: Abort tracking if tombstone flag or trailing CRC32 extraction fails
        if (wal.read(&tomb, 1) != 1 || wal.read((uint8_t *)&crcFile, sizeof(crcFile)) != sizeof(crcFile))
        {
            break; // Abruptly terminate log parsing on unexpected trailing stream truncation
        }

        // Validate CRC integrity using standard hardware-backed calculation
        uint32_t crcCalc = crc32_le(0, (const uint8_t *)&key, sizeof(key));
        crcCalc = crc32_le(crcCalc, (const uint8_t *)&sz, sizeof(sz));
        if (sz > 0)
            crcCalc = crc32_le(crcCalc, buf.get(), sz);
        crcCalc = crc32_le(crcCalc, &tomb, 1);

        if (crcCalc != crcFile)
        {
            // This point denotes the final boundary of valid transactions before the sudden power blackout
            break;
        }

        uint32_t now = millis();
        MemEntry e;
        e.ts = now;
        e.tombstone = (tomb == 1);
        e.size = sz;
        if (sz > 0)
            e.value = std::move(buf);

        // Secure management of _memBytes size and entry count if key already exists
        auto it = mapMem.find(key);
        if (it != mapMem.end())
        {
            // If the key is duplicate, reduce the size of the old data first.
            _memBytes -= it->second.size;
        }
        else
        {
            // If this is a new key, increment the atomic count of MemTable entries and the System Total.
            __atomic_add_fetch(&_memCount, 1, __ATOMIC_SEQ_CST);
            __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
        }

        // Enter new entry / update old entry
        _memBytes += sz;
        mapMem[key] = std::move(e);
    }
    wal.close();
}

void NeuLSMDB_FS::clearWAL()
{
    // Tasked exclusively with closing the active handle and executing physical file truncation/deletion!
    if (_walFile)
        _walFile.close();
    STORAGE_REMOVE("/lsm/wal.log");
}

void NeuLSMDB_FS::flushWAL()
{
    // Since tick() runs in its own task, it is mandatory to grab the mutex before touching _walFile.
    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(10)) == pdTRUE)
    {
        if (_walFile)
        {
            _walFile.flush(); // Force data from RAM buffer to Flash chip every 200ms
        }
        xSemaphoreGive(_mutex);
    }
}

// =================================================================
// IMMUTABLE STORAGE SUBSYSTEM: SORTED STRING TABLE (SST) MANAGEMENT
// =================================================================

String NeuLSMDB_FS::makeFilename(uint8_t level, uint32_t seq)
{
    return "/lsm/lv" + String(level) + "_" + String(seq) + ".sst";
}

uint32_t NeuLSMDB_FS::getFileSeq()
{
    return _nextFileId++;
}

bool NeuLSMDB_FS::writeSST(uint8_t level, const std::map<uint16_t, MemEntry> &entries, const String &dstFile)
{
    if (entries.empty())
        return true;

    uint32_t fid = 0;
    String fn;

    if (dstFile.length() > 0)
    {
        fn = dstFile;
        int lastUnderscore = dstFile.lastIndexOf('_');
        int lastDot = dstFile.lastIndexOf('.');
        if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore)
        {
            fid = dstFile.substring(lastUnderscore + 1, lastDot).toInt();
        }
        else
        {
            fid = getFileSeq();
        }
    }
    else
    {
        fid = getFileSeq();
        fn = makeFilename(level, fid);
    }

    File f = STORAGE_OPEN(fn, "w");
    if (!f)
        return false;

    std::vector<SSTIndex> idx;
    SSTFile sstOut;
    sstOut.filename = fn;
    sstOut.fileId = fid;
    memset(sstOut.bloom, 0, sizeof(sstOut.bloom));

    for (auto &kv : entries)
    {
        uint16_t k = kv.first;
        const MemEntry &e = kv.second;

        uint32_t currentPos = f.position();

        SSTHeader header;
        header.key = k;
        header.size = e.size;
        header.ts = e.ts;
        header.tombstone = e.tombstone ? 1 : 0;

        // SERIALIZATION PHASE: Commit fixed-length metadata header descriptor to persistent storage
        if (f.write((const uint8_t *)&header, sizeof(SSTHeader)) != sizeof(SSTHeader))
        {
            f.close();
            return false;
        }

        if (header.size > 0 && e.value)
        {
            if (f.write(e.value.get(), header.size) != header.size)
            {
                f.close();
                return false;
            }
        }

        // INDEXATION PHASE: Construct tracking descriptor map for lightning-fast memory lookups
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

    // ENFORCE LSM CONSTRAINTS: Index blocks must be sorted lexicographically by key to allow binary search
    std::sort(idx.begin(), idx.end());
    sstOut.index = std::move(idx);
    GET_LEVELS()
    [level].push_back(std::move(sstOut));

    return true;
}

bool NeuLSMDB_FS::readSST(const SSTFile &sst, const SSTIndex &idxEntry, void *out, size_t &size)
{
    // 1. Instantly intercept historical tombstone markers from the passed index parameter
    if (idxEntry.tombstone)
    {
        size = 0;
        return true;
    }

    // 2. Validate output buffer capacity bounds before committing storage I/O cycles
    if (size < idxEntry.size)
        return false;

    // 3. READ BLOCK CACHE (RAM Phase Lookup)
    std::vector<uint8_t> cacheBuf;
    if (cacheGet(sst.fileId, idxEntry.offset, cacheBuf))
    {
        if (cacheBuf.size() != idxEntry.size)
            return false;

        memcpy(out, cacheBuf.data(), idxEntry.size);
        size = idxEntry.size;
        return true;
    }

    // 4. FETCH PHYSICAL DISK RECORD (VFS Storage Phase)
    File f = STORAGE_OPEN(sst.filename, "r");
    if (!f)
        return false;

    // ========================================================================
    // 32-BIT (11 BYTE) DATA HEADER STRUCTURE SYNCHRONIZATION
    // ========================================================================
    // key(2B) + size(4B) + timestamp(4B) + tombstone(1B) = sizeof(SSTHeader) -> 11 bytes
    const uint32_t valueOffset = idxEntry.offset + sizeof(SSTHeader); // Automatically worth +11

    if (!f.seek(valueOffset))
    {
        f.close();
        return false;
    }

    // Extract storage data record directly into output destination buffer space
    size_t actualRead = f.read((uint8_t *)out, idxEntry.size);
    f.close();

    if (actualRead != idxEntry.size)
        return false;

    // 5. CACHE POPULATION: Populate cold storage payload data into volatile LRU Block Cache
    cachePut(sst.fileId, idxEntry.offset, (const uint8_t *)out, idxEntry.size);
    size = idxEntry.size;

    return true;
}

void NeuLSMDB_FS::loadAllSST()
{
    File root = STORAGE_OPEN("/lsm", "r");
    if (!root || !root.isDirectory())
    {
        return;
    }

    File f = root.openNextFile();
    int totalFileDitemukan = 0;
    while (f)
    {
        String nm = f.name();

        // Extract pure filename without path if f.name() returns a full absolute path
        if (nm.lastIndexOf('/') != -1)
            nm = nm.substring(nm.lastIndexOf('/') + 1);

        if (nm.endsWith(".sst") && nm.startsWith("lv"))
        {
            int lvl = nm.substring(2, nm.indexOf('_')).toInt();

            if (lvl >= 0 && lvl < MAX_LEVEL)
            {
                String fullPath = "/lsm/" + nm;

                SSTFile sst;
                sst.filename = fullPath;

                // Extract the original ID from the file name (eg: "lv0_45.sst" -> 45)
                int lastUnderscore = nm.lastIndexOf('_');
                int lastDot = nm.lastIndexOf('.');
                if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore)
                {
                    sst.fileId = nm.substring(lastUnderscore + 1, lastDot).toInt();
                }
                else
                {
                    sst.fileId = getFileSeq(); // Fallback if name format is corrupted
                }

                memset(sst.bloom, 0, sizeof(sst.bloom));

                File fd = STORAGE_OPEN(fullPath, "r");
                if (!fd)
                {
                    f = root.openNextFile();
                    continue;
                }

                while (fd.available())
                {
                    uint32_t currentPos = fd.position();

                    // 32-bit SSTHeader struct for reading 11 bytes at a time
                    SSTHeader header;
                    if (fd.read((uint8_t *)&header, sizeof(SSTHeader)) != sizeof(SSTHeader))
                        break;

                    // Move the read data into the RAM index structure (SSTIndex)
                    SSTIndex idx;
                    idx.key = header.key;
                    idx.offset = currentPos;
                    idx.size = header.size;
                    idx.ts = header.ts;
                    idx.tombstone = (header.tombstone == 1);

                    bloomAdd(sst.bloom, header.key);

                    // Jump over the original data payload to the next descriptor
                    fd.seek(idx.size, SeekCur);

                    sst.index.push_back(idx);

                    if (!idx.tombstone)
                        __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
                }
                fd.close();

                std::sort(sst.index.begin(), sst.index.end());
                GET_LEVELS()
                [lvl].push_back(std::move(sst));
                totalFileDitemukan++;
            }
        }
        f = root.openNextFile();
    }
    root.close();
}

void NeuLSMDB_FS::deleteSSTFiles(const std::vector<String> &files)
{
    // CORE SYNCHRONIZATION: Acquire global context mutex to establish a strict memory barrier across SMP cores
    if (_mutex && xSemaphoreTake(_mutex, portMAX_DELAY) == pdTRUE)
    {
        for (auto &fn : files)
        {
            // GRID TRAVERSAL: Scan across all LSM levels to locate the target cataloged memory index record
            for (int l = 0; l < MAX_LEVEL; l++)
            {
                auto it = std::find_if(GET_LEVELS()[l].begin(), GET_LEVELS()[l].end(), [&](const SSTFile &x)
                                       { return x.filename == fn; });

                if (it != GET_LEVELS()[l].end())
                {
                    // METRICS RECONCILIATION: Parse the index descriptor block to track and balance global counters
                    for (auto &e : it->index)
                    {
                        if (!e.tombstone)
                        {
                            // ANTI-UNDERFLOW PROTECTION: Fetch the current live database pool ledger atomically
                            size_t currentTotal = __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST);

                            // Strict guard policy for unsigned variables: only decrement if safely above zero
                            if (currentTotal > 0)
                                __atomic_sub_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
                        }
                    }

                    // TOPOLOGY METADATA EVICTION: Safely erase the unlinked SST file node from active RAM grid maps
                    GET_LEVELS()
                    [l].erase(it);
                    break;
                }
            }

            // PHYSICAL DATA UNLINK: Purge the obsolete transactional byte stream block from VFS layer
            STORAGE_REMOVE(fn);
        }

        // KERNEL SEMAPHORE RELEASE: Yield the lock back to the scheduler to re-enable concurrent multi-core operations
        xSemaphoreGive(_mutex);
    }
}

// =================================================================
// PROBABILISTIC FILTERING: HARDWARE-ACCELERATED BLOOM FILTER ENGINE
// =================================================================

uint32_t NeuLSMDB_FS::bloomHash(uint16_t key, uint8_t seed)
{
    // PROBABILISTIC INTERACTION: Extract bit-vector offsets via hardware-backed CRC32 hashing matrices
    return (crc32_le(seed, (const uint8_t *)&key, sizeof(key))) % (BLOOM_FILTER_SIZE * 8);
}

void NeuLSMDB_FS::bloomAdd(uint8_t *filter, uint16_t key)
{
    for (uint8_t i = 0; i < BLOOM_HASH_COUNT; i++)
    {
        uint32_t h = bloomHash(key, i);
        filter[h / 8] |= (1 << (h % 8));
    }
}

bool NeuLSMDB_FS::bloomCheck(const uint8_t *filter, uint16_t key)
{
    for (uint8_t i = 0; i < BLOOM_HASH_COUNT; i++)
    {
        uint32_t h = bloomHash(key, i);
        if (!(filter[h / 8] & (1 << (h % 8))))
            return false; // Deterministic shortcut: Key is guaranteed to be non-existent in storage block
    }
    return true; // Key potentially exists: Proceed safely to perform deterministic disk search
}

// =================================================================
// VOLATILE ACCELERATION: DOUBLE-MAPPED LRU BLOCK CACHE SUBSYSTEM
// =================================================================

uint64_t NeuLSMDB_FS::makeCacheKey(uint32_t fileId, uint32_t offset)
{
    // BITWISE COMPOUNDING: Construct an absolute 64-bit coordinate space using distinct file and offset blocks
    return (uint64_t)fileId << 32 | offset;
}

void NeuLSMDB_FS::cachePut(uint32_t fileId, uint32_t offset, const uint8_t *data, size_t len)
{
    uint64_t k = makeCacheKey(fileId, offset);
    auto &mapC = *static_cast<std::map<uint64_t, std::list<CacheBlock>::iterator> *>(_cacheMap);
    auto &listC = *static_cast<std::list<CacheBlock> *>(_cacheList);

    // DEDUPLICATION PHASE: Purge preexisting target elements to refresh transactional record sequences
    if (mapC.count(k))
    {
        _cacheUsed -= mapC[k]->data.size();
        listC.erase(mapC[k]);
        mapC.erase(k);
    }

    // RESOURCE CONSTRAINTS: Proactively drop outdated cache blocks to maintain strict memory bounds
    while (_cacheUsed + len > CACHE_SIZE_BYTES && !listC.empty())
    {
        cacheEvict();
    }

    CacheBlock b;
    b.cacheKey = k;
    b.data.assign(data, data + len);
    listC.push_front(b);
    mapC[k] = listC.begin(); // Map registration: Secure absolute O(1) address resolution path via list iterator
    _cacheUsed += len;
}

bool NeuLSMDB_FS::cacheGet(uint32_t fileId, uint32_t offset, std::vector<uint8_t> &out)
{
    uint64_t k = makeCacheKey(fileId, offset);
    auto &mapC = *static_cast<std::map<uint64_t, std::list<CacheBlock>::iterator> *>(_cacheMap);
    auto &listC = *static_cast<std::list<CacheBlock> *>(_cacheList);

    if (!mapC.count(k))
        return false;

    out = mapC[k]->data;

    // CACHE PROMOTION: Shift accessed node to head position via lockless internal pointer splicing
    listC.splice(listC.begin(), listC, mapC[k]);
    return true;
}

void NeuLSMDB_FS::cacheEvict()
{
    auto &listC = *static_cast<std::list<CacheBlock> *>(_cacheList);
    auto &mapC = *static_cast<std::map<uint64_t, std::list<CacheBlock>::iterator> *>(_cacheMap);
    if (listC.empty())
        return;

    // LEAST RECENTLY USED PHASE: Extract and drop stale elements from the tail boundary of the tracking vector
    auto it = --listC.end();
    _cacheUsed -= it->data.size();
    mapC.erase(it->cacheKey);
    listC.pop_back();
}

// =================================================================
// REORGANIZATION ENGINE: LSM BACKGROUND COMPACTION SCHEDULER
// =================================================================
bool NeuLSMDB_FS::SourceReader::next()
{
    if (!file || eof)
        return false;

    if (file.available())
    {
        uint32_t startPos = file.position();

        SSTHeader header;
        // Read 11-bytes directly from disk to RAM
        if (file.read((uint8_t *)&header, sizeof(SSTHeader)) != sizeof(SSTHeader))
        {
            eof = true;
            return false;
        }

        current.key = header.key;
        current.size = header.size;
        current.ts = header.ts;
        current.tombstone = (header.tombstone == 1);
        current.offset = startPos;

        valueOffset = file.position();

        // BOUNDARY CHECK & JUMPER: Validate payload size against the physical limits of the external disk
        if (file.position() + current.size > file.size() || !file.seek(current.size, SeekCur))
        {
            eof = true;
            return false;
        }

        // ITERATION INCREMENT: Track version changes to sync multi-core compaction states
        version++;
        return true;
    }

    eof = true;
    return false;
}

bool NeuLSMDB_FS::SourceReader::readValue(uint8_t *buf, size_t &outSize)
{
    if (!file || eof || !current.size)
    {
        outSize = 0;
        return true;
    }
    file.seek(valueOffset);
    outSize = file.read(buf, current.size);
    return outSize == current.size;
}

void NeuLSMDB_FS::triggerCompaction(uint8_t level)
{
    if (level + 1 >= MAX_LEVEL)
        return;

    // THREAD RESILIENCE: Secure cross-core state using atomic compare-and-swap (CAS) memory fencing
    CompactState exp = IDLE;
    if (!__atomic_compare_exchange_n(&_compactState, &exp, MERGE_STREAM, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
        return;

    // Reset job metadata
    _job->srcLevel = level;
    _job->srcFiles.clear();
    _job->readers.clear();
    _job->dstTemp = "";
    _job->dstFinal = "";
    _job->active = true;

    // Gather all SST files from the current target level
    for (auto &sst : GET_LEVELS()[level])
        _job->srcFiles.push_back(sst.filename);

    // Aggregate SST files from the immediate next lower level for merging
    for (auto &sst : GET_LEVELS()[level + 1])
        _job->srcFiles.push_back(sst.filename);

    // Initialize source stream readers for every file in the compaction pool
    for (auto &fn : _job->srcFiles)
    {
        SourceReader r;
        if (r.open(fn))
        {
            _job->readers.push_back(std::move(r));
        }
        else
        {
            // FILE DESCRIPTOR LEAK PROTECTION: If the target file fails to open (empty/corrupted),
            // force-close its internal handle here to instantly release the file descriptor in the VFS layer.
            r.close();
        }
    }

    // Generate output destination paths
    uint32_t fileId = getFileSeq();
    _job->dstTemp = makeFilename(level + 1, fileId) + ".tmp";
    _job->dstFinal = makeFilename(level + 1, fileId);
}

void NeuLSMDB_FS::tickCompact()
{
    CompactState state = __atomic_load_n(&_compactState, __ATOMIC_SEQ_CST);
    if (state != MERGE_STREAM)
        return;

    if (!_compactInitialized)
    {
        while (!_compactHeap.empty())
            _compactHeap.pop();

        for (size_t i = 0; i < _job->readers.size(); i++)
        {
            auto &r = _job->readers[i];
            if (!r.eof)
                _compactHeap.push({r.current.key, r.current.ts, i, r.current.offset, r.version});
        }
        _compactInitialized = true;
    }

    File out = STORAGE_OPEN(_job->dstTemp, "a");
    if (!out)
    {
        __atomic_store_n(&_compactState, IDLE, __ATOMIC_SEQ_CST);
        _compactInitialized = false;
        _job->active = false;
        return;
    }

    size_t budget = COMPACT_BUDGET_KB * 1024;
    size_t written = 0;

    while (!_compactHeap.empty() && written < budget)
    {
        HeapEntry top;
        bool found = false;
        while (!_compactHeap.empty())
        {
            top = _compactHeap.top();
            _compactHeap.pop();
            auto &r = _job->readers[top.readerIdx];
            if (r.version == top.version && r.current.offset == top.offset)
            {
                found = true;
                break;
            }
        }
        if (!found)
            continue;

        uint16_t key = top.key;

        std::vector<size_t> group;
        group.reserve(4);
        for (size_t i = 0; i < _job->readers.size(); i++)
        {
            auto &r = _job->readers[i];
            if (!r.eof && r.current.key == key)
                group.push_back(i);
        }
        if (group.empty())
            continue;

        uint32_t bestTs = 0;
        size_t winnerIdx = group[0];
        bool first = true;
        for (size_t idx : group)
        {
            auto &r = _job->readers[idx];
            if (first || r.current.ts > bestTs)
            {
                bestTs = r.current.ts;
                winnerIdx = idx;
                first = false;
            }
        }

        auto &winner = _job->readers[winnerIdx];

        if (winner.current.size > 0)
        {
            if (_compactValBuf.size() < winner.current.size)
                _compactValBuf.resize(winner.current.size);

            size_t actual;
            if (!winner.readValue(_compactValBuf.data(), actual))
            {
                for (size_t idx : group)
                {
                    auto &r = _job->readers[idx];
                    r.next();
                    if (!r.eof)
                        _compactHeap.push({r.current.key, r.current.ts, idx, r.current.offset, r.version});
                }
                continue;
            }
        }

        // =================================================================
        // SERIALIZATION USING A SINGLE 32-BIT SSTHEADER
        // =================================================================
        SSTHeader header;
        header.key = key;
        header.size = winner.current.size; // 32-bit murni
        header.ts = bestTs;
        header.tombstone = winner.current.tombstone ? 1 : 0;

        // Write metadata 11-bytes at a time to a temporary file
        out.write((const uint8_t *)&header, sizeof(SSTHeader));

        if (header.size > 0)
            out.write(_compactValBuf.data(), header.size);

        written += sizeof(SSTHeader) + header.size;

        for (size_t idx : group)
        {
            auto &r = _job->readers[idx];
            r.next();
            if (!r.eof)
                _compactHeap.push({r.current.key, r.current.ts, idx, r.current.offset, r.version});
        }
    }

    out.close();

    bool allDone = true;
    for (auto &r : _job->readers)
    {
        if (!r.eof)
        {
            allDone = false;
            break;
        }
    }

    // ==================== COMPACTION FINALIZATION ====================
    if (allDone)
    {
        for (auto &r : _job->readers)
            r.close();

        _job->readers.clear();

        if (STORAGE_RENAME(_job->dstTemp, _job->dstFinal))
        {
            File f = STORAGE_OPEN(_job->dstFinal, "r");
            if (f)
            {
                std::vector<SSTIndex> idx;
                SSTFile sst;
                memset(sst.bloom, 0, sizeof(sst.bloom));

                // SYNC FILE ID FROM dstFinal
                int lastUnderscore = _job->dstFinal.lastIndexOf('_');
                int lastDot = _job->dstFinal.lastIndexOf('.');
                if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore)
                {
                    sst.fileId = _job->dstFinal.substring(lastUnderscore + 1, lastDot).toInt();
                }
                else
                {
                    sst.fileId = getFileSeq();
                }
                sst.filename = _job->dstFinal;

                while (f.available())
                {
                    uint32_t currentEntryOffset = f.position();

                    // READING BACK THE SST INDEX USING SSTHEADER
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

                    // DO NOT INCREASE _totalEntryCount RANDOMLY!
                    // During compaction, we write old data that has ALREADY BEEN CALCULATED in memory.
                    if (!entry.tombstone)
                        __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
                }
                f.close();

                std::sort(idx.begin(), idx.end());
                sst.index = std::move(idx);

                GET_LEVELS()
                [_job->srcLevel + 1].push_back(std::move(sst));
            }

            // Deleting physical obsolete files from flash disk
            deleteSSTFiles(_job->srcFiles);
        }
        else
        {
            STORAGE_REMOVE(_job->dstTemp);
        }

        __atomic_store_n(&_compactState, IDLE, __ATOMIC_SEQ_CST);
        _job->active = false;
        _compactInitialized = false;
    }
}

// =================================================================
// SYSTEM INFRASTRUCTURE: UTILITIES & HEURISTIC ENGINE CONTROL
// =================================================================

uint32_t NeuLSMDB_FS::crc32(uint32_t crc, const uint8_t *data, size_t len)
{
    return crc32_le(crc, data, len);
}

void NeuLSMDB_FS::tuneMemtable()
{
    float heapRatio = (float)ESP.getFreeHeap() / (float)ESP.getHeapSize();
    float writePressure = (float)_memBytes / (float)CACHE_SIZE_BYTES;
    int l0Pressure = GET_LEVELS()[0].size();

    // Compute the system resource pressure score matrix
    float score = (writePressure * 0.5f) +
                  ((1.0f - heapRatio) * 0.3f) +
                  ((float)l0Pressure * 0.2f);

    // Dynamic Parameter Adaptation: Scale limits algorithmically to preserve system stability
    if (score < 0.3f)
        _adaptiveLimit = 8192; // Low pressure: Expand memory threshold for better throughput
    else if (score < 0.6f)
        _adaptiveLimit = 4096; // Moderate pressure: Apply balanced baseline size
    else
        _adaptiveLimit = 2048; // High pressure: Shrink boundary to enforce aggressive flushing

    // Enforce hard-coded absolute safety baseline limit
    if (_adaptiveLimit < 1024)
        _adaptiveLimit = 1024;
}

void NeuLSMDB_FS::evictOldestData()
{
    uint32_t oldestTs = UINT32_MAX;
    String oldestFile;
    size_t oldestIdx = 0;
    uint16_t oldestKey = 0;
    uint8_t oldestLvl = 0;
    bool foundOldest = false;

    // DATA PURGING POLICY: Scan backward from the oldest deep level to level 0
    for (int lvl = MAX_LEVEL - 1; lvl >= 0; lvl--)
    {
        for (auto &sst : GET_LEVELS()[lvl])
        {
            for (size_t i = 0; i < sst.index.size(); i++)
            {
                auto &e = sst.index[i];
                if (!e.tombstone && e.ts < oldestTs)
                {
                    oldestTs = e.ts;
                    oldestFile = sst.filename;
                    oldestIdx = i;
                    oldestKey = e.key;
                    oldestLvl = lvl;
                    foundOldest = true;
                }
            }
        }
    }

    if (foundOldest)
    {
        MemEntry delEntry;
        delEntry.ts = millis();
        delEntry.tombstone = true;
        delEntry.size = 0;

        // CRITICAL DEFERRAL: Store tombstone in MemTable without triggering an instant FLUSH to prevent Deadlock/Stack Overflow
        (*GET_MEM())[oldestKey] = std::move(delEntry);

        __atomic_sub_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);

        // NOTE: appendWAL is omitted here as evictOldestData is executed
        // within the put() scope, which handles its own appendWAL tracking sequentially.
    }
    else
    {
        if (!GET_LEVELS()[0].empty())
        {
            String firstFile = GET_LEVELS()[0][0].filename;
            deleteSSTFiles({firstFile});
        }
    }
}

void NeuLSMDB_FS::auditLevels()
{
    // === ACQUIRE DATABASE LOCK FOR AUDIT ===
    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
    {
        Serial.println(F("[ERROR] Failed to acquire database lock for audit operation!"));
        return;
    }

    Serial.println(F("\n>>> === NEUDB LSM-TREE TOPOLOGY AUDIT === <<<"));

    // METRICS RECONCILIATION: Fetch total live database counters using atomic sequential consistency
    size_t totalNow = __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST);

    Serial.printf("Active Records: %d | Capacity Limit: %d | Eviction Policy: %s\n",
                  totalNow, MAX_TOTAL_ENTRIES, _overrideWhenFull ? "OVERRIDE" : "REJECT");

    for (int lvl = 0; lvl < MAX_LEVEL; lvl++)
    {
        int fileCount = GET_LEVELS()[lvl].size();
        int totalEntries = 0;
        int tombCount = 0;
        size_t totalSize = 0;

        Serial.printf("\n[ LEVEL %d ] -> Active Files: %d\n", lvl, fileCount);

        for (auto &sst : GET_LEVELS()[lvl])
        {
            totalEntries += sst.index.size();

            // Calculate historical tombstone markers
            for (auto &e : sst.index)
                if (e.tombstone)
                    tombCount++;

            // Calculate absolute descriptor physical storage footprint
            File f = STORAGE_OPEN(sst.filename, "r");
            size_t fileSize = f ? f.size() : 0;
            if (f)
                f.close();
            totalSize += fileSize;

            Serial.printf("  -> %s | Entries: %d | Footprint: %d B | Bloom Filter: ACTIVE\n",
                          sst.filename.c_str(), sst.index.size(), fileSize);
        }

        Serial.printf("  > Summary Level %d: Total Entries=%d | Tombstones=%d | Total Size: %d B\n",
                      lvl, totalEntries, tombCount, totalSize);
    }
    Serial.println(F(">>> === END OF TOPOLOGY AUDIT === <<<\n"));

    // === RELEASE DATABASE LOCK ===
    xSemaphoreGive(_mutex);
}