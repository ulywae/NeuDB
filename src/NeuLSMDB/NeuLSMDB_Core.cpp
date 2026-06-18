#include "NeuLSMDB/DB_Token.h"
#include "NeuLSMDB/NeuLSMDB.h"

#include "rom/crc.h"
#include <Arduino.h>
#include <algorithm>
#include <cstring>
#include <list>
#include <memory>

// =================================================================
// TYPE ABSTRACTION LAYER: OPAQUE POINTER DATA ARCHITECTURE MAPPING
// =================================================================
#define GET_LEVELS() (_levels)
#define GET_MEM() static_cast<std::map<uint32_t, NeuLSMDB::MemEntry> *>(_mem)
#define GET_CACHE_LIST()                                                       \
  static_cast<std::list<NeuLSMDB::CacheBlock> *>(_cacheList)
#define GET_CACHE_MAP()                                                        \
  static_cast<                                                                 \
      std::map<uint64_t, std::list<NeuLSMDB::CacheBlock>::iterator> *>(        \
      _cacheMap)

#define GET_LEVELS_LOG() (_levelsLog)

struct NeuLSMDB::CacheBlock {
  uint64_t cacheKey;
  std::vector<uint8_t> data;
};

// =================================================================
// CONTEXT INSTANTIATION: CONSTRUCTOR & DESTRUCTOR ALLOCATIONS
// =================================================================

NeuLSMDB::NeuLSMDB() {
  // HEAP ALLOCATION: Instantiate physical structures behind data isolation
  // masks
  _mem = new std::map<uint32_t, MemEntry>();
  // _levels and _levelsLog are now statically allocated arrays inside the class
  // header

  _cacheList = new std::list<CacheBlock>();
  _cacheMap = new std::map<uint64_t, std::list<CacheBlock>::iterator>();

  _job = new CompactJob();
  _jobLog = new CompactJob();

  // METRICS INITIALIZATION: Baseline structural limits
  _memCount = 0;
  _memBytes = 0;
  _nextFileId = 1;
  _lastFlush = 0;
  _lastTune = 0;
  _cacheUsed = 0;
  _adaptiveLimit = 4096;
  _compactState = IDLE;
  _compactLogState = IDLE;
  _overrideWhenFull = true;
  _totalEntryCount = 0;

  _job->active = false;
  _jobLog->active = false;

  _systemReady = false;
  _stopTaskRequested = false;
  _compactLogInitialized = false;

  // KERNEL INITIALIZATION: Instantiate thread-safety synchronization handles
  _mutex = xSemaphoreCreateMutex();
}

NeuLSMDB::~NeuLSMDB() {
  flush();
  if (_walFile)
    _walFile.close();

  // MEMORY SANITIZATION: Reclaim heap space allocated behind opaque pointer
  // masks
  delete GET_MEM();
  // GET_LEVELS() and GET_LEVELS_LOG() are now statically allocated

  delete GET_CACHE_LIST();
  delete GET_CACHE_MAP();

  delete _job;
  delete _jobLog;

  if (_mutex)
    vSemaphoreDelete(_mutex);
}

// =================================================================
// BOOTSTRAP CONTROL PIPELINE: SYSTEM ENGINE BOOT INIT
// =================================================================

bool NeuLSMDB::init() {
  if (_systemReady)
    return true;

  // VFS INITIALIZATION: Mount partition topology map via low-level storage
  // drivers.
  if (!STORAGE_INIT())
    return false;

  // SYSTEM PATH CHECK: Guarantee transactional directory space profiles exist
  // safely.
  if (!STORAGE_EXISTS("/lsm"))
    STORAGE_MKDIR("/lsm");

  // DATA RECOVERY SEQUENCING: Reconstruct operational system topology maps.
  // Scan and reconstruct the volatile RAM metadata memory map for regular
  // storage segments.
  loadAllSST();

  // ========================================================================
  // LOG RECOVERY SEQUENCING: RECONSTRUCT HISTORICAL LOG TOPOLOGY IN RAM
  // ========================================================================
  // Scan and mount all partitioned 'log_lvX_Y.sst' file blocks from
  // non-volatile disk storage. This populates the dedicated high-address log
  // metadata index map during early boot initialization.
  loadAllSSTLog();

  // Reconstruct the remaining runtime transaction delta states left inside the
  // volatile pool before a crash. This sweeps the write-ahead log to
  // reconstruct un-flushed regular and log records into the active RAM
  // MemTable.
  replayWAL();

  // STORAGE ACCESS CHANNEL: Open append-only pipeline stream to commit
  // transaction history log.
  _walFile = STORAGE_OPEN("/lsm/wal.log", FILE_APPEND);
  if (!_walFile)
    return false;

  _lastFlush = millis();
  _lastTune = millis();

  _stopTaskRequested = false;
  _systemReady = true;

  if (_taskHandle == NULL) {
    // KERNEL DISPATCHER: Isolate low-priority structural reorganization tasks
    // to physical CPU Core 1. This offloads heavy multi-way merge compactions
    // away from Core 0 to protect active application threads.
    xTaskCreatePinnedToCore(
        [](void *param) {
          NeuLSMDB *db = static_cast<NeuLSMDB *>(param);
          for (;;) {
            // INTERRUPT SERVICE DETECTOR: Gracefully break continuous
            // processing if termination flags match.
            if (db->_stopTaskRequested)
              break;

            db->tick();
            vTaskDelay(
                pdMS_TO_TICKS(5)); // Relinquish CPU execution control window to
                                   // prevent watchdog starvation
          }

          TaskHandle_t localHandle = db->_taskHandle;
          db->_taskHandle = NULL;
          vTaskDelete(
              NULL); // Terminate background process task context cleanly
        },
        "LSM_Task", 4096, this, 1, &_taskHandle, 1);
  }

  return true;
}

bool NeuLSMDB::format() {
  // Execution Suspension Phase: Transition active pipeline states to safe
  // boundaries and broadcast a shutdown signal to the asynchronous worker
  // thread.
  _systemReady = false;
  _compactState = IDLE;
  _compactLogState = IDLE;
  _stopTaskRequested = true;

  // Thread Synchronization Fencing: Enforce a strict timed wait interval to
  // allow the asynchronous task handle to exit gracefully on its own accord.
  int timeout = 0;
  while (_taskHandle != NULL && timeout < 100) {
    vTaskDelay(pdMS_TO_TICKS(5));
    timeout++;
  }

  // Task Purge Path: We avoid forcefully deleting the task context here
  // to prevent orphaning the _mutex or LittleFS locks, which would cause a
  // guaranteed deadlock.
  if (_taskHandle != NULL) {
    _taskHandle = NULL;
  }

  if (_mutex != nullptr) {
    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(2000)) != pdTRUE)
      return false;
  }

  // Pipeline Sanitization Phase: Reset active structural reorganization
  // trackers for regular storage paths.
  if (_job != nullptr) {
    _job->active = false;
    for (auto &reader : _job->readers)
      reader.close();
    _job->readers.clear();
    _job->srcFiles.clear();
  }

  // ========================================================================
  // 2. LOG PIPELINE SANITIZATION: PURGE BACKGROUND COMPACTION JOBS
  // ========================================================================
  // Reset active structural reorganization trackers and close outstanding file
  // descriptors specifically belonging to the isolated log pipeline cascade.
  if (_jobLog != nullptr) {
    _jobLog->active = false;
    for (auto &reader : _jobLog->readers)
      reader.close();
    _jobLog->readers.clear();
    _jobLog->srcFiles.clear();
  }

  if (_walFile)
    _walFile.close();

  // Storage Destruction Sequence: Enumerate and recursively sweep the
  // non-volatile disk directory.
  std::vector<String> listFileDelete;

  if (STORAGE_EXISTS("/lsm")) {
    File dir = STORAGE_OPEN("/lsm", "r");
    if (dir && dir.isDirectory()) {
      File file = dir.openNextFile();
      while (file) {
        listFileDelete.push_back(String("/lsm/") + file.name());
        file.close();
        file = dir.openNextFile();
      }
    }
    if (dir)
      dir.close();

    // Disk Block Eviction: Clear physical files from the storage layer.
    // This systematically erases all immutable regular blocks and rolling log
    // files simultaneously.
    for (const auto &filePath : listFileDelete) {
      STORAGE_REMOVE(filePath);
      vTaskDelay(pdMS_TO_TICKS(2)); // Introduce pacing delays to allow the
                                    // virtual filesystem layout to settle
    }
    STORAGE_RMDIR("/lsm");
  }
  STORAGE_MKDIR("/lsm");

  // Volatile Memory Sanitization Phase: Wipe shared runtime map states.
  // Clearing this unified container removes both regular records and log
  // records instantly.
  auto memPtr = GET_MEM();
  if (memPtr != nullptr)
    memPtr->clear();

  // Level Index Wiping: Clear regular RAM vector indexes across all tree
  // depths.
  auto levelsPtr = GET_LEVELS();
  if (levelsPtr != nullptr) {
    for (uint8_t i = 0; i < MAX_LEVEL; i++)
      levelsPtr[i].clear();
  }

  // ========================================================================
  // 3. RAM METADATA SANITIZATION: WIPE LOG LEVEL RAM VECTOR INDEX
  // ========================================================================
  // Clear log RAM vector indexes across all tree depths to cleanly reset the
  // isolated log pipeline track.
  auto levelsLogPtr = GET_LEVELS_LOG();
  if (levelsLogPtr != nullptr) {
    for (uint8_t i = 0; i < MAX_LEVEL; i++)
      levelsLogPtr[i].clear();
  }

  // Acceleration Block Sanitization: ClearLeast-Recently-Used (LRU) cache lists
  // and lookup registers.
  auto cacheListPtr = GET_CACHE_LIST();
  if (cacheListPtr != nullptr)
    cacheListPtr->clear();

  auto cacheMapPtr = GET_CACHE_MAP();
  if (cacheMapPtr != nullptr)
    cacheMapPtr->clear();

  // Metric Invalidation Sequence: Enforce absolute sequential consistency while
  // resetting atomic variables.
  __atomic_store_n(&_memCount, 0, __ATOMIC_SEQ_CST);
  __atomic_store_n(&_totalEntryCount, 0, __ATOMIC_SEQ_CST);
  _memBytes = 0;
  _cacheUsed = 0;
  _lastFlush = millis();
  _lastTune = millis();

  // Reset log-specific compaction scheduler initial status flags
  _compactLogInitialized = false;

  if (_mutex != nullptr)
    xSemaphoreGive(_mutex);

  // Engine Reboot: Trigger a cold system bootstrap sequence to re-mount fresh,
  // blank directory paths.
  return init();
}

void NeuLSMDB::setOverrideWhenFull(bool enable) { _overrideWhenFull = enable; }
bool NeuLSMDB::getOverrideWhenFull() const { return _overrideWhenFull; }

bool NeuLSMDB::del(uint16_t key) {
  // Defensive Range Validation: Instantly reject out-of-bounds keys before
  // securing resource locks.
  if (!_systemReady || key >= NEU_KEY_SPACE_LIMIT)
    return false;

  // Mutex Acquisition: Acquire the unified system lock block to prevent dynamic
  // multi-core race conditions.
  if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
    return false;

  bool res = false;

  do {
    // Resource Capacity Guard: Intercept write cycles to execute reactive
    // eviction parameters if limits breach constraints.
    if (_flashFullGuard ||
        __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST) >=
            MAX_TOTAL_ENTRIES) {
      if (_overrideWhenFull)
        evictOldestData();
      else
        break;
    }

    // Write-Ahead Log (WAL) Serialization Channel: Commit the deletion
    // cancellation frame to physical disk instantly. Size parameter is locked
    // strictly to 0, and the tombstone boolean flag parameter is forced to
    // true.
    int retryWAL = 0;
    bool walSuccess = false;
    while (retryWAL < 10) {
      if (appendWAL(key, nullptr, 0,
                    true)) // Ingesting (key, nullptr, size=0, tombstone=true)
      {
        walSuccess = true;
        break;
      }
      xSemaphoreGive(_mutex);
      vTaskDelay(pdMS_TO_TICKS(2)); // Back off control window to allow
                                    // outstanding sector flushes to complete
      if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        break;
      retryWAL++;
    }

    if (!walSuccess)
      break;

    uint32_t now = millis();
    auto &mapMem = *GET_MEM();

    // Tombstone Frame Assignment: Construct an empty data node containing the
    // cancellation marker.
    MemEntry tombEntry;
    tombEntry.size = 0;
    tombEntry.ts = now;
    tombEntry.tombstone = true;

    auto it = mapMem.find(key);
    if (it != mapMem.end()) {
      // Mutation Path: Replace preexisting memory bytes allocation values and
      // assign the tombstone node directly.
      _memBytes -= it->second.size;
      mapMem[key] = std::move(tombEntry);
    } else {
      // Allocation Path: Inject the brand new cancellation node into the
      // volatile lookup tree map structure.
      mapMem[key] = std::move(tombEntry);
      __atomic_add_fetch(&_memCount, 1, __ATOMIC_SEQ_CST);
    }

    // Global Metrics Balancing: Safely decrement live active system metrics
    // records counter tracking variables.
    size_t currentTotal = __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST);
    if (currentTotal > 0) {
      __atomic_sub_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
    }

    res = true;
  } while (0);

  xSemaphoreGive(_mutex);
  return res;
}

// ==========================================
// LOOP SISTEM: tick()
// ==========================================
void NeuLSMDB::tick() {
  // 1. Core safety guard to prevent execution before bootstrap sequence
  // completes
  if (!_systemReady)
    return;

  // NOTE: Allow tick() to execute concurrently without holding any global lock
  // mutex constraints.

  // ========================================================================
  // PREDICTIVE RESOURCE-AWARE AUTO-TUNING
  // ========================================================================
  // Asynchronously compute current hardware stress matrices (RAM heap, write
  // volume, L0 file pressure) and recalculate the elastic _adaptiveLimit
  // threshold before checking boundaries.
  tuneMemtable();

  // 2. Evaluate and handle periodic Write-Ahead Log serialization (flushWAL)
  // every 200ms
  if (millis() - _lastFlush >= 200) {
    // Apply fixed-timestep accumulator trick to guarantee consistent 200ms
    // scheduling intervals
    _lastFlush += 200;
    flushWAL();
  }

  // 3. Monitor if RAM MemTable capacity boundaries or entry counts breach
  // active profile limits Read the atomic _memCount variable safely without
  // locking overhead
  size_t currentMemCount = __atomic_load_n(&_memCount, __ATOMIC_SEQ_CST);

  if (currentMemCount >= MEMTABLE_MAX_ENTRIES || _memBytes >= _adaptiveLimit) {
    // Call flush(). The original flush() function is already equipped with
    // internal xSemaphoreTake, so it will lock itself safely!
    flush();
  }

  // ========================================================================
  // DELEGATE LSM TREE COMPACTION ROUTINES (REGULAR & LOG PIPELINE)
  // ========================================================================
  // Allow the compaction engine pipelines to manage their own granular mutex
  // locking internally
  if (_compactState != IDLE)
    tickCompact();
  else
    runCompactionScheduler();

  if (_compactLogState != IDLE)
    tickCompactLog();
  else
    runLogCompactionScheduler();
}

void NeuLSMDB::runCompactionScheduler() {
  // Concurrency Verification Phase: Read the current compaction pipeline phase
  // atomically. Early exit if any background structural data reorganization job
  // is already active.
  CompactState st = __atomic_load_n(&_compactState, __ATOMIC_SEQ_CST);
  if (st != IDLE)
    return;

  // Hard Level-0 Density Barrier: Instantly trigger an immutable level merge if
  // the Level 0 file count exceeds the hard threshold. This directly curtails
  // query lookup degradation caused by un-compacted file sprawl.
  if (GET_LEVELS()[0].size() > 6) {
    triggerCompaction(0);
    return;
  }

  // Heuristic Level Sizing Sweeper: Scan deep architectural tree tiers to
  // evaluate volume imbalances.
  for (int lvl = 0; lvl < MAX_LEVEL - 1; lvl++) {
    // Exponential Threshold Strategy: Compute file density caps algorithmically
    // using bitwise shifts (2^lvl). This optimizes structural scaling
    // parameters dynamically as data flows down into deeper levels.
    int thr = (lvl == 0) ? 2 : (2 << lvl);

    // Complex Boundary Invalidation Fence: Evaluate tiered capacity limits over
    // regular storage partitions. Once a specific tier breaches its file volume
    // ceiling, a background compaction worker job is enqueued.
    if ((lvl == 0 &&
         (GET_LEVELS()[lvl].size() >= thr || GET_LEVELS()[lvl].size() >= 10)) ||
        (lvl > 0 && GET_LEVELS()[lvl].size() >= thr)) {
      triggerCompaction(lvl);
      return;
    }
  }
}

// ==========================================
// WRITE PATH PIPELINE: put()
// ==========================================

bool NeuLSMDB::put(uint16_t key, const void *data, size_t size) {
  if (!_systemReady || key >= NEU_KEY_SPACE_LIMIT || size > 65535)
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
  if (__atomic_load_n(&_compactState, __ATOMIC_SEQ_CST) == MERGE_STREAM) {
    xSemaphoreGive(_mutex); // Release the unified key resource lock
    vTaskDelay(
        pdMS_TO_TICKS(4)); // Force a 4-millisecond adaptive backoff window
    if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
      return false;
  }

  bool res = false;

  do {
    // =================================================================
    // STORAGE INTEGRITY: RESOURCE CAPACITY BOUNDARY INTERRUPT
    // =================================================================
    if (_flashFullGuard ||
        __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST) >=
            MAX_TOTAL_ENTRIES) {
      if (_overrideWhenFull)
        evictOldestData(); // Force reactive cache eviction on active MemTable
                           // element
      else
        break; // Hard abort incoming transactions if override policy is
               // suppressed
    }

    // =================================================================
    // CONCURRENCY CONTROL: ADAPTIVE WAL TRANSACTION QUEUE RETRY LOOPS
    // =================================================================
    int retryWAL = 0;
    bool walSuccess = false;
    while (retryWAL < 10) {
      if (appendWAL(key, data, size, false)) {
        walSuccess = true;
        break;
      }

      // Yield CPU control context to resolve background flush resource locks
      xSemaphoreGive(_mutex); // Temporarily release resource lock allocation
      vTaskDelay(pdMS_TO_TICKS(
          2)); // Force a brief 2ms backoff delay to allow background task to
               // complete flush and release locks
      if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
        break;

      retryWAL++;
    }

    if (!walSuccess)
      break; // Terminate state processing if write pipeline execution fails

    uint32_t now = millis();
    auto &mapMem = *GET_MEM();
    auto it = mapMem.find(key);

    if (it != mapMem.end()) {
      // MUTATION PATH: UPDATE IN-MEMORY RECORD VECTOR
      _memBytes -= it->second.size;
      it->second.value.reset(new uint8_t[size]);
      if (data)
        memcpy(it->second.value.get(), data, size);
      it->second.size = size;
      it->second.ts = now;
      it->second.tombstone = (size == 0);
      _memBytes += size;
    } else {
      // ALLOCATION PATH: INSERT FRESH MEMTABLE TRANSACTION RECORD
      MemEntry e;
      if (size > 0 && data) {
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

bool NeuLSMDB::get(uint16_t key, void *out, size_t &size) {
  if (!_systemReady)
    return false;

  if (key >= NEU_KEY_SPACE_LIMIT) {
    size = 0;
    return false; // Fast-fail early exit before acquiring mutex to optimize CPU
                  // cycles
  }

  // ========================================================================
  // ADAPTIVE READ-STALL POLICY: CONCURRENCY DESCRIPTOR SATURATION BRAKE
  // ========================================================================
  size_t level0Density = GET_LEVELS()[0].size();

  if (level0Density >= 16) {
    // Critical Saturation Boundary: Force a 4-millisecond reactive backoff
    // window to allow the background K-Way Merge worker to clear out Level 0
    // file shards.
    vTaskDelay(pdMS_TO_TICKS(4));
  } else if (level0Density >= 8) {
    // Elevated Pressure Alert: Execute a brief 1ms cooperative multitask yield
    // to appease the ESP32 Task Watchdog Timer (TWDT) boundaries.
    vTaskDelay(pdMS_TO_TICKS(1));
  }

  if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE) {
    return false;
  }
  bool seek = false;

  do {
    // 1. VOLATILE MEMORY READ PIPELINE (RAM PHASE LOOKUP)
    auto &mapMem = *GET_MEM();
    auto it = mapMem.find(key);
    if (it != mapMem.end()) {
      if (it->second.tombstone) {
        size = 0;
        break; // Key found but status DELETED (Tombstone) -> Return false
      }

      // Save the original size of the data to the size variable so that the
      // user knows the original size.
      size_t requiredSize = it->second.size;
      if (size < requiredSize) {
        size = requiredSize; // Tell me the size you need
        break; // Exit with seek status = false (Buffer not big enough)
      }

      if (requiredSize > 0 && out && it->second.value) {
        memcpy(out, it->second.value.get(), requiredSize);
      }
      size = requiredSize;
      seek = true;
      break;
    }

    // 2. PERSISTENT STORAGE SEARCH PIPELINE (DISPATCH LOOP LAYER)
    for (int lvl = 0; lvl < MAX_LEVEL; lvl++) {
      auto &level = GET_LEVELS()[lvl];
      for (auto itSST = level.rbegin(); itSST != level.rend(); ++itSST) {
        auto &sst = *itSST;

        if (!bloomCheck(sst.bloom, key))
          continue;

        // Binary search on SST index in RAM
        auto idxIt = std::lower_bound(sst.index.begin(), sst.index.end(),
                                      SSTIndex{(uint32_t)key, 0, 0, 0, false});
        if (idxIt != sst.index.end() && idxIt->key == key) {
          // TOMBSTONE INTERCEPTION DIRECTLY FROM RAM INDEX
          if (idxIt->tombstone) {
            size = 0;
            seek = false; // Official key deleted at this sst level
            goto end_get;
          }

          // Validate buffer capacity before reading physical files (saves I/O)
          if (size < idxIt->size) {
            size = idxIt->size; // Tell the user the original size required.
            seek = false;
            goto end_get;
          }

          size_t tmp = size;
          // Perform a physical read to disk via readSST
          if (readSST(sst, *idxIt, out, tmp)) {
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
void NeuLSMDB::flush() {
  if (!_systemReady)
    return;

  // Concurrency Lock Acquisition: Acquire the unified database mutex resource
  // lock. This serializes the memory serialization pass to ensure atomic data
  // transitions.
  if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE)
    return;

  auto &mapMem = *GET_MEM();

  if (mapMem.empty()) {
    xSemaphoreGive(_mutex);
    return;
  }

  // ========================================================================
  // RESOURCE MONITORING LAYER: HARDWARE CEILING WITH SDCARD FILTER
  // ========================================================================
  size_t totalBytesFlash = 0;
  size_t usedBytesFlash = 0;

#ifdef USE_SDCARD
  if (_flushCounter % 10 == 0 || _totalBytesFlash == 0) {
    totalBytesFlash = STORAGE_TOTAL();
    usedBytesFlash = STORAGE_USED();

    _totalBytesFlash = totalBytesFlash;
    _usedBytesFlash = usedBytesFlash;
  } else {
    totalBytesFlash = _totalBytesFlash;
    usedBytesFlash = _usedBytesFlash;
  }
  _flushCounter++;
#else
  totalBytesFlash = STORAGE_TOTAL();
  usedBytesFlash = STORAGE_USED();

  _totalBytesFlash = totalBytesFlash;
  _usedBytesFlash = usedBytesFlash;
#endif

  // Enforce a strict 90% non-volatile hardware storage safety ceiling.
  _flashFullGuard = (usedBytesFlash >= (totalBytesFlash * 9) / 10);

  // ========================================================================
  // DATA SPLITTER PIPELINE: STRUCTURAL ISOLATION LAYER
  // ========================================================================
  // Allocate dual stack containers to partition the blended memory map.
  // This forms the core backbone of our isolated Dual-Pipeline storage
  // architecture.
  std::map<uint32_t, MemEntry> regulerEntries;
  std::map<uint32_t, MemEntry> logEntries;

  for (auto &kv : mapMem) {
    // Address Isolation Logic: Evaluate the key coordinate against the
    // high-address offset baseline. Keys above the offset are automatically
    // routed to the historical rolling log track.
    if (kv.first >= NEU_LOG_KEY_OFFSET) {
      // Zero-Copy Optimization Pass: Move volatile pointers into the log pool
      // via std::move to eliminate expensive deep heap copy overhead operations
      // during memory splitting.
      logEntries[kv.first] = std::move(kv.second);
    } else {
      // Move volatile pointers into the regular storage data pool cleanly.
      regulerEntries[kv.first] = std::move(kv.second);
    }
  }

  // ========================================================================
  // DUAL PERSISTENCE SYNC TRANSACTION
  // ========================================================================
  bool flushSuccess = true;

  // Serialization Phase 1: Flush regular transactions down into immutable Level
  // 0 regular tables.
  if (!regulerEntries.empty()) {
    if (!writeSST(0, regulerEntries)) {
      flushSuccess = false;
    }
  }

  // Serialization Phase 2: Flush historical telemetry records down into
  // isolated Level 0 log tables.
  if (!logEntries.empty()) {
    if (!writeSSTLog(0, logEntries)) {
      flushSuccess = false;
    }
  }

  // Fault-Tolerant Transaction Rollback Protocol: Protect memory states from
  // sudden disk I/O errors. If any underlying filesystem write sequence fails,
  // reconstruct the original combined RAM map completely.
  if (!flushSuccess) {
    // Re-inject elements backwards using zero-copy moves to ensure no data is
    // lost during structural failures.
    for (auto &kv : regulerEntries)
      mapMem[kv.first] = std::move(kv.second);
    for (auto &kv : logEntries)
      mapMem[kv.first] = std::move(kv.second);

    xSemaphoreGive(_mutex);
    return;
  }

  // Persistence Commit Synchronization: Clear volatile RAM trackers since all
  // elements are now safely in flash disk storage.
  mapMem.clear();
  _memBytes = 0;
  __atomic_store_n(&_memCount, 0, __ATOMIC_SEQ_CST);

  // Close and truncate the old Write-Ahead Log (WAL) descriptor from the active
  // storage layer.
  clearWAL();

  // Re-initialize an append-only file channel stream to log the next incoming
  // transactional lifecycle sequence.
  _walFile = STORAGE_OPEN("/lsm/wal.log", FILE_APPEND);
  if (!_walFile) {
    // Fallback error descriptor logging can be appended here if required.
  }

  _lastFlush = millis();

  xSemaphoreGive(_mutex);
}

// =================================================================
// TRANSACTION LOG MANAGEMENT: WRITE-AHEAD LOGGING (WAL) SUBSYSTEM
// =================================================================

bool NeuLSMDB::appendWAL(uint16_t key, const void *data, size_t size,
                         bool tombstone) {
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
  // PIPELINE GUARD: Stream the 16-bit key and 32-bit payload size descriptors
  // directly into VFS block device. Instantly abort the transaction pipeline if
  // any fixed-length layout block write operation fails.
  if (_walFile.write((const uint8_t *)&key, sizeof(key)) != sizeof(key) ||
      _walFile.write((const uint8_t *)&writeSize, sizeof(writeSize)) !=
          sizeof(writeSize))
    return false;

  if (writeSize > 0 && data) {
    if (_walFile.write((const uint8_t *)data, writeSize) != writeSize)
      return false;
  }

  // =================================================================
  // TRANSACTION TRAILING DISK COMMIT
  // =================================================================
  // FOOTER GUARD: Commit tombstone state flag and trailing hardware-backed
  // CRC32 checksum to flash. Instantly abort pipeline transaction and return
  // false if any filesystem hardware block write fails.
  if (_walFile.write(&tombByte, 1) != 1 ||
      _walFile.write((const uint8_t *)&crc, sizeof(crc)) != sizeof(crc))
    return false;

  return true;
}

void NeuLSMDB::replayWAL() {
  File wal = STORAGE_OPEN("/lsm/wal.log", "r");
  if (!wal)
    return;

  auto &mapMem = *GET_MEM();
  mapMem.clear();
  _memBytes = 0;
  __atomic_store_n(&_memCount, 0, __ATOMIC_SEQ_CST);

  while (wal.available()) {
    uint16_t key;
    uint32_t sz;
    uint8_t tomb;
    uint32_t crcFile;

    // =================================================================
    // TRANSACTION LIFECYCLE VFS STREAM VALIDATION
    // =================================================================
    // RECOVERY GUARD: Break iteration if stream reads fail OR payload size 'sz'
    // overflows safety bounds
    if (wal.read((uint8_t *)&key, sizeof(key)) != sizeof(key) ||
        wal.read((uint8_t *)&sz, sizeof(sz)) != sizeof(sz) || sz > 4096) {
      break; // Abruptly terminate recovery log parsing if any validation fence
             // is tripped
    }

    std::unique_ptr<uint8_t[]> buf;
    if (sz > 0) {
      buf.reset(new uint8_t[sz]);
      if (wal.read(buf.get(), sz) != sz)
        break;
    }

    // =================================================================
    // TRANSACTION TRAILING PIPELINE VALIDATION
    // =================================================================
    // FOOTER GUARD: Abort tracking if tombstone flag or trailing CRC32
    // extraction fails
    if (wal.read(&tomb, 1) != 1 ||
        wal.read((uint8_t *)&crcFile, sizeof(crcFile)) != sizeof(crcFile)) {
      break; // Abruptly terminate log parsing on unexpected trailing stream
             // truncation
    }

    // Validate CRC integrity using standard hardware-backed calculation
    uint32_t crcCalc = crc32_le(0, (const uint8_t *)&key, sizeof(key));
    crcCalc = crc32_le(crcCalc, (const uint8_t *)&sz, sizeof(sz));
    if (sz > 0)
      crcCalc = crc32_le(crcCalc, buf.get(), sz);
    crcCalc = crc32_le(crcCalc, &tomb, 1);

    if (crcCalc != crcFile) {
      // This point denotes the final boundary of valid transactions before the
      // sudden power blackout
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
    if (it != mapMem.end()) {
      // If the key is duplicate, reduce the size of the old data first.
      _memBytes -= it->second.size;
    } else {
      // If this is a new key, increment the atomic count of MemTable entries
      // and the System Total.
      __atomic_add_fetch(&_memCount, 1, __ATOMIC_SEQ_CST);
      __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
    }

    // Enter new entry / update old entry
    _memBytes += sz;
    mapMem[key] = std::move(e);
  }
  wal.close();
}

void NeuLSMDB::clearWAL() {
  // Tasked exclusively with closing the active handle and executing physical
  // file truncation/deletion!
  if (_walFile)
    _walFile.close();
  STORAGE_REMOVE("/lsm/wal.log");
}

void NeuLSMDB::flushWAL() {
  // Since tick() runs in its own task, it is mandatory to grab the mutex before
  // touching _walFile.
  if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(10)) == pdTRUE) {
    if (_walFile) {
      _walFile.flush(); // Force data from RAM buffer to Flash chip every 200ms
    }
    xSemaphoreGive(_mutex);
  }
}

// =================================================================
// IMMUTABLE STORAGE SUBSYSTEM: SORTED STRING TABLE (SST) MANAGEMENT
// =================================================================

String NeuLSMDB::makeFilename(uint8_t level, uint32_t seq) {
  return "/lsm/lv" + String(level) + "_" + String(seq) + ".sst";
}

uint32_t NeuLSMDB::getFileSeq() { return _nextFileId++; }

bool NeuLSMDB::writeSST(uint8_t level,
                        const std::map<uint32_t, MemEntry> &entries,
                        const String &dstFile) {
  if (entries.empty())
    return true;

  uint32_t fid = 0;
  String fn;

  // File Target Resolver: Re-use an assigned destination name string
  // (compaction path) or generate a brand new Level 0 state block from our
  // linear global unique identifier sequence.
  if (dstFile.length() > 0) {
    fn = dstFile;
    int lastUnderscore = dstFile.lastIndexOf('_');
    int lastDot = dstFile.lastIndexOf('.');
    if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore) {
      fid = dstFile.substring(lastUnderscore + 1, lastDot).toInt();
    } else {
      fid = getFileSeq();
    }
  } else {
    fid = getFileSeq();
    fn = makeFilename(level, fid);
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

  // Transaction Conversion Pass: Linear sweep across the map structure to
  // execute binary block writes.
  for (auto &kv : entries) {
    uint32_t k = kv.first; // Implicit type alignment to match the custom 32-bit
                           // internal register coordinator.
    const MemEntry &e = kv.second;

    uint32_t currentPos = f.position();

    SSTHeader header;
    header.key = k;
    header.size = e.size;
    header.ts = e.ts;
    header.tombstone = e.tombstone ? 1 : 0;

    // Metadata Commit Phase: Direct byte write of fixed-length layout frames
    // onto non-volatile target blocks. If the file descriptor fails to write,
    // instantly truncate the link to prevent storage fragmentation.
    if (f.write((const uint8_t *)&header, sizeof(SSTHeader)) !=
        sizeof(SSTHeader)) {
      f.close();
      return false;
    }

    // Payload Commit Phase: Stream raw data structures onto flash sectors
    // directly trailing the header frame.
    if (header.size > 0 && e.value) {
      if (f.write(e.value.get(), header.size) != header.size) {
        f.close();
        return false;
      }
    }

    // Memory Index Construction: Map runtime file-pointer bounds for fast
    // lookups. This hydatrates our active probabilistic bloom filter bitmask
    // matrix concurrently during the write pass.
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

  // Index Standardization Pass: Sort the extracted tracking elements
  // lexicographically by key to satisfy strict binary search lookahead
  // constraints inside our unified non-volatile read path layer.
  std::sort(idx.begin(), idx.end());
  sstOut.index = std::move(idx);
  GET_LEVELS()
  [level].push_back(std::move(sstOut));

  return true;
}

bool NeuLSMDB::readSST(const SSTFile &sst, const SSTIndex &idxEntry, void *out,
                       size_t &size) {
  // 1. Instantly intercept historical tombstone markers from the passed index
  // parameter
  if (idxEntry.tombstone) {
    size = 0;
    return true;
  }

  // 2. Validate output buffer capacity bounds before committing storage I/O
  // cycles
  if (size < idxEntry.size)
    return false;

  // 3. READ BLOCK CACHE (RAM Phase Lookup)
  std::vector<uint8_t> cacheBuf;
  if (cacheGet(sst.fileId, idxEntry.offset, cacheBuf)) {
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
  // key(2B) + size(4B) + timestamp(4B) + tombstone(1B) = sizeof(SSTHeader) ->
  // 11 bytes
  const uint32_t valueOffset =
      idxEntry.offset + sizeof(SSTHeader); // Automatically worth +11

  if (!f.seek(valueOffset)) {
    f.close();
    return false;
  }

  // Extract storage data record directly into output destination buffer space
  size_t actualRead = f.read((uint8_t *)out, idxEntry.size);
  f.close();

  if (actualRead != idxEntry.size)
    return false;

  // 5. CACHE POPULATION: Populate cold storage payload data into volatile LRU
  // Block Cache
  cachePut(sst.fileId, idxEntry.offset, (const uint8_t *)out, idxEntry.size);
  size = idxEntry.size;

  return true;
}

void NeuLSMDB::loadAllSST() {
  File root = STORAGE_OPEN("/lsm", "r");
  if (!root || !root.isDirectory()) {
    return;
  }

  File f = root.openNextFile();
  int totalFileDitemukan = 0;
  while (f) {
    String nm = f.name();

    // Extract pure filename without path if f.name() returns a full absolute
    // path
    if (nm.lastIndexOf('/') != -1)
      nm = nm.substring(nm.lastIndexOf('/') + 1);

    if (nm.endsWith(".sst") && nm.startsWith("lv")) {
      int lvl = nm.substring(2, nm.indexOf('_')).toInt();

      if (lvl >= 0 && lvl < MAX_LEVEL) {
        String fullPath = "/lsm/" + nm;

        SSTFile sst;
        strncpy(sst.filename, fullPath.c_str(), sizeof(sst.filename) - 1);
        sst.filename[sizeof(sst.filename) - 1] = '\0';

        // Extract the original ID from the file name (eg: "lv0_45.sst" -> 45)
        int lastUnderscore = nm.lastIndexOf('_');
        int lastDot = nm.lastIndexOf('.');
        if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore) {
          sst.fileId = nm.substring(lastUnderscore + 1, lastDot).toInt();
        } else {
          sst.fileId = getFileSeq(); // Fallback if name format is corrupted
        }

        memset(sst.bloom, 0, sizeof(sst.bloom));

        File fd = STORAGE_OPEN(fullPath, "r");
        if (!fd) {
          f = root.openNextFile();
          continue;
        }

        while (fd.available()) {
          uint32_t currentPos = fd.position();

          // 32-bit SSTHeader struct for reading 11 bytes at a time
          SSTHeader header;
          if (fd.read((uint8_t *)&header, sizeof(SSTHeader)) !=
              sizeof(SSTHeader))
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

__attribute__((always_inline)) inline void
NeuLSMDB::internalDeleteSST(const std::vector<String> &files,
                            std::vector<SSTFile> *levelsTarget) {
  // Concurrency Lock Acquisition: Guard the metadata vector arrays from
  // cross-core race conditions by blocking outstanding application write
  // requests until the tracking metadata is purged.
  std::vector<String> filesToDelete;
  if (_mutex && xSemaphoreTake(_mutex, portMAX_DELAY) == pdTRUE) {
    for (auto &fn : files) {
      // Level Hierarchy Sweep: Traverse across all deeper log/regular tree
      // tiers to find the exact target file pointer context in RAM.
      for (int l = 0; l < MAX_LEVEL; l++) {
        auto &currentLevel = levelsTarget[l];

        // Linear Lookahead Search: Locate the targeting SSTable object using
        // filename comparison string queries.
        auto it = std::find_if(currentLevel.begin(), currentLevel.end(),
                               [&](const SSTFile &x) {
                                 return strcmp(x.filename, fn.c_str()) == 0;
                               });

        if (it != currentLevel.end()) {
          // Global Sizing Reconciliation Loop: Scan the internal file indices
          // before deleting the node to reverse atomic system metrics
          // accurately.
          for (auto &e : it->index) {
            // Deduplication Count Guard: Only decrement active system metrics
            // if the record does not possess a tombstone cancellation marker.
            if (!e.tombstone) {
              size_t currentTotal =
                  __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST);
              if (currentTotal > 0)
                __atomic_sub_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
            }
          }

          // Metadata Eviction Phase: Cleanly erase the cached SSTable
          // descriptor from the active RAM array map.
          currentLevel.erase(it);
          filesToDelete.push_back(fn);
          break;
        }
      }
    }
    xSemaphoreGive(_mutex);
  }

  for (const auto &fn : filesToDelete) {
    // Physical Unlinking Sequence: Trigger a virtual filesystem physical remove
    // request. This systematically frees up empty non-volatile flash or storage
    // card blocks trailing compaction passes.
    STORAGE_REMOVE(fn);
  }
}

void NeuLSMDB::deleteSSTFiles(const std::vector<String> &files) {
  // Routing Forwarder Phase: Delegate execution to the inline utility,
  // passing the dedicated regular storage file pipeline metadata array as the
  // target.
  internalDeleteSST(files, GET_LEVELS());
}

void NeuLSMDB::deleteSSTLogFiles(const std::vector<String> &files) {
  // Routing Forwarder Phase: Delegate execution to the inline utility,
  // passing the dedicated isolated log pipeline metadata array as the target.
  internalDeleteSST(files, GET_LEVELS_LOG());
}

// =================================================================
// PROBABILISTIC FILTERING: HARDWARE-ACCELERATED BLOOM FILTER ENGINE
// =================================================================

uint32_t NeuLSMDB::bloomHash(uint16_t key, uint8_t seed) {
  // PROBABILISTIC INTERACTION: Extract bit-vector offsets via hardware-backed
  // CRC32 hashing matrices
  return (crc32_le(seed, (const uint8_t *)&key, sizeof(key))) %
         (BLOOM_FILTER_SIZE * 8);
}

void NeuLSMDB::bloomAdd(uint8_t *filter, uint16_t key) {
  for (uint8_t i = 0; i < BLOOM_HASH_COUNT; i++) {
    uint32_t h = bloomHash(key, i);
    filter[h / 8] |= (1 << (h % 8));
  }
}

bool NeuLSMDB::bloomCheck(const uint8_t *filter, uint16_t key) {
  for (uint8_t i = 0; i < BLOOM_HASH_COUNT; i++) {
    uint32_t h = bloomHash(key, i);
    if (!(filter[h / 8] & (1 << (h % 8))))
      return false; // Deterministic shortcut: Key is guaranteed to be
                    // non-existent in storage block
  }
  return true; // Key potentially exists: Proceed safely to perform
               // deterministic disk search
}

// =================================================================
// VOLATILE ACCELERATION: DOUBLE-MAPPED LRU BLOCK CACHE SUBSYSTEM
// =================================================================

uint64_t NeuLSMDB::makeCacheKey(uint32_t fileId, uint32_t offset) {
  // BITWISE COMPOUNDING: Construct an absolute 64-bit coordinate space using
  // distinct file and offset blocks
  return (uint64_t)fileId << 32 | offset;
}

void NeuLSMDB::cachePut(uint32_t fileId, uint32_t offset, const uint8_t *data,
                        size_t len) {
  uint64_t k = makeCacheKey(fileId, offset);
  auto &mapC = *GET_CACHE_MAP();
  auto &listC = *GET_CACHE_LIST();

  // DEDUPLICATION PHASE: Purge preexisting target elements to refresh
  // transactional record sequences
  if (mapC.count(k)) {
    _cacheUsed -= mapC[k]->data.size();
    listC.erase(mapC[k]);
    mapC.erase(k);
  }

  // RESOURCE CONSTRAINTS: Proactively drop outdated cache blocks to maintain
  // strict memory bounds
  while (_cacheUsed + len > CACHE_SIZE_BYTES && !listC.empty()) {
    cacheEvict();
  }

  CacheBlock b;
  b.cacheKey = k;
  b.data.assign(data, data + len);
  listC.push_front(b);
  mapC[k] = listC.begin(); // Map registration: Secure absolute O(1) address
                           // resolution path via list iterator
  _cacheUsed += len;
}

bool NeuLSMDB::cacheGet(uint32_t fileId, uint32_t offset,
                        std::vector<uint8_t> &out) {
  uint64_t k = makeCacheKey(fileId, offset);
  auto &mapC = *GET_CACHE_MAP();
  auto &listC = *GET_CACHE_LIST();

  if (!mapC.count(k))
    return false;

  out = mapC[k]->data;

  // CACHE PROMOTION: Shift accessed node to head position via lockless internal
  // pointer splicing
  listC.splice(listC.begin(), listC, mapC[k]);
  return true;
}

void NeuLSMDB::cacheEvict() {
  auto &listC = *GET_CACHE_LIST();
  auto &mapC = *GET_CACHE_MAP();

  if (listC.empty())
    return;

  // LEAST RECENTLY USED PHASE: Extract and drop stale elements from the tail
  // boundary of the tracking vector
  auto it = --listC.end();
  _cacheUsed -= it->data.size();
  mapC.erase(it->cacheKey);
  listC.pop_back();
}

// =================================================================
// REORGANIZATION ENGINE: LSM BACKGROUND COMPACTION SCHEDULER
// =================================================================
bool NeuLSMDB::SourceReader::next() {
  if (!file || eof)
    return false;

  if (file.available()) {
    uint32_t startPos = file.position();

    SSTHeader header;
    // Read 11-bytes directly from disk to RAM
    if (file.read((uint8_t *)&header, sizeof(SSTHeader)) != sizeof(SSTHeader)) {
      eof = true;
      return false;
    }

    current.key = header.key;
    current.size = header.size;
    current.ts = header.ts;
    current.tombstone = (header.tombstone == 1);
    current.offset = startPos;

    valueOffset = file.position();

    // BOUNDARY CHECK & JUMPER: Validate payload size against the physical
    // limits of the external disk
    if (file.position() + current.size > file.size() ||
        !file.seek(current.size, SeekCur)) {
      eof = true;
      return false;
    }

    // ITERATION INCREMENT: Track version changes to sync multi-core compaction
    // states
    version++;
    return true;
  }

  eof = true;
  return false;
}

bool NeuLSMDB::SourceReader::readValue(uint8_t *buf, size_t &outSize) {
  if (!file || eof || !current.size) {
    outSize = 0;
    return true;
  }
  file.seek(valueOffset);
  outSize = file.read(buf, current.size);
  return outSize == current.size;
}

void NeuLSMDB::triggerCompaction(uint8_t level) {
  if (level + 1 >= MAX_LEVEL)
    return;

  // THREAD RESILIENCE: Secure cross-core state using atomic compare-and-swap
  // (CAS) memory fencing
  CompactState exp = IDLE;
  if (!__atomic_compare_exchange_n(&_compactState, &exp, MERGE_STREAM, false,
                                   __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
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
  for (auto &fn : _job->srcFiles) {
    SourceReader r;
    if (r.open(fn)) {
      _job->readers.push_back(std::move(r));
    } else {
      // FILE DESCRIPTOR LEAK PROTECTION: If the target file fails to open
      // (empty/corrupted), force-close its internal handle here to instantly
      // release the file descriptor in the VFS layer.
      r.close();
    }
  }

  // Generate output destination paths
  uint32_t fileId = getFileSeq();
  _job->dstTemp = makeFilename(level + 1, fileId) + ".tmp";
  _job->dstFinal = makeFilename(level + 1, fileId);
}

void NeuLSMDB::tickCompact() {
  CompactState state = __atomic_load_n(&_compactState, __ATOMIC_SEQ_CST);
  if (state != MERGE_STREAM)
    return;

  if (!_compactInitialized) {
    while (!_compactHeap.empty())
      _compactHeap.pop();

    for (size_t i = 0; i < _job->readers.size(); i++) {
      auto &r = _job->readers[i];
      if (!r.eof)
        _compactHeap.push(
            {r.current.key, r.current.ts, i, r.current.offset, r.version});
    }
    _compactInitialized = true;
  }

  File out = STORAGE_OPEN(_job->dstTemp, "a");
  if (!out) {
    __atomic_store_n(&_compactState, IDLE, __ATOMIC_SEQ_CST);
    _compactInitialized = false;
    _job->active = false;
    return;
  }

  size_t budget = COMPACT_BUDGET_KB * 1024;
  size_t written = 0;

  while (!_compactHeap.empty() && written < budget) {
    HeapEntry top;
    bool found = false;
    while (!_compactHeap.empty()) {
      top = _compactHeap.top();
      _compactHeap.pop();
      auto &r = _job->readers[top.readerIdx];
      if (r.version == top.version && r.current.offset == top.offset) {
        found = true;
        break;
      }
    }
    if (!found)
      continue;

    uint16_t key = top.key;

    std::vector<size_t> group;
    group.reserve(4);
    for (size_t i = 0; i < _job->readers.size(); i++) {
      auto &r = _job->readers[i];
      if (!r.eof && r.current.key == key)
        group.push_back(i);
    }
    if (group.empty())
      continue;

    uint32_t bestTs = 0;
    size_t winnerIdx = group[0];
    bool first = true;
    for (size_t idx : group) {
      auto &r = _job->readers[idx];
      if (first || r.current.ts > bestTs) {
        bestTs = r.current.ts;
        winnerIdx = idx;
        first = false;
      }
    }

    auto &winner = _job->readers[winnerIdx];

    if (winner.current.size > 0) {
      if (_compactValBuf.size() < winner.current.size)
        _compactValBuf.resize(winner.current.size);

      size_t actual;
      if (!winner.readValue(_compactValBuf.data(), actual)) {
        for (size_t idx : group) {
          auto &r = _job->readers[idx];
          r.next();
          if (!r.eof)
            _compactHeap.push({r.current.key, r.current.ts, idx,
                               r.current.offset, r.version});
        }
        continue;
      }
    }

    // =================================================================
    // SERIALIZATION USING A SINGLE 32-BIT SSTHEADER
    // =================================================================
    SSTHeader header;
    header.key = key;
    header.size = winner.current.size;
    header.ts = bestTs;
    header.tombstone = winner.current.tombstone ? 1 : 0;

    // Write metadata 11-bytes at a time to a temporary file
    out.write((const uint8_t *)&header, sizeof(SSTHeader));

    if (header.size > 0)
      out.write(_compactValBuf.data(), header.size);

    written += sizeof(SSTHeader) + header.size;

    for (size_t idx : group) {
      auto &r = _job->readers[idx];
      r.next();
      if (!r.eof)
        _compactHeap.push(
            {r.current.key, r.current.ts, idx, r.current.offset, r.version});
    }
  }

  out.close();

  bool allDone = true;
  for (auto &r : _job->readers) {
    if (!r.eof) {
      allDone = false;
      break;
    }
  }

  // ==================== COMPACTION FINALIZATION ====================
  if (allDone) {
    for (auto &r : _job->readers)
      r.close();

    _job->readers.clear();

    if (STORAGE_RENAME(_job->dstTemp, _job->dstFinal)) {
      File f = STORAGE_OPEN(_job->dstFinal, "r");
      if (f) {
        std::vector<SSTIndex> idx;
        SSTFile sst;
        memset(sst.bloom, 0, sizeof(sst.bloom));

        // SYNC FILE ID FROM dstFinal
        int lastUnderscore = _job->dstFinal.lastIndexOf('_');
        int lastDot = _job->dstFinal.lastIndexOf('.');
        if (lastUnderscore != -1 && lastDot != -1 && lastDot > lastUnderscore) {
          sst.fileId =
              _job->dstFinal.substring(lastUnderscore + 1, lastDot).toInt();
        } else {
          sst.fileId = getFileSeq();
        }
        strncpy(sst.filename, _job->dstFinal.c_str(), sizeof(sst.filename) - 1);
        sst.filename[sizeof(sst.filename) - 1] = '\0';

        while (f.available()) {
          uint32_t currentEntryOffset = f.position();

          // READING BACK THE SST INDEX USING SSTHEADER
          SSTHeader readHeader;
          if (f.read((uint8_t *)&readHeader, sizeof(SSTHeader)) !=
              sizeof(SSTHeader))
            break;

          SSTIndex entry;
          entry.key = readHeader.key;
          entry.offset = currentEntryOffset;
          entry.size = readHeader.size;
          entry.ts = readHeader.ts;
          entry.tombstone = (readHeader.tombstone == 1);

          bloomAdd(sst.bloom, entry.key);

          if (entry.size > 0) {
            f.seek(entry.size, SeekCur);
          }

          idx.push_back(entry);

          // DO NOT INCREASE _totalEntryCount RANDOMLY!
          // During compaction, we write old data that has ALREADY BEEN
          // CALCULATED in memory.
          if (!entry.tombstone)
            __atomic_add_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
        }
        f.close();

        std::sort(idx.begin(), idx.end());
        sst.index = std::move(idx);

        if (xSemaphoreTake(_mutex, portMAX_DELAY) == pdTRUE) {
          GET_LEVELS()[_job->srcLevel + 1].push_back(std::move(sst));
          xSemaphoreGive(_mutex);
        }
      }

      // Deleting physical obsolete files from flash disk
      deleteSSTFiles(_job->srcFiles);
    } else {
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

uint32_t NeuLSMDB::crc32(uint32_t crc, const uint8_t *data, size_t len) {
  return crc32_le(crc, data, len);
}

void NeuLSMDB::tuneMemtable() {
  float heapRatio = (float)ESP.getFreeHeap() / (float)ESP.getHeapSize();
  float writePressure = (float)_memBytes / (float)CACHE_SIZE_BYTES;
  int l0Pressure = GET_LEVELS()[0].size();

  // Compute the system resource pressure score matrix
  float score = (writePressure * 0.5f) + ((1.0f - heapRatio) * 0.3f) +
                ((float)l0Pressure * 0.2f);

  // Dynamic Parameter Adaptation: Scale limits algorithmically to preserve
  // system stability
  if (score < 0.3f)
    _adaptiveLimit =
        8192; // Low pressure: Expand memory threshold for better throughput
  else if (score < 0.6f)
    _adaptiveLimit = 4096; // Moderate pressure: Apply balanced baseline size
  else
    _adaptiveLimit =
        2048; // High pressure: Shrink boundary to enforce aggressive flushing

  // Enforce hard-coded absolute safety baseline limit
  if (_adaptiveLimit < 1024)
    _adaptiveLimit = 1024;
}

void NeuLSMDB::evictOldestData() {
  uint32_t oldestTs = UINT32_MAX;
  String oldestFile;
  size_t oldestIdx = 0;
  uint32_t oldestKey = 0;
  uint8_t oldestLvl = 0;
  bool foundOldest = false;
  bool isLogType = false;

  // Phase 1: Scan Regular Storage Partition (Standard Key Range 0 to 2047)
  // Sweep deep immutable levels in reverse order to find the absolute oldest
  // historical metadata mutation.
  for (int lvl = MAX_LEVEL - 1; lvl >= 0; lvl--) {
    for (auto &sst : GET_LEVELS()[lvl]) {
      for (size_t i = 0; i < sst.index.size(); i++) {
        auto &e = sst.index[i];
        // Compare hardware timestamps to intercept and locate the most obsolete
        // transaction node.
        if (!e.tombstone && e.ts < oldestTs) {
          oldestTs = e.ts;
          oldestFile = sst.filename;
          oldestIdx = i;
          oldestKey = e.key;
          oldestLvl = lvl;
          foundOldest = true;
          isLogType = false;
        }
      }
    }
  }

  // Phase 2: Scan Isolated Log Pipeline Track (Virtual High-Address Space
  // Region) Sweep matching log cascades to cross-evaluate data expiration
  // states across both storage tracks.
  for (int lvl = MAX_LEVEL - 1; lvl >= 0; lvl--) {
    for (auto &sst : GET_LEVELS_LOG()[lvl]) {
      for (size_t i = 0; i < sst.index.size(); i++) {
        auto &e = sst.index[i];
        if (!e.tombstone && e.ts < oldestTs) {
          oldestTs = e.ts;
          oldestFile = sst.filename;
          oldestIdx = i;
          oldestKey = e.key;
          oldestLvl = lvl;
          foundOldest = true;
          isLogType = true;
        }
      }
    }
  }

  // Phase 3: Reactive Eviction Execution (MemTable Tombstone Injection)
  if (foundOldest) {
    MemEntry delEntry;
    delEntry.ts = millis();
    delEntry.tombstone = true;
    delEntry.size = 0;

    // Commit an in-memory deletion marker directly at the target absolute
    // location inside the active tree map. This dynamically balances global
    // counter states before the next scheduled flush routine.
    (*GET_MEM())[oldestKey] = std::move(delEntry);
    __atomic_sub_fetch(&_totalEntryCount, 1, __ATOMIC_SEQ_CST);
  } else {
    // Emergency Fallback Path: Hard file unlinking if volatile RAM metadata
    // becomes blank under critical storage stress. Direct non-volatile
    // unlinking prevents physical disk saturation when memory lookup limits
    // collapse.
    if (!isLogType && !GET_LEVELS()[0].empty()) {
      String firstFile = GET_LEVELS()[0][0].filename;
      deleteSSTFiles({firstFile});
    } else if (isLogType && !GET_LEVELS_LOG()[0].empty()) {
      String firstFile = GET_LEVELS_LOG()[0][0].filename;
      deleteSSTLogFiles({firstFile});
    }
  }
}

void NeuLSMDB::auditLevels() {
  // === ACQUIRE DATABASE LOCK FOR AUDIT ===
  if (xSemaphoreTake(_mutex, pdMS_TO_TICKS(1000)) != pdTRUE) {
    Serial.println(
        F("[ERROR] Failed to acquire database lock for audit operation!"));
    return;
  }

  Serial.println(F("\n>>> === NEUDB LSM-TREE TOPOLOGY AUDIT === <<<"));

  // METRICS RECONCILIATION: Fetch total live database counters using atomic
  // sequential consistency
  size_t totalNow = __atomic_load_n(&_totalEntryCount, __ATOMIC_SEQ_CST);

  Serial.printf("Active Records (Global Counter): %d | Capacity Limit: %d | "
                "Eviction Policy: %s\n",
                totalNow, MAX_TOTAL_ENTRIES,
                _overrideWhenFull ? "OVERRIDE" : "REJECT");

  // ========================================================================
  // PARTITION 1: AUDIT JALUR DATA REGULER (GET_LEVELS)
  // ========================================================================
  Serial.println(F("\n--- [ REGULAR DATA STORAGE PIPELINE ] ---"));
  for (int lvl = 0; lvl < MAX_LEVEL; lvl++) {
    int fileCount = GET_LEVELS()[lvl].size();
    int totalEntries = 0;
    int tombCount = 0;
    size_t totalSize = 0;

    Serial.printf("  [ LEVEL %d ] -> Active Files: %d\n", lvl, fileCount);

    for (auto &sst : GET_LEVELS()[lvl]) {
      totalEntries += sst.index.size();

      for (auto &e : sst.index)
        if (e.tombstone)
          tombCount++;

      File f = STORAGE_OPEN(sst.filename, "r");
      size_t fileSize = f ? f.size() : 0;
      if (f)
        f.close();
      totalSize += fileSize;

      Serial.printf(
          "    -> %s | Entries: %d | Footprint: %d B | Bloom Filter: ACTIVE\n",
          sst.filename, sst.index.size(), fileSize);
    }

    Serial.printf("    > Summary Level %d (Regular): Total Entries=%d | "
                  "Tombstones=%d | Total Size: %d B\n",
                  lvl, totalEntries, tombCount, totalSize);
  }

  // ========================================================================
  // PARTITION 2: REUSE INTEGRITY - AUDIT JALUR DATA LOG (GET_LEVELS_LOG)
  // ========================================================================
  Serial.println(F("\n--- [ AUTOMATIC LOG SNAPSHOT PIPELINE ] ---"));
  auto levelsLogPtr = GET_LEVELS_LOG();

  if (levelsLogPtr != nullptr) {
    for (int lvl = 0; lvl < MAX_LEVEL; lvl++) {
      int fileCount = levelsLogPtr[lvl].size();
      int totalEntries = 0;
      int tombCount = 0;
      size_t totalSize = 0;

      Serial.printf("  [ LEVEL %d ] -> Active Files: %d\n", lvl, fileCount);

      for (auto &sst : levelsLogPtr[lvl]) {
        totalEntries += sst.index.size();

        for (auto &e : sst.index)
          if (e.tombstone)
            tombCount++;

        File f = STORAGE_OPEN(sst.filename, "r");
        size_t fileSize = f ? f.size() : 0;
        if (f)
          f.close();
        totalSize += fileSize;

        Serial.printf("    -> %s | Entries: %d | Footprint: %d B | Bloom "
                      "Filter: ACTIVE\n",
                      sst.filename, sst.index.size(), fileSize);
      }

      Serial.printf("    > Summary Level %d (Log Pipeline): Total Entries=%d | "
                    "Tombstones=%d | Total Size: %d B\n",
                    lvl, totalEntries, tombCount, totalSize);
    }
  }

  Serial.println(F("\n>>> === END OF TOPOLOGY AUDIT === <<<\n"));

  // === RELEASE DATABASE LOCK ===
  xSemaphoreGive(_mutex);
}
