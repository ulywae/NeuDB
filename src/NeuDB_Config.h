/**
 * @file NeuDB_Config.h
 * @brief Adaptive Compile-Time Configuration Profiles for the NeuLSMDB_FS Engine.
 * @version 1.2.1
 * @date 2026
 * @author ulywae / NeuDB Core Team
 *
 * @copyright Copyright (c) 2026. Licensed under the MIT License.
 *
 * This configuration header automatically orchestrates the structural tuning parameters,
 * memory budgets, and peripheral pin mappings of the underlying LSM-Tree database engine.
 * Toggle between internal Flash (LittleFS) or external MicroSD high-capacity logging profiles
 * seamlessly via static preprocessor macro selectors.
 *
 * ==================================================================================
 *  ⚠️ CRITICAL CRASH & WARRANTY VOID WARNING / SYNC INTEGRITY LIABILITY NOTICE ⚠️
 * ==================================================================================
 * DO NOT, under any circumstances, manually modify the downstream internal core engine
 * parameters or memory thresholds beyond this configuration file boundary.
 *
 * Altering the operational level depths, atomic register flags, or page buffer allocation
 * matrices without mathematical validation WILL trigger catastrophic virtual filesystem (VFS)
 * deadlock loops, memory fragmentation, or total data corruption.
 *
 * YOU MODIFIED IT? YOU BROKE IT. YOU OWN BOTH PIECES. THE RISK IS ENTIRELY YOURS.
 * ==================================================================================
 */

#ifndef NEU_DB_CONFIG_H
#define NEU_DB_CONFIG_H

// ==================================================================================
// SYSTEM STORAGE TARGET INTERFACE SELECTOR
// ==================================================================================
/**
 * @brief ACTIVE STORAGE TOPOLOGY DRIVER
 * Only uncomment ONE active definition boundary to lock the targeted virtual filesystem.
 */
#define USE_LITTLEFS ///< Mount Built-In Internal Flash Partition Topology (Default)
// #define USE_SDCARD  ///< Mount External MicroSD SPI Hardware Peripheral Bus Layer

// ==================================================================================
// ADAPTIVE HARDWARE COMPILER PROFILES
// ==================================================================================

#if defined(USE_LITTLEFS)
/**
 * @name PROFILE 1: ULTRA-LEAN INTERNAL FLASH CONFIGURATION
 * Optimized for constraint volatile memory footpads running on native flash partitions.
 * @{
 */
#define NEU_MAX_LEVEL 4              ///< Maximum deep hierarchy thresholds of the LSM-Tree (Levels 0 to 3)
#define NEU_KEY_SPACE_LIMIT 2048     ///< Structural ceiling for distinct logical address indexing (IDs 0 to 2047)
#define NEU_MAX_TOTAL_ENTRIES 2048   ///< Absolute physical capacity limit tracking non-tombstone entries on disk
#define NEU_MEMTABLE_MAX_ENTRIES 512 ///< Volatile transaction threshold before forcing an immediate Level 0 SST serialization
#define NEU_SST_TARGET_SIZE 4096     ///< Target byte block metric size constrained per structural physical file (4KB)
#define NEU_CACHE_SIZE_BYTES 1024    ///< Maximum RAM budget dedicated to the Least-Recently-Used (LRU) block cache
#define NEU_COMPACT_BUDGET_KB 16     ///< Max runtime dynamic heap allocated memory allowed during background compaction merges
#define NEU_BLOOM_FILTER_SIZE 64     ///< 64-Byte (512-bit) probabilistic filter matrix size allocating bits per SST file
#define NEU_BLOOM_HASH_COUNT 4       ///< Number of distinct hardware-assisted hash transformations mapped per key entry
/** @} */

#elif defined(USE_SDCARD)
/**
 * @name PROFILE 2: HIGH-CAPACITY EXTERNAL SD CARD DATA-LOGGER
 * Amplified specifications balancing dynamic block clusters and wear-leveling endurance constraints.
 * @{
 */
// SPI Bus Physical Hardware Pin Configurations (Fixed "Kolor Ijo" Pin Allocation)
#define SD_CS 5                       ///< Hardware SPI Chip Select (CS) Active-Low Line Controller GPIO
#define SD_MOSI 23                    ///< Hardware SPI Master Out Slave In Peripheral Data Line GPIO
#define SD_MISO 19                    ///< Hardware SPI Master In Slave Out Peripheral Data Line GPIO
#define SD_SCK 18                     ///< Hardware SPI Serial Clock Synchronizer Signal Line GPIO
#define SD_SPEED 4000000              ///< SPI Transmission Bus clock velocity bound clocked at 4MHz

// Advanced LSM-Tree Architectural Overrides
#define NEU_MAX_LEVEL 5               ///< Expanded tree depth (Levels 0 to 4) to distribute larger file cascades cleanly
#define NEU_KEY_SPACE_LIMIT 2048      ///< Static address limit maintained to prevent volatile index array bloating in RAM
#define NEU_MAX_TOTAL_ENTRIES 32768   ///< Massive capacity unlock allowing up to 32,768 physical log records on disk
#define NEU_MEMTABLE_MAX_ENTRIES 2048 ///< Large memory pipeline aggregation buffer to shield MicroSD from fatal write amplification
#define NEU_SST_TARGET_SIZE 32768     ///< 32KB chunk serialization targeted to maintain optimal FAT/VFS block sector alignment
#define NEU_CACHE_SIZE_BYTES 4096     ///< Enlarged cache boundary window to speed up lookup lookaheads over the SPI link
#define NEU_COMPACT_BUDGET_KB 32      ///< Increased background merge worker buffer capacity allocating cross-level streaming
#define NEU_BLOOM_FILTER_SIZE 128     ///< 128-Byte (1024-bit) dense bitmask array to depress false-positive lookup slips
#define NEU_BLOOM_HASH_COUNT 5        ///< Mathematical hash collision dampener count optimized for 128-byte density matrices
/** @} */
#endif

#endif // NEU_DB_CONFIG_H
