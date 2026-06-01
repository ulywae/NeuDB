/**
 * @file NeuDB_Export_Streaming.ino
 * @brief Enterprise Demonstration of Zero-Copy Binary Wire-Streaming Extensions.
 *
 * @details
 * This example demonstrates the formal implementation pathways for extracting packed
 * binary datasets directly from the NeuDB core architecture. It evaluates the execution 
 * parameters of both the regular Key-Value sequence sweep and the multi-version circular 
 * log pipeline lookahead. Data payloads are piped directly onto active system Stream 
 * channels without triggering intermediate dynamic heap allocations.
 */

#include <Arduino.h>
#include "NeuDB.h"

// =============================================================================
// GLOBAL STRUCTURAL TELEMETRY CONFIGURATION
// =============================================================================
#pragma pack(push, 1)
/**
 * @brief High-Density Industrial Telemetry Structure.
 * @details Packed to a strict 1-byte boundary constraint to prevent sloppy alignment
 *          padding and ensure cross-platform serialization integrity.
 */
struct HardwareTelemetry {
    float ambientTemperature; ///< Native floating-point sensor resolution
    uint32_t diagnosticTicks; ///< Monotonic processor event counter state
    uint16_t transactionToken; ///< Cryptographic or error validation indicator flag
};
#pragma pack(pop)

// Global reference identifier for the targeted circular log pipeline segment
const uint16_t SENSOR_LOG_PIPELINE_ID = 7; 

// =============================================================================
// SECTOR FUNCTION PROTOTYPES
// =============================================================================
void populateSimulationDatasets();
void executeSystemBulkExport();

// =============================================================================
// SYSTEM BOOTSTRAP GATEWAY
// =============================================================================
void setup() 
{
    // Initialize primary communication bus lane
    Serial.begin(115200);
    while (!Serial) {
        delay(10); // Halt execution path until the hardware channel stabilizes
    }

    Serial.println(F("\n================================================================="));
    Serial.println(F("    NEUDB — INITIALIZING BULK EXPORT SUBSYSTEM TEST       "));
    Serial.println(F("================================================================="));

    // Initialize the monolithic kernel-inspired storage architecture
    if (!db.begin()) {
        Serial.println(F("[FATAL] Database core bootstrap failed! Halting processor execution layer."));
        while (1) { delay(1000); }
    }
    Serial.println(F("[SUCCESS] Virtual File System mounted. Core storage engine armed securely."));

    // Ingest dummy data to prepare the storage levels for the streaming sweep test
    populateSimulationDatasets();

    // Trigger the official enterprise wire-streaming utilities
    executeSystemBulkExport();
}

void loop() 
{
    // The main thread context remains fully isolated from background compaction tasks
    vTaskDelay(pdMS_TO_TICKS(1000));
}

// =============================================================================
// DATA INGESTION ENGINE SUBSYSTEM (SIMULATION PATHWAY)
// =============================================================================
void populateSimulationDatasets()
{
    Serial.println(F("\n[INJECTION] Bombarding storage matrix with transaction payloads..."));

    // 1. Commit Heap-Allocated Dynamic String components into the Regular Dictionary Space
    db.putString(100, "System_Node_Alpha_Operational_Parameters");
    db.putString(200, "Network_Gateway_Interface_Secure_Token_0x7FFF");
    db.putString(300, "Industrial_Chamber_Pressure_Matrix_Ceiling_Set_942_PSI");

    // 2. Bombard the Circular Log Buffer past the 128-bit boundary ceiling metrics
    HardwareTelemetry frame;
    for (int i = 1; i <= 150; i++) 
    {
        frame.ambientTemperature = 24.5f + ((float)i * 0.15f);
        frame.diagnosticTicks = i * 1000;
        frame.transactionToken = (i >= 130) ? 0xBBBB : 0xAAAA; // Simulate real state transitions
        
        db.putLog(SENSOR_LOG_PIPELINE_ID, &frame, sizeof(HardwareTelemetry));
    }

    // Force immediate volatile serialization to commit MemTable states to permanent disk sectors
    db.flush();
    Serial.println(F("[SUCCESS] Ingestion completed. Storage files committed to LittleFS partitions."));
}

// =============================================================================
// CORE BULK EXPORT AND STREAM SERIALIZATION EXECUTIVE
// =============================================================================
void executeSystemBulkExport()
{
    Serial.println(F("\n================================================================="));
    Serial.println(F("   EXECUTION STEP 1: CONCURRENT DICTIONARY KEY-VALUE SWEEP SCAN  "));
    Serial.println(F("================================================================="));
    Serial.println(F("[ACTION] Invoking exportKeyValuesToStream() on target interface channel..."));
    
    // Divert the raw binary streams out onto the active physical HardwareSerial link
    // Expected binary wire layout packet sequence: [KEY 2B][SIZE 2B][PAYLOAD]
    Serial.print(F("--- START REGULAR DICTIONARY BINARY WIRE STREAM ---\n"));
    
    if (db.exportKeyValuesToStream(&Serial)) {
        Serial.print(F("\n--- END REGULAR DICTIONARY BINARY WIRE STREAM ---\n"));
        Serial.println(F("[SUCCESS] Regular dictionary index tracking sweep completed smoothly."));
    } else {
        Serial.println(F("\n[ERROR] Regular dataset extraction aborted or source partition empty."));
    }

    vTaskDelay(pdMS_TO_TICKS(1000)); // Yield execution block to allow hardware buffers to drain cleanly

    Serial.println(F("\n================================================================="));
    Serial.println(F("   EXECUTION STEP 2: MULTI-VERSION CIRCULAR HISTORY LOG SWEEP     "));
    Serial.println(F("================================================================="));
    Serial.println(F("[ACTION] Invoking exportLogsToStream() on target interface channel..."));

    // Sweep across active log snapshot layers via space-optimized MVCC bitmap guards
    // Expected binary wire layout packet sequence: [LOG_ID 2B][SLOT_INDEX 2B][SIZE 2B][PAYLOAD]
    Serial.print(F("--- START CIRCULAR LOG PIPELINE BINARY WIRE STREAM ---\n"));
    
    if (db.exportLogsToStream(&Serial)) {
        Serial.print(F("\n--- END CIRCULAR LOG PIPELINE BINARY WIRE STREAM ---\n"));
        Serial.println(F("[SUCCESS] Circular history tracking dataset successfully serialized."));
    } else {
        Serial.println(F("\n[ERROR] Circular log extraction aborted or target data miss encountered."));
    }

    Serial.println(F("\n================================================================="));
    Serial.println(F("      ALL EXTENSION UTILITY EXPERIMENTS COMPLETED SECURELY        "));
    Serial.println(F("================================================================="));
}
