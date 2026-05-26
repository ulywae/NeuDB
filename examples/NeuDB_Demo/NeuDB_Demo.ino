/**
 * @file NeuDB_Demo.ino
 * @author Ulywae (2026)
 * @brief Official Demonstration for the NeuDB LSM-Tree Embedded Storage Engine.
 *
 * @details
 * This example demonstrates basic CRUD operations using type-deducting template helpers,
 * dynamic String serialization, crash recovery characteristics, and system topology monitoring.
 */

#include <NeuDB.h>

// Fast bitwise mask for optimized key ranges (0-63), bypassing slow modulo instructions
#define KEY_MASK_64 0x3F

// Struct packed ensures dense storage alignment without structural memory padding bytes
struct __attribute__((packed)) SensorPayload
{
    uint32_t timestamp;
    float temperature;
    uint16_t dynamicStatus;
};

void setup()
{
    Serial.begin(115200);
    delay(3000); // Allow hardware serial bridge port to stabilize

    Serial.println(F("\n====================================================="));
    Serial.println(F("           NEUDB ECOSYSTEM DEMO BOOTSTRAP            "));
    Serial.println(F("====================================================="));

    // 1. Initialize the storage engine subsystem (automatically mounts LittleFS & replays WAL if dirty)
    if (db.begin())
    {
        Serial.println(F("[INFO] Storage engine successfully mounted and operational."));
    }
    else
    {
        Serial.println(F("[CRITICAL] Storage subsystem initialization FAILED! Halting execution."));
        while (1)
            delay(1000);
    }

    Serial.println(F("\n--- Step 1: Performing Template Variable Writes (putVar) ---"));
    uint32_t initialCounter = 555555;
    uint8_t keyCounter = 10;

    // Write primitive variable directly without using pointers or explicit sizeof handlers
    db.putVar(keyCounter, initialCounter);
    Serial.println(F("[OK] Successfully ingested uint32_t record into volatile MemTable layer."));

    Serial.println(F("\n--- Step 2: Retrieving Template Variables (getVar) ---"));
    uint32_t recoveredCounter = 0;
    if (db.getVar(keyCounter, recoveredCounter))
    {
        Serial.printf("[SUCCESS] Variable retrieved. Value: %u\n", recoveredCounter);
    }

    Serial.println(F("\n--- Step 3: Serializing Dense Packed Structs ---"));
    uint8_t keyStruct = 24;
    SensorPayload originalNode = {millis(), 27.65f, 0xAA55};

    // Template helper handles struct boundary validation implicitly
    db.putVar(keyStruct, originalNode);
    Serial.println(F("[OK] Packed sensor telemetry structural payload committed."));

    Serial.println(F("\n--- Step 4: Extracting Structural Payloads ---"));
    SensorPayload bufferedNode;
    if (db.getVar(keyStruct, bufferedNode))
    {
        Serial.printf("[SUCCESS] Struct unpacked -> Time: %u ms | Temp: %.2f C | Status: 0x%X\n",
                      bufferedNode.timestamp, bufferedNode.temperature, bufferedNode.dynamicStatus);
    }

    Serial.println(F("\n--- Step 5: High-Speed Native String Support ---"));
    uint8_t keyString = 50;
    db.putString(keyString, "NeuDB Industrial Micro-Framework Mode PC Badas! 😎🔥");

    String messageOut = db.getString(keyString);
    Serial.printf("[SUCCESS] String string payload recovered: \"%s\"\n", messageOut.c_str());

    Serial.println(F("\n--- Step 6: Visualizing Active LSM-Tree Topology ---"));
    // Executes a real-time topology layout audit showing file metrics and active tier allocations
    db.auditLevels();

    Serial.println(F("\n>>> Setup phase transactions completed. Entering loop routine..."));
    Serial.println(F(">>> Continuous background streaming metrics will trace below."));
    Serial.println(F("=====================================================\n"));
}

void loop()
{
    static uint32_t cycleTracker = 0;
    cycleTracker++;

    // Calculate rapid bitwise key range indexes (0-63) to avoid blocking runtime cycles
    uint8_t operationalKey = (uint8_t)(cycleTracker & KEY_MASK_64);

    // Continuous simulated real-time asynchronous background ingestion streaming
    SensorPayload telemetryNode = {millis(), 25.0f + ((float)operationalKey * 0.1f), (uint16_t)cycleTracker};
    db.putVar(operationalKey, telemetryNode);

    // Periodic system audit reporting every 128 background transaction loops
    if ((cycleTracker & 0x7F) == 0)
    {
        Serial.printf("[LOOP] Aggregated ingestion cycle checkpoints reached: %u active entries.\n", cycleTracker);
        db.auditLevels();
    }

    // Yield control back to the FreeRTOS thread slicing engine to handle async compaction loops
    vTaskDelay(pdMS_TO_TICKS(5));
}
