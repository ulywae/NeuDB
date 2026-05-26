#ifndef NEU_DB_H
#define NEU_DB_H

#include <cstdint>
#include <cstddef>
#include <Arduino.h> // Butuh untuk tipe data String bawaan Arduino

class NeuDB
{
public:
    NeuDB();
    ~NeuDB();

    bool begin();
    bool put(uint8_t key, const void *data, size_t size);
    bool get(uint8_t key, void *out, size_t &size);
    void flush();
    void auditLevels();
    bool format();

    void setOverrideWhenFull(bool enable);
    bool getOverrideWhenFull() const;

    // =================================================================
    // TEMPLATE HELPER: Biar nulis di .ino gak pake & dan sizeof lagi!
    // =================================================================
    template <typename T>
    bool putVar(uint8_t key, const T &value)
    {
        return this->put(key, &value, sizeof(T));
    }

    template <typename T>
    bool getVar(uint8_t key, T &out)
    {
        size_t size = sizeof(T);
        return this->get(key, &out, size);
    }

    // Helper Khusus tipe data String bawaan Arduino (Karena String dinamis di heap)
    bool putString(uint8_t key, const String &str);
    String getString(uint8_t key);

private:
    void *_engine;
};

extern NeuDB db;

#endif
