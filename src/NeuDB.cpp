#include "NeuDB.h"
#include "NeuLSMDB_FS.h" // Hanya di-include di sini agar terisolasi

NeuDB::NeuDB()
{
    // Alokasikan mesin database asli Anda ke memori heap
    _engine = static_cast<void *>(new NeuLSMDB_FS());
}

NeuDB::~NeuDB()
{
    // Bersihkan memori mesin saat sistem dimatikan
    delete static_cast<NeuLSMDB_FS *>(_engine);
}

bool NeuDB::begin()
{
    return static_cast<NeuLSMDB_FS *>(_engine)->begin();
}

bool NeuDB::put(uint8_t key, const void *data, size_t size)
{
    return static_cast<NeuLSMDB_FS *>(_engine)->put(key, data, size);
}

bool NeuDB::get(uint8_t key, void *out, size_t &size)
{
    return static_cast<NeuLSMDB_FS *>(_engine)->get(key, out, size);
}

void NeuDB::flush()
{
    static_cast<NeuLSMDB_FS *>(_engine)->flush();
}

void NeuDB::auditLevels()
{
    static_cast<NeuLSMDB_FS *>(_engine)->auditLevels();
}

bool NeuDB::format()
{
    return static_cast<NeuLSMDB_FS *>(_engine)->format();
}

void NeuDB::setOverrideWhenFull(bool enable)
{
    static_cast<NeuLSMDB_FS *>(_engine)->setOverrideWhenFull(enable);
}

bool NeuDB::getOverrideWhenFull() const
{
    return static_cast<NeuLSMDB_FS *>(_engine)->getOverrideWhenFull();
}

bool NeuDB::putString(uint8_t key, const String &str)
{
    // Tulis panjang string + karakter datanya sekaligus
    return this->put(key, str.c_str(), str.length() + 1);
}

String NeuDB::getString(uint8_t key)
{
    char buffer[128]; // Sesuaikan batas maksimum panjang string yang aman di stack
    size_t size = sizeof(buffer);
    memset(buffer, 0, size);

    if (this->get(key, buffer, size))
    {
        return String(buffer);
    }
    return String(""); // Kembalikan string kosong jika key tidak ditemukan
}

// INSTANCE GLOBAL UNTUK USER
NeuDB db;
