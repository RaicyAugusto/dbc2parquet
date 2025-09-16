/*****************************************************************************
 * @file dbf_reader.cpp
 * @brief Provides in-memory DBF reading functionality.
 *
 * This file contains functions for reading and processing DBF (dBase) files
 * in memory. It includes functionality for decompressing DBC files, reading
 * header and field information, and retrieving field values.
 *
 * @note This implementation is adapted from libdbf (dbf.c and dbf_endian.c)
 * @original-author Björn Berg <clergyman@gmx.de>
 * @original-license Permission granted with copyright notice
 * @original-source dbf.berlios.de (discontinued site)
 *
 * @author Raicy Augusto
 * @version 1.0
 * @date September 2025
 *
 * @copyright Copyright (C) 2025 Raicy Augusto
 *
 * This file is part of the dbc2parquet project.
 * Additional code licensed under the Apache License, Version 2.0.
 * See LICENSE file for details.
 *
 * @note This implementation provides enhanced functionality for in-memory
 * DBF reading, optimized for modern C++ practices and integration with
 * the dbc2parquet converter project.
 ****************************************************************************/

#include <cstdint>
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include "dbf_reader.hpp"
extern "C" {
    #include "blast.h"
}


/**
 * @brief Swaps the bytes of a 16-bit integer.
 *
 * @param var The 16-bit integer to swap.
 * @return uint16_t The byte-swapped 16-bit integer.
 */
static uint16_t rotate2b(const uint16_t var) {
    uint16_t tmp = var;
    const unsigned char *ptmp = reinterpret_cast<unsigned char *>(&tmp);
    return (static_cast<uint16_t>(ptmp[1]) << 8) + static_cast<uint16_t>(ptmp[0]);
}

/**
 * @brief Swaps the bytes of a 32-bit integer.
 *
 * @param var The 32-bit integer to swap.
 * @return uint32_t The byte-swapped 32-bit integer.
 */
static uint32_t rotate4b(const uint32_t var) {
    uint32_t tmp = var;
    const unsigned char *ptmp = reinterpret_cast<unsigned char *>(&tmp);
    return (static_cast<uint32_t>(ptmp[3]) << 24) +
           (static_cast<uint32_t>(ptmp[2]) << 16) +
           (static_cast<uint32_t>(ptmp[1]) << 8) +
           static_cast<uint32_t>(ptmp[0]);
}


/**
 * @brief Returns the number of columns in the DBF file.
 *
 * @param dbf The DBF file structure.
 * @return unsigned int The number of columns.
 */
unsigned int dbf_NumCols(const DBF& dbf) {
    if ( dbf.header->header_length > sizeof(DB_HEADER)) {
        return (dbf.header->header_length - sizeof(DB_HEADER) -1) / sizeof(DB_FIELD);
    }

    return 0;
}

/**
 * @brief Returns the number of rows in the DBF file.
 *
 * @param dbf The DBF file structure.
 * @return unsigned int The number of rows.
 */
unsigned int dbf_NumRows(const DBF& dbf) {
    return (dbf.header->records> 0) ? dbf.header->records : 0;
}

/**
 * @brief Reads and processes the header information from the DBF file.
 *
 * @param dbf The DBF file structure to populate with header information.
 * @return int 0 on success, -1 on failure.
 */
static int dbf_ReadHeaderInfo(DBF& dbf) {
    if (dbf.mem_buffer.size() < sizeof(DB_HEADER)) return -1;

    auto header_ptr = std::make_unique<DB_HEADER>();
    memcpy(header_ptr.get(), dbf.mem_buffer.data(), sizeof(DB_HEADER));

    header_ptr->header_length = rotate2b(header_ptr->header_length);
    header_ptr->records = rotate4b(header_ptr->records);

    switch (header_ptr->language) {
        case 0x01: dbf.encoding = "CP437"; break;
        case 0x02: dbf.encoding = "CP850"; break;
        case 0x03: dbf.encoding = "CP852"; break;
        case 0x65: dbf.encoding = "CP1252"; break;
        default:   dbf.encoding = "CP850"; break;
    }
    dbf.header = std::move(header_ptr);

    return 0;
}
/**
 * @brief Reads and processes the field information from the DBF file.
 *
 * @param dbf The DBF file structure to populate with field information.
 * @return int 0 on success, -1 on failure.
 */
static int dbf_ReadFieldInfo(DBF& dbf) {
    if (dbf.mem_buffer.size() < sizeof(DB_HEADER)) return -1;

    unsigned int col = dbf_NumCols(dbf);
    if (!col) return -1;

    auto field_ptr = std::make_unique<DB_FIELD[]>(col);
    memcpy(field_ptr.get(),dbf.mem_buffer.data() + sizeof(DB_HEADER), col * sizeof(DB_FIELD));

    int offset = 1;
    for (int i = 0;  i < col;  i++) {
        field_ptr[i].field_offset = offset;
        offset += field_ptr[i].field_length;
    }

    dbf.fields = std::move(field_ptr);
    dbf.columns = col;

    return 0;

}

/**
 * @brief Retrieves the value of a specific field in the DBF file.
 *
 * @param dbf The DBF file structure.
 * @param col The column index of the field.
 * @param row The row index of the record.
 * @return std::string The trimmed value of the field.
 */
std::string dbf_get_field_value(const DBF& dbf, const int col, const int row) {
    const size_t record_offset = dbf.header->header_length + (row * dbf.header->record_length);

    const size_t field_offset = dbf.fields[col].field_offset;

    const auto field_ptr = reinterpret_cast<const char*>(dbf.mem_buffer.data() + record_offset + field_offset);
    const int field_len = dbf.fields[col].field_length;

    std::string value(field_ptr, field_len);

    value.erase(0, value.find_first_not_of(" \t\n\r"));
    value.erase(value.find_last_not_of(" \t\n\r") + 1);

    return value;
}

/**
 * @brief Reads the header size from the DBF file.
 *
 * @param input The input file stream.
 * @return uint16_t The header size, or 1 on error.
 */
static uint16_t dbf_ReadHeaderSize(FILE* input) {
    unsigned char rawHeader[2];

    if(fseek(input, 8, SEEK_SET) != 0) return 1;
    if(fread(rawHeader, 2, 1, input) != 1) return 1;

    return rawHeader[0] + (rawHeader[1] << 8);
}

/**
 * @brief Callback function for reading input from a file.
 *
 * @param how Pointer to the FILE object.
 * @param buf Pointer to the buffer where data will be stored.
 * @return unsigned The number of bytes read.
 */
static unsigned in_from_file(void* how, unsigned char** buf) {
    FILE* file = static_cast<FILE *>(how);

    static unsigned char file_buffer[4096];

    size_t read_bytes = std::fread(file_buffer, 1, sizeof(file_buffer), file);

    *buf = file_buffer;

    return static_cast<unsigned>(read_bytes);
}

/**
 * @brief Callback function for writing output to memory.
 *
 * @param how Pointer to the output vector.
 * @param buf Pointer to the data to be written.
 * @param len Length of the data to be written.
 * @return int Always returns 0.
 */
static int out_to_memory(void* how, unsigned char* buf, const unsigned len) {
    const auto vec = static_cast<std::vector<unsigned char>*>(how);
    vec->insert(vec->end(), buf, buf + len);
    return 0;
}

/**
 * @brief Decompresses the DBF file data.
 *
 * @param input The input file stream.
 * @param header_size The size of the DBF header.
 * @param output_buf The vector to store the decompressed data.
 * @return int 0 on success, -1 on failure.
 */
static int dbf_DecompressData(FILE* input, const uint16_t header_size,  std::vector<unsigned char>& output_buf) {
    if (fseek(input, header_size + 4, SEEK_SET) != 0) return -1;

    int ret = blast(in_from_file, input, out_to_memory, &output_buf);
    if (ret != 0) {
        fprintf(stderr, "blast error: %d\n", ret);
        return -1;
    }

    return 0;
}


/**
 * @brief Loads a DBF file into memory and processes its structure.
 *
 * @param input The input file stream.
 * @param dbf The DBF file structure to populate.
 * @return bool true on success, false on failure.
 */
bool dbc_load_dbf(FILE* input, DBF& dbf) {
    const uint16_t header_size = dbf_ReadHeaderSize(input);
    if (header_size == 1) return false;

    if (fseek(input, 0, SEEK_SET) != 0) return false;
    dbf.mem_buffer.resize(header_size);
    if (fread(dbf.mem_buffer.data(), 1, header_size, input) != header_size) {
        return false;
    }

    if (dbf_DecompressData(input, header_size, dbf.mem_buffer) != 0) return false;
    if (dbf_ReadHeaderInfo(dbf) != 0) return false;
    if (dbf_ReadFieldInfo(dbf) != 0) return false;

    return true;
}




