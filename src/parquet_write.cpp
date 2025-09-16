/*****************************************************************************
 * @file parquet_write.cpp
 * @brief Implements functionality to write DBF data to Parquet format.
 *
 * This file contains functions for converting DBF (dBase) file data to
 * Apache Parquet format. It includes functionality for creating Arrow schemas,
 * processing DBF records, and writing data to Parquet files using Apache Arrow.
 *
 * Key features:
 * - Creation of Arrow schemas based on DBF field definitions
 * - Efficient batch processing of DBF records
 * - Conversion of various DBF data types to Arrow-compatible formats
 * - UTF-8 encoding conversion for text fields
 * - Optimized Parquet file writing with ZSTD compression
 *
 * @author Raicy Augusto
 * @version 1.0
 * @date September 2025
 *
 * @copyright Copyright (C) 2025 Raicy Augusto
 *
 * This file is part of the dbc2parquet project.
 * Licensed under the Apache License, Version 2.0.
 * See LICENSE file for details.
 *
 * @note This implementation leverages Apache Arrow and Parquet libraries
 * for efficient data processing and storage. It is designed to handle
 * large DBF files with optimal performance.
 ****************************************************************************/

#include <ctime>
#include <string>
#include <vector>
#include <memory>
#include <arrow/api.h>
#include "dbf_reader.hpp"
#include <arrow/io/file.h>
#include <arrow/util/macros.h>
#include "parquet_write.hpp"
#include <parquet/arrow/writer.h>
#include "libs/fast_float/fast_float.h"

// Platform-specific includes for character encoding conversion
#ifdef _WIN32
#include <windows.h>
#else
#include <iconv.h>
#include <charconv>
#endif


/**
 * @brief Creates an Arrow schema based on the DBF file structure.
 *
 * @param dbf The DBF file structure.
 * @return std::shared_ptr<arrow::Schema> The created Arrow schema.
 */
static std::shared_ptr<arrow::Schema> create_schema(const DBF &dbf) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    const unsigned int cols = dbf_NumCols(dbf);

    for (unsigned int i = 0; i < cols; i++) {
        const auto field_name = std::string(reinterpret_cast<const char*>(dbf.fields[i].field_name));

        switch (dbf.fields[i].field_type) {
            case 'C': fields.push_back(arrow::field(field_name, arrow::utf8())); break;
            case 'N':
                if (dbf.fields[i].field_decimals > 0) {
                    fields.push_back(arrow::field(field_name, arrow::float64()));
                } else {
                    if (dbf.fields[i].field_length <= 9) {
                        fields.push_back(arrow::field(field_name, arrow::int32()));
                    } else {
                        fields.push_back(arrow::field(field_name, arrow::int64()));
                    }
                }
                break;
            case 'D': fields.push_back(arrow::field(field_name, arrow::date32())); break;
            case 'L': fields.push_back(arrow::field(field_name, arrow::boolean())); break;
            default: fields.push_back(arrow::field(field_name, arrow::utf8())); break;
        }
    }
    return arrow::schema(fields);
}


/**
 * @brief Checks if a string contains only ASCII characters.
 *
 * @param str Pointer to the string to check.
 * @param len Length of the string.
 * @return true If the string contains only ASCII characters.
 * @return false If the string contains non-ASCII characters.
 */
inline bool is_ascii(const char* str, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        if (static_cast<unsigned char>(str[i]) > 127) {
            return false;
        }
    }
    return true;
}

#ifdef _WIN32
// --- Windows-specific encoding conversion ---

inline UINT get_windows_codepage(const std::string& encoding) {
    if (encoding == "CP850" || encoding == "cp850") return 850;
    if (encoding == "CP852" || encoding == "cp852") return 852;
    if (encoding == "CP1252" || encoding == "cp1252") return 1252;
    if (encoding == "CP437" || encoding == "cp437") return 437;
    if (encoding == "ISO-8859-1" || encoding == "iso-8859-1") return 28591;
    if (encoding == "ISO-8859-15" || encoding == "iso-8859-15") return 28605;
    return 1252; // Default fallback
}

/**
 * @brief Converts a string to UTF-8 encoding using Windows API.
 *
 * @param input Pointer to the input string.
 * @param in_len Length of the input string.
 * @param codepage The Windows codepage identifier.
 * @param buffer A vector to store the converted string.
 * @return std::string_view The converted UTF-8 string.
 */
inline std::string_view convert_to_utf8_opt(const char* input, const size_t in_len, const UINT codepage, std::vector<char>& buffer) {
    if (in_len == 0) {
        return {};
    }

    int wide_len = MultiByteToWideChar(codepage, 0, input, static_cast<int>(in_len), nullptr, 0);
    if (wide_len == 0) return {};

    std::vector<wchar_t> wide_buffer(wide_len);
    if (MultiByteToWideChar(codepage, 0, input, static_cast<int>(in_len), wide_buffer.data(), wide_len) == 0) return {};

    int utf8_len = WideCharToMultiByte(CP_UTF8, 0, wide_buffer.data(), wide_len, nullptr, 0, nullptr, nullptr);
    if (utf8_len == 0) return {};

    if (buffer.size() < static_cast<size_t>(utf8_len)) {
        buffer.resize(utf8_len);
    }

    if (WideCharToMultiByte(CP_UTF8, 0, wide_buffer.data(), wide_len, buffer.data(), utf8_len, nullptr, nullptr) == 0) return {};

    return { buffer.data(), static_cast<size_t>(utf8_len) };
}

#else
// --- POSIX-specific encoding conversion (Linux/macOS) ---


/**
 * @brief Converts a string to UTF-8 encoding using iconv.
 *
 * @param input Pointer to the input string.
 * @param in_len Length of the input string.
 * @param cd The iconv conversion descriptor.
 * @param buffer A vector to store the converted string.
 * @return std::string_view The converted UTF-8 string.
 */
inline std::string_view convert_to_utf8_opt(const char* input, const size_t in_len, const iconv_t cd, std::vector<char>& buffer) {
    if (in_len == 0) {
        return {};
    }

    if (buffer.size() < in_len * 4) { // Reserve enough space for worst-case UTF-8 conversion
        buffer.resize(in_len * 4);
    }

    char* in_ptr = const_cast<char*>(input);
    size_t in_bytes_left = in_len;
    char* out_ptr = buffer.data();
    size_t out_bytes_left = buffer.size();

    iconv(cd, nullptr, nullptr, nullptr, nullptr); // Reset iconv state

    if (iconv(cd, &in_ptr, &in_bytes_left, &out_ptr, &out_bytes_left) == static_cast<size_t>(-1)) {
        return {}; // Conversion error
    }

    return {buffer.data(), static_cast<size_t>(out_ptr - buffer.data())};
}
#endif

/**
 * @brief Trims whitespace from the beginning and end of a string in-place.
 *
 * @param start Pointer to the start of the string.
 * @param len Length of the string, will be updated after trimming.
 * @return char* Pointer to the trimmed string.
 */
inline char* trim_in_place(char* start, size_t& len) {
    if (len == 0) return start;

    char* end = start + len - 1;

    while (start <= end && isspace(static_cast<unsigned char>(*start))) {
        start++;
        len--;
    }
    while (end > start && isspace(static_cast<unsigned char>(*end))) {
        end--;
        len--;
    }

    *(end + 1) = '\0';

    return start;
}


/**
 * @brief Creates an Arrow RecordBatch from a subset of DBF records.
 *
 * @param dbf The DBF file structure.
 * @param schema The Arrow schema to use.
 * @param start_row The starting row index in the DBF file.
 * @param num_rows The number of rows to include in the batch.
 * @return std::shared_ptr<arrow::RecordBatch> The created Arrow RecordBatch.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> create_arrow_batch(DBF& dbf, const std::shared_ptr<arrow::Schema>& schema, const int start_row, const int num_rows) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    const unsigned int actual_rows = (start_row + num_rows > dbf.header->records) ? dbf.header->records - start_row : num_rows;

    std::vector<unsigned char*> record_pointers(actual_rows);
    for (int i = 0; i < actual_rows; ++i) {
        record_pointers[i] = dbf.mem_buffer.data() + dbf.header->header_length + ((start_row + i) * dbf.header->record_length);
    }

    std::vector<char> conv_buffer(1024);

    // Platform-specific setup for encoding conversion
#ifdef _WIN32
    UINT codepage = get_windows_codepage(dbf.encoding);
#else
    iconv_t conv_desc = iconv_open("UTF-8", dbf.encoding.c_str());
    if (conv_desc == (iconv_t)-1) {
        return arrow::Status::Invalid("Failed to initialize encoding conversion (iconv_open).");
    }
#endif

    for (int col = 0; col < schema->num_fields(); col++) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), schema->field(col)->type(), &builder));

        const auto& field_def = dbf.fields[col];
        const size_t field_offset = field_def.field_offset;
        const size_t field_length = field_def.field_length;
        auto field_type_id = schema->field(col)->type()->id();

        for (int i = 0; i < actual_rows; i++) {
            char* field_data = reinterpret_cast<char*>(record_pointers[i] + field_offset);

            const char backup_char = field_data[field_length];
            field_data[field_length] = '\0';

            size_t current_len = field_length;
            char* trimmed_data = trim_in_place(field_data, current_len);

            if (*trimmed_data == '\0') {
                ARROW_RETURN_NOT_OK(builder->AppendNull());
            } else {
                switch (field_type_id) {
                    case arrow::Type::STRING: {
                        if (is_ascii(trimmed_data, current_len)) {
                            ARROW_RETURN_NOT_OK(static_cast<arrow::StringBuilder*>(builder.get())->Append(trimmed_data, current_len));
                        } else {
#ifdef _WIN32
                            std::string_view utf8_view = convert_to_utf8_opt(trimmed_data, current_len, codepage, conv_buffer);
#else
                            std::string_view utf8_view = convert_to_utf8_opt(trimmed_data, current_len, conv_desc, conv_buffer);
#endif
                            ARROW_RETURN_NOT_OK(static_cast<arrow::StringBuilder*>(builder.get())->Append(utf8_view));
                        }
                        break;
                    }
                    case arrow::Type::INT32: {
                        int32_t value;
                        if (std::from_chars(trimmed_data, trimmed_data + current_len, value).ec == std::errc())
                            ARROW_RETURN_NOT_OK(static_cast<arrow::Int32Builder*>(builder.get())->Append(value));
                        else ARROW_RETURN_NOT_OK(builder->AppendNull());
                        break;
                    }
                    case arrow::Type::INT64: {
                        int64_t value;
                        if (std::from_chars(trimmed_data, trimmed_data + current_len, value).ec == std::errc())
                            ARROW_RETURN_NOT_OK(static_cast<arrow::Int64Builder*>(builder.get())->Append(value));
                        else ARROW_RETURN_NOT_OK(builder->AppendNull());
                        break;
                    }
                    case arrow::Type::DOUBLE: {
                        double value;
                        if (fast_float::from_chars(trimmed_data, trimmed_data + current_len, value).ec == std::errc())
                            ARROW_RETURN_NOT_OK(static_cast<arrow::DoubleBuilder*>(builder.get())->Append(value));
                        else ARROW_RETURN_NOT_OK(builder->AppendNull());
                        break;
                    }
                    case arrow::Type::BOOL: {
                        bool value = (current_len == 1 && (*trimmed_data == 'T' || *trimmed_data == 't' || *trimmed_data == '1' || *trimmed_data == 'Y' || *trimmed_data == 'y'));
                        ARROW_RETURN_NOT_OK(static_cast<arrow::BooleanBuilder*>(builder.get())->Append(value));
                        break;
                    }
                    case arrow::Type::DATE32: {
                        int year, month, day;
                        if (sscanf(trimmed_data, "%4d%2d%2d", &year, &month, &day) == 3) {
                            struct tm tm = {0};
                            tm.tm_year = year - 1900;
                            tm.tm_mon = month - 1;
                            tm.tm_mday = day;
                            const time_t epoch_seconds = mktime(&tm);
                            const auto days_since_epoch = static_cast<int32_t>(epoch_seconds / (24 * 60 * 60));
                            ARROW_RETURN_NOT_OK(static_cast<arrow::Date32Builder*>(builder.get())->Append(days_since_epoch));
                        } else {
                            ARROW_RETURN_NOT_OK(builder->AppendNull());
                        }
                        break;
                    }
                    default:
                        ARROW_RETURN_NOT_OK(builder->AppendNull());
                        break;
                }
            }
            field_data[field_length] = backup_char;
        }

        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder->Finish(&array));
        columns.push_back(array);
    }

#ifndef _WIN32
    iconv_close(conv_desc);
#endif

    return arrow::RecordBatch::Make(schema, actual_rows, columns);
}



/**
 * @brief Writes the contents of a DBF file to a Parquet file.
 *
 * @param dbf The DBF file structure.
 * @param path The output path for the Parquet file.
 * @param batch_size The number of rows to process in each batch.
 * @return true If the Parquet file was successfully written.
 * @return false If an error occurred during the writing process.
 */
arrow::Status write_parquet(DBF& dbf, const std::string& path, const int batch_size) {
    auto schema = create_schema(dbf);
    if (!schema) {
        return arrow::Status::Invalid("Schema creation failed.");
    }

    ARROW_ASSIGN_OR_RAISE(const auto outfile, arrow::io::FileOutputStream::Open(path));

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    auto writer_properties = props_builder.build();

    std::shared_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outfile, writer_properties));

    for (int start = 0; start < dbf.header->records; start += batch_size) {
        ARROW_ASSIGN_OR_RAISE(auto record_batch, create_arrow_batch(dbf, schema, start, batch_size));
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
    }

    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(outfile->Close());

    return arrow::Status::OK();
}
