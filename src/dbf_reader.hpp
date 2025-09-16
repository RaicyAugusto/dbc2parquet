/*****************************************************************************
 * dbf_reader.hpp
 *
 * Adapted from libdbf (dbf.h)
 * Original Author: Björn Berg <clergyman@gmx.de>
 * Original license: Permission granted with copyright notice
 * Original source: dbf.berlios.de (discontinued site)
 *
 * Additional code by Raicy Augusto (September 2025, v1.0)
 * Additional code licensed under the Apache License, Version 2.0.
 * See LICENSE file for details.
 *
 * Header file for in-memory DBF reading.
 ****************************************************************************/



#ifndef PROJECT_C_DBF_READER2_H
#define PROJECT_C_DBF_READER2_H

#include <memory>
#include  <vector>
#include <cstdint>

#define CHUNK 4096
#define _(str) (str)

// ================================ DBF STRUCTURES ================================


/*! \struct DB_HEADER
	\brief table file header
	\warning It is recommended not to access DB_HEADER directly.
*/
struct DB_HEADER {
	/*! Byte: 0; dBase version */
	unsigned char version;
	/*! Byte: 1-3; date of last update */
	unsigned char last_update[3];
	/*! Byte: 4-7; number of records in table */
	uint32_t records;
	/*! Byte: 8-9; number of bytes in the header */
	uint16_t header_length;
	/*! Byte: 10-11; number of bytes in the record */
	uint16_t record_length;
	/*! Byte: 12-13; reserved, see specification of dBase databases */
	unsigned char reserved01[2];
	/*! Byte: 14; Flag indicating incomplete transaction */
	unsigned char transaction;
	/*! Byte: 15; Encryption Flag */
	unsigned char encryption;
	/*! Byte: 16-27; reserved for dBASE in a multiuser environment*/
	unsigned char reserved02[12];
	/*! Byte: 28; Production MDX file flag */
	unsigned char mdx;
	/*! Byte: 29; Language driver ID, for Visual FoxPro */
	unsigned char language;
	/*! Byte: 30-31; reserved, filled with zero */
	unsigned char reserved03[2];
};


/*! \struct DB_FIELD
	\brief The field descriptor array
	\warning It is recommend not to access DB_FIELD directly.
*/
struct DB_FIELD {
	/*! Byte: 0-10; fieldname in ASCII */
	unsigned char field_name[11];
	/*! Byte: 11; field type in ASCII (C, D, L, M or N) */
	unsigned char field_type;
	/*! Byte: 12-15; field data adress */
	uint32_t field_address;
	/*! Byte: 16; field length in binary */
	unsigned char field_length;
	/*! Byte: 17; field decimal count in binary */
	unsigned char field_decimals;
	/*! Byte: 18-30; reserved */
	unsigned char reserved1[2];
	uint32_t field_offset;
	unsigned char reserved2[7];
	/*! Byte: 31; Production MDX field flag */
	unsigned char mdx;
};


/*! \struct DBF
	\brief In-memory DBF file handler
	P_DBF store the file in memory
*/
struct DBF {
	/*! buffer in memory */
	std::vector<unsigned char> mem_buffer;
	/*! the pysical size of the file, as stated from filesystem */
	std::unique_ptr<DB_HEADER> header;
	/*! array of field specification */
	std::unique_ptr<DB_FIELD[]> fields;
	/*! number of fields */
	uint32_t columns;
	/*! record counter */
	int cur_record;
    /*! enconding file */
	std::string encoding;
};


// I/O and memory utility functions
bool dbc_load_dbf(FILE* input, DBF& dbf);
unsigned int dbf_NumCols(const DBF& dbf);
unsigned int dbf_NumRows(const DBF& dbf);
std::string dbf_get_field_value(const DBF& dbf, int col, int row);

#endif //PROJECT_C_DBF_READER2_H