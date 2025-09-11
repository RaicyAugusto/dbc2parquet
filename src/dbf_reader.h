/*****************************************************************************
 * dbf_reader.h
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


#ifndef DBF_READER_H
#define DBF_READER_H

#include <stdio.h>
#include <stdint.h>

#define CHUNK 4096
#define _(str) (str)


// ================================ DBF STRUCTURES ================================


/*! \struct MEM_OUTPUT_DESC
   \brief Memory buffer descriptor for decompressed data
   Used to manage dynamically allocated output buffers during decompression
*/
typedef struct MEM_OUTPUT_DESC {
   /*! output data buffer */
   unsigned char* buffer;
   /*! current size of data in buffer */
   size_t size;
   /*! total allocated capacity of buffer */
   size_t capacity;
} MEM_OUTPUT_DESC;


/*! \struct DB_HEADER
	\brief table file header
	\warning It is recommend not to access DB_HEADER directly.
*/
typedef struct DB_HEADER {
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
} DB_HEADER;


/*! \struct DB_FIELD
	\brief The field descriptor array
	\warning It is recommend not to access DB_FIELD directly.
*/
typedef struct DB_FIELD {
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
} DB_FIELD;


/*! \struct P_DBF
	\brief In-memory DBF file handler
	P_DBF store the file in memory
*/
typedef struct P_DBF {
	/*! buffer in memory */
    unsigned char *mem_buffer;
    /*! size buffer in memory*/
    size_t mem_size;
	/*! the pysical size of the file, as stated from filesystem */
	DB_HEADER *header;
	/*! array of field specification */
	DB_FIELD *fields;
	/*! number of fields */
	uint32_t columns;
	/*! record counter */
	int cur_record;
    /*! enconding file */
    char *encoding;
} P_DBF;


// I/O and memory utility functions
P_DBF* dbf_OpenFromMemory(unsigned char* buffer, size_t size);
unsigned char* dbc_DecompressMemory(FILE* input, size_t* out_size);
void dbf_Close(P_DBF *p_dbf);

#endif