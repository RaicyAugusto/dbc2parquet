/*****************************************************************************
 * dbf_reader.c
 *
 * Adapted from libdbf (dbf.c and dbf_endian.c)
 * Original Author: Björn Berg <clergyman@gmx.de>
 * Original license: Permission granted with copyright notice
 * Original source: dbf.berlios.de (discontinued site)
 *
 * Additional code by Raicy Augusto (September 2025, v1.0)
 * Additional code licensed under the Apache License, Version 2.0.
 * See LICENSE file for details.
 *
 * Provides in-memory DBF reading.
 ****************************************************************************/




#include "dbf_reader.h"
#include <glib.h>
#include <string.h>
#include "blast.h"

#define CHUNK 4096
#define _(str) (str)


// ======================= FUNCTION IMPLEMENTATIONS =======================


/* rotate2b()
* swap 4 byte integers
*/
static uint16_t rotate2b(uint16_t var) {
	uint16_t tmp;
	unsigned char *ptmp;
	tmp = var;
	ptmp = (unsigned char *) &tmp;
	return(((uint16_t) ptmp[1] << 8) + (uint16_t) ptmp[0]);
    
}


/* rotate4b()
* swap 4 byte integers
*/
static uint32_t rotate4b(uint32_t var) {
	uint32_t tmp;
	unsigned char *ptmp;
	tmp = var;
	ptmp = (unsigned char *) &tmp;
	return(((uint32_t) ptmp[3] << 24) + ((uint32_t) ptmp[2] << 16) + ((uint32_t) ptmp[1] << 8) + (uint32_t) ptmp[0]);
}

/* dbf_NumCols() 
* Returns the number of fields.
*/
static int dbf_NumCols(P_DBF *p_dbf){
	if ( p_dbf->header->header_length > 0) {
		// TODO: Backlink muss noch eingerechnet werden
		return ((p_dbf->header->header_length - sizeof(DB_HEADER) -1)
					 / sizeof(DB_FIELD));
	} else {
		perror(_("In function dbf_NumCols(): "));
		return -1;
	}

	return EXIT_SUCCESS;
}



/* static dbf_ReadHeaderInfo() 
* Reads header from the in-memory buffer into the struct.
*/
static int dbf_ReadHeaderInfo(P_DBF *p_dbf) {
    if(!p_dbf || !p_dbf->mem_buffer) return -1;

	DB_HEADER *header = (DB_HEADER *)p_dbf->mem_buffer;
      
	header->header_length = rotate2b(header->header_length);
	header->record_length = rotate2b(header->record_length);
	header->records = rotate4b(header->records);

	p_dbf->header = header;

    switch (header->language) {
        case 0x01:
            p_dbf->encoding = g_strdup("CP437");
            break;
        case 0x02:
            p_dbf->encoding = g_strdup("CP850");
            break;
        case 0x03:
            p_dbf->encoding = g_strdup("CP852");
            break;
        case 0x65:
            p_dbf->encoding = g_strdup("CP1252");
            break;
        default:
            p_dbf->encoding = g_strdup("CP850");
            break;
    }

	return EXIT_SUCCESS;
}


/* static dbf_ReadFieldInfo() 
* Sets p_dbf->fields to an array of DB_FIELD containing the specification for all columns.
*/
static int dbf_ReadFieldInfo(P_DBF *p_dbf){

    if(!p_dbf || !p_dbf->mem_buffer) return -1;

	int columns = dbf_NumCols(p_dbf);
    if(columns <= 0) return -1;

	DB_FIELD *fields = malloc(columns * sizeof(DB_FIELD));
    if(!fields) return -1;

    memcpy(fields, p_dbf->mem_buffer + sizeof(DB_HEADER), columns * sizeof(DB_FIELD));
    
	int offset = 1;
	for(int i = 0; i < columns; i++) {
		fields[i].field_offset = offset;
		offset += fields[i].field_length;
	}

    
	p_dbf->fields = fields;
	p_dbf->columns = columns;

	return EXIT_SUCCESS;
}


/* dbf_ReadRecord() 
* Read data from the current record and advances to the next record
*/
static int dbf_ReadRecord(P_DBF *p_dbf, char *record) {

    if(!p_dbf || !p_dbf->mem_buffer) return -1;

	if(p_dbf->cur_record >= p_dbf->header->records)
		return -1;

	if (p_dbf->mem_buffer) {
        size_t offset = p_dbf->header->header_length + p_dbf->cur_record * (p_dbf->header->record_length);
        memcpy(record, p_dbf->mem_buffer + offset, p_dbf->header->record_length);
    } 

	p_dbf->cur_record++;
	return p_dbf->cur_record-1;
}


/* dbf_Close() 
 * Frees all memory allocated to the P_DBF structure.
 */
void dbf_Close(P_DBF *p_dbf) {
    if (p_dbf == NULL) {
        return;
    }

    if(p_dbf->encoding){
        g_free(p_dbf->encoding);
    }

    if (p_dbf->fields) {
        free(p_dbf->fields);
    }

    if (p_dbf->mem_buffer) {
        free(p_dbf->mem_buffer);
    }

    free(p_dbf);
}


/* dbf_NumRows()
* Returns the number of records.
*/
static int dbf_NumRows(P_DBF *p_dbf){
	if ( p_dbf->header->records > 0 ) {
		return p_dbf->header->records;
	} else {
		perror(_("In function dbf_NumRows(): "));
		return -1;
	}

	return EXIT_SUCCESS;
}



/* dbf_SetRecordOffset()
* Sets current record position with offset validation
*/
static int dbf_SetRecordOffset(P_DBF *p_dbf, int offset) {
	if(offset == 0)
		return -3;
	if(offset > (int) p_dbf->header->records)
		return -1;
	if((offset < 0) && (abs(offset) > p_dbf->header->records))
		return -2;
	if(offset < 0)
		p_dbf->cur_record = (int) p_dbf->header->records + offset;
	else
		p_dbf->cur_record = offset-1;
	return p_dbf->cur_record;
}


/* in_from_file()
* Input file helper function
*/
static unsigned in_from_file(void* how, unsigned char** buf) {
    FILE* file = (FILE*)how;

    static unsigned char file_buffer[CHUNK];
    
    size_t read_bytes = fread(file_buffer, 1, sizeof(file_buffer), file);
    
    *buf = file_buffer;
    
    return (unsigned)read_bytes;
}


/* out_to_memory()
* Output memory helper function
*/
static int out_to_memory(void* how, unsigned char* buf, unsigned len) {
    MEM_OUTPUT_DESC* desc = (MEM_OUTPUT_DESC*)how;
    
    if (desc->size + len > desc->capacity) {
        size_t new_capacity = desc->capacity * 2;
        if (new_capacity < desc->size + len) {
            new_capacity = desc->size + len;
        }
        
        
        unsigned char* new_buffer = realloc(desc->buffer, new_capacity);
        if (new_buffer == NULL) {
            return EXIT_FAILURE;
        }
        
        desc->buffer = new_buffer;
        desc->capacity = new_capacity;
    }
    
    memcpy(desc->buffer + desc->size, buf, len);
    desc->size += len;
        
    return EXIT_SUCCESS;
}


/* dbf_OpenFromMemory()
* Open DBF from memory buffer and reads header/field info
*/
P_DBF* dbf_OpenFromMemory(unsigned char* buffer, size_t size) {
    P_DBF *p_dbf = malloc(sizeof(P_DBF));
    if(!p_dbf) return NULL;
    
    p_dbf->mem_buffer = buffer;
    p_dbf->mem_size = size;
    p_dbf->cur_record = 0;
    
    p_dbf->header = NULL;
    if (0 > dbf_ReadHeaderInfo(p_dbf)) {
        free(p_dbf);
        return NULL;
    }

    
    p_dbf->fields = NULL;
    if (0 > dbf_ReadFieldInfo(p_dbf)) {
        free(p_dbf);
        return NULL;
    }
    
    return p_dbf;
}


/*
* dbf_ReadHeaderSize()
* Read DBF header size from file offset
*/
static uint16_t dbf_ReadHeaderSize(FILE* input) {
    unsigned char rawHeader[2];
    
    if (fseek(input, 8, SEEK_SET) != 0) return EXIT_SUCCESS;
    if (fread(rawHeader, 2, 1, input) != 1) return EXIT_SUCCESS;
    
    return rawHeader[0] + (rawHeader[1] << 8);
}


/*
* dbf_ReadHeaderBuffer()
* Read DBF header into allocated buffer
*/
static unsigned char* dbf_ReadHeaderBuffer(FILE* input, uint16_t header_size) {
    if (fseek(input, 0, SEEK_SET) != 0) return NULL;
    if (header_size < 1) return NULL;
    
    unsigned char* header_buf = malloc(header_size);
    if (!header_buf) return NULL;
    
    if (fread(header_buf, 1, header_size, input) != header_size) {
        free(header_buf);
        return NULL;
    }
    
    header_buf[header_size - 1] = 0x0D;
    return header_buf;
}


/*
* dbf_InitOutputBuffer()
* Create output buffer with header data pre-loaded
*/
static MEM_OUTPUT_DESC* dbf_InitOutputBuffer(unsigned char* header_buf, uint16_t header_size) {
    MEM_OUTPUT_DESC* output_desc = malloc(sizeof(MEM_OUTPUT_DESC));
    if (!output_desc) return NULL;
    
    output_desc->capacity = header_size + CHUNK;
    output_desc->buffer = malloc(output_desc->capacity);
    output_desc->size = 0;
    
    if (!output_desc->buffer) {
        free(output_desc);
        return NULL;
    }
    
    memcpy(output_desc->buffer, header_buf, header_size);
    output_desc->size = header_size;
    
    return output_desc;
}


/*
* dbf_DecompressData()
* Decompresse DBF file data section
*/
static int dbf_DecompressData(FILE* input, uint16_t header_size, MEM_OUTPUT_DESC* output_desc) {
    if (fseek(input, header_size + 4, SEEK_SET) != 0) return -1;
    
    int ret = blast(in_from_file, input, out_to_memory, output_desc);
    if (ret != 0) {
        fprintf(stderr, "blast error: %d\n", ret);
        return -1;
    }
    
    return EXIT_SUCCESS;
}


/*
* dbc_DecompressMemory()
* Read DBC file and decompresses data to memory buffer
*/
unsigned char* dbc_DecompressMemory(FILE* input, size_t* out_size) {
    uint16_t header_size = dbf_ReadHeaderSize(input);
    if (!header_size) return NULL;
    
    unsigned char* header_buf = dbf_ReadHeaderBuffer(input, header_size);
    if (!header_buf) return NULL;
    
    MEM_OUTPUT_DESC* output_desc = dbf_InitOutputBuffer(header_buf, header_size);
    free(header_buf);
    
    if (!output_desc) return NULL;
    
    if (dbf_DecompressData(input, header_size, output_desc) != 0) {
        free(output_desc->buffer);
        free(output_desc);
        *out_size = 0;
        return NULL;
    }
    
    *out_size = output_desc->size;
    unsigned char* result = output_desc->buffer;
    free(output_desc);
    
    return result;
}
