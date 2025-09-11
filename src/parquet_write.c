/*****************************************************************************
 * parquet_write.c
 *
 * Author: Raicy Augusto
 * Copyright (C) 2025 Raicy Augusto
 * Version: 1.0
 * Date: September 2025
 *
 * This file is part of the dbc2parquet project.
 *
 * Licensed under the Apache License, Version 2.0.
 * See LICENSE file for details.
 ****************************************************************************/



#include <time.h>
#include <string.h>
#include <ctype.h>
#include "parquet_write.h"
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/parquet-glib.h>




/* create_Schema()
 * Creates an Arrow Schema based on the DBF file header.
 */
static GArrowSchema* create_Schema(P_DBF *p_dbf){
    
    GError *error = NULL;
    GList *fields_list = NULL;

    for(unsigned int i = 0; i < p_dbf->columns; i++){
        GArrowDataType *data_type = NULL;

        switch (p_dbf->fields[i].field_type){

            case 'C': data_type = GARROW_DATA_TYPE(garrow_string_data_type_new());  break;
            
            case 'D': data_type = GARROW_DATA_TYPE(garrow_date32_data_type_new());  break;
            
            case 'L': data_type = GARROW_DATA_TYPE(garrow_boolean_data_type_new()); break;

            case 'N': 

                if(p_dbf->fields[i].field_decimals > 0 ) data_type = GARROW_DATA_TYPE(garrow_double_data_type_new());
                
                else{
                    
                    if (p_dbf->fields[i].field_length <= 9) data_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());

                    else data_type = GARROW_DATA_TYPE(garrow_int64_data_type_new());                    
                }
                break;
                       
            default:  data_type = GARROW_DATA_TYPE(garrow_string_data_type_new()); break;
        }

        GArrowField *field = garrow_field_new(p_dbf->fields[i].field_name, data_type);
        fields_list = g_list_append(fields_list, field);        
        g_object_unref(data_type);
    }    

     if(!fields_list){
        g_set_error(&error, g_quark_from_string("create-schema"), 1, "Failed to create field list");
        if(error){
            g_error_free(error);
        }
        return NULL;
    }

    GArrowSchema *schema = garrow_schema_new(fields_list);
    
    if(!schema){
        g_list_free_full(fields_list, g_object_unref);
        g_set_error(&error, g_quark_from_string("create-schema"), 2, "Failed to create Schema");
       
        if(error){
            g_error_free(error);
        }
        return NULL;
    }

    g_list_free_full(fields_list, g_object_unref);
    return schema;
}


static gboolean is_ascii(const gchar *str) {
    while (*str) {
        if ((unsigned char)*str > 127) {
            return FALSE;
        }
        str++;
    }
    return TRUE;
}   

/* create_ArrowBatch()
 * Creates a batch of Arrow arrays and populates it with DBF data.
 */
static GList* create_ArrowBatch(P_DBF *p_dbf, GArrowSchema *schema, int start_row, int num_rows) {
    GError *error = NULL;
    GList *fields = garrow_schema_get_fields(schema);
    GList *column_list = NULL;
    int col = 0;
   
    if (start_row >= p_dbf->header->records) {
        printf("Error: start_row (%d) >= total records (%d)\n", start_row, p_dbf->header->records);
        g_list_free_full(fields, g_object_unref);
        return NULL;
    }

    int actual_rows = (start_row + num_rows > p_dbf->header->records) ? p_dbf->header->records - start_row : num_rows;
    

    for (GList *l = fields; l != NULL; l = l->next, col++) {
        GArrowField *field = (GArrowField *)l->data;       
        GArrowDataType *type = garrow_field_get_data_type(field);
        GArrowArrayBuilder *builder = NULL;
        
        if (GARROW_IS_DATE32_DATA_TYPE(type))      builder = GARROW_ARRAY_BUILDER(garrow_date32_array_builder_new());
        else if (GARROW_IS_INT32_DATA_TYPE(type))  builder = GARROW_ARRAY_BUILDER(garrow_int32_array_builder_new());
        else if (GARROW_IS_INT64_DATA_TYPE(type))  builder = GARROW_ARRAY_BUILDER(garrow_int64_array_builder_new());
        else if (GARROW_IS_DOUBLE_DATA_TYPE(type)) builder = GARROW_ARRAY_BUILDER(garrow_double_array_builder_new());
        else if (GARROW_IS_BOOLEAN_DATA_TYPE(type)) builder = GARROW_ARRAY_BUILDER(garrow_boolean_array_builder_new());
        else builder = GARROW_ARRAY_BUILDER(garrow_string_array_builder_new());
         
        size_t base_row_offset = p_dbf->header->header_length + (p_dbf->header->record_length * start_row);
        size_t field_offset_in_record = p_dbf->fields[col].field_offset;
        size_t record_length = p_dbf->header->record_length;        
        
        size_t *row_offsets = malloc(actual_rows * sizeof(size_t));

        if (!row_offsets) {
            g_printerr("Error: Failed to allocate memory for row_offsets\n");
            g_object_unref(builder);
            g_object_unref(type);
            g_list_free_full(column_list, g_object_unref);
            g_list_free(fields);
            return NULL;
        }

        size_t base_offset = base_row_offset + field_offset_in_record;

        for (int i = 0; i < actual_rows; i++) {
            row_offsets[i] = base_offset + (i * record_length);
        }

        for (int i = 0; i < actual_rows; i++) {
            unsigned char *row_ptr = p_dbf->mem_buffer + row_offsets[i];
            char backup_char = row_ptr[p_dbf->fields[col].field_length];
            
            row_ptr[p_dbf->fields[col].field_length] = '\0';

            char *start = (char*)row_ptr;
            char *end = start + strlen(start) - 1;

            while (*start && isspace(*start)) start++;
            while (end > start && isspace(*end)) *end-- = '\0';

            gboolean is_null = (*start == '\0');

            if (is_null) {
                garrow_array_builder_append_null(builder, &error);
            } else {
                if (GARROW_IS_STRING_DATA_TYPE(type)) {
                    gchar *utf8_string = NULL;

                    if (is_ascii(start))  utf8_string = g_strdup(start);
                    else {
                        utf8_string = g_convert(start, -1, "UTF-8", p_dbf->encoding, NULL, NULL, &error);

                        if (error) {
                            g_printerr("Error converting string to UTF-8: %s\n", error->message);
                            g_error_free(error);
                            g_object_unref(builder);
                            return NULL;
                        }
                    }

                    garrow_string_array_builder_append_string(GARROW_STRING_ARRAY_BUILDER(builder), utf8_string, &error);

                    g_free(utf8_string);

                } else if (GARROW_IS_DATE32_DATA_TYPE(type)) {
                    int year, month, day;
                    sscanf(start, "%4d%2d%2d", &year, &month, &day);

                    struct tm tm = {0};
                    tm.tm_year = year - 1900;
                    tm.tm_mon  = month - 1;
                    tm.tm_mday = day;

                    time_t epoch_seconds = mktime(&tm);
                    
                    gint32 days = (gint32)(epoch_seconds / (24 * 60 * 60));
                    garrow_date32_array_builder_append_value(GARROW_DATE32_ARRAY_BUILDER(builder), days, &error);

                } else if (GARROW_IS_INT32_DATA_TYPE(type)) {

                    gint32 value = g_ascii_strtoll(start, NULL, 10);
                    garrow_int32_array_builder_append_value(GARROW_INT32_ARRAY_BUILDER(builder), value, &error);

                } else if (GARROW_IS_INT64_DATA_TYPE(type)) {

                    gint64 value = g_ascii_strtoll(start, NULL, 10);
                    garrow_int64_array_builder_append_value(GARROW_INT64_ARRAY_BUILDER(builder), value, &error);
                
                } else if (GARROW_IS_DOUBLE_DATA_TYPE(type)) {
                
                    gdouble value = g_ascii_strtod(start, NULL);
                    garrow_double_array_builder_append_value(GARROW_DOUBLE_ARRAY_BUILDER(builder), value, &error);
                
                } else if (GARROW_IS_BOOLEAN_DATA_TYPE(type)) {
                
                    gboolean value = (g_strcmp0(start, "1") == 0);
                    garrow_boolean_array_builder_append_value(GARROW_BOOLEAN_ARRAY_BUILDER(builder), value, &error);
                }
            }

            row_ptr[p_dbf->fields[col].field_length] = backup_char;
            if (error) {
                g_printerr("Error appending to array builder: %s\n", error->message);
                g_error_free(error);
                free(row_offsets);
                g_object_unref(builder);
                g_object_unref(type);
                g_list_free_full(column_list, g_object_unref);
                g_list_free(fields);
                return NULL;
            }
        }

        free(row_offsets);
        GArrowArray *array = garrow_array_builder_finish(builder, &error);

        if (!array || error) {
            if (error) {
                g_printerr("Error finishing array builder: %s\n", error->message);
                g_error_free(error);
            }
            g_object_unref(builder);
            g_list_free_full(column_list, g_object_unref);
            g_list_free(fields);
            return NULL;
        }
        
        column_list = g_list_append(column_list, array);
        g_object_unref(builder);
    }

    g_list_free_full(fields, g_object_unref);
    return column_list;
}



/* write_Parquet()
 * Converts and saves the DBF data to a Parquet file.
 */
int write_Parquet(P_DBF *p_dbf, gchar *path){
    int batch_size = 10000;
    GError *error = NULL;
    GArrowSchema *schema = create_Schema(p_dbf);
    GParquetWriterProperties *writer_properties = NULL;
    GParquetArrowFileWriter *writer = NULL; 
    
    if (!schema) {
        g_printerr("Error creating Schema: %s\n", error->message);
        goto fail;
    }

    writer_properties = gparquet_writer_properties_new();
    if (!writer_properties) {
        g_printerr("Error creating writer properties\n");
        goto fail;
    }

    gparquet_writer_properties_set_compression(writer_properties, GARROW_COMPRESSION_TYPE_ZSTD, NULL);
    if (error) {
        g_printerr("Error setting compression: %s\n", error->message);
        goto fail;
    }

    writer = gparquet_arrow_file_writer_new_path(schema, path, writer_properties, &error);
    if(!writer){
        g_printerr("Error creating Parquet writer: %s\n", error ? error->message : "Unknown error");
        goto fail;
    }

    {
        for (int start = 0; start < p_dbf->header->records; start += batch_size) {
            
            int actual_rows = MIN(batch_size, p_dbf->header->records - start);
        
            GList *batch_arrays = create_ArrowBatch(p_dbf, schema, start, actual_rows);
            
            if (!batch_arrays) {
                g_printerr("Error creating array batch: %s\n", error->message);
                goto fail;
            }

            GArrowRecordBatch *record_batch = garrow_record_batch_new(schema, actual_rows, batch_arrays, &error);

            if (!record_batch) {
                g_printerr("Error creating record batch: %s\n", error->message);
                g_list_free_full(batch_arrays, g_object_unref);
                goto fail;
            }

            gboolean write_success = gparquet_arrow_file_writer_write_record_batch(writer, record_batch, &error);
            if (!write_success || error) {
                g_printerr("Error writing record batch: %s\n", error ? error->message : "Unknown error");
                g_object_unref(record_batch);
                g_list_free_full(batch_arrays, g_object_unref);
                goto fail;
            }

            g_object_unref(record_batch);
            g_list_free_full(batch_arrays, g_object_unref);
        }
    }
    
    gboolean close_success = gparquet_arrow_file_writer_close(writer, &error);
    if (!close_success || error) {
        g_printerr("Error closing Parquet file: %s\n", error ? error->message : "Unknown error");
        goto fail;
    }

    g_object_unref(writer);
    g_object_unref(writer_properties);
    g_object_unref(schema);

    return EXIT_SUCCESS;

fail:
    if (writer) {
        gparquet_arrow_file_writer_close(writer, NULL);
        g_object_unref(writer);
    }
    if (writer_properties) g_object_unref(writer_properties);
    if (schema) g_object_unref(schema);
    if (error) g_error_free(error);
    return EXIT_FAILURE;

}

