/*****************************************************************************
 * main.c
 *
 * Author: Raicy Augusto
 * Copyright (C) 2025 Raicy Augusto
 * Version: 1.0
 * Date: September 2025
 *
 * This file is part of the dbc2parquet project.
 * Main entry point for DBC to Parquet file conversion.
 *
 * Licensed under the Apache License, Version 2.0.
 *****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dbf_reader.h"
#include "parquet_write.h"



int main(int argc, char **argv){
    FILE* input = NULL;
    size_t data_size = 0;
    unsigned char* data_buffer = NULL;
    P_DBF *p_dbf = NULL;

    // clock_t start, end;
    // double cpu_time_used; 

    if(argc < 3){
        fprintf(stderr, "Usage: %s input.dbc output.parquet\n", argv[0]);
        return EXIT_FAILURE;
    }

    if(strstr(argv[1],".dbc") == NULL || strstr(argv[2],".parquet") == NULL){
        fprintf(stderr, "Usage: %s input.dbc output.parquet\n", argv[0]);
        return EXIT_FAILURE;
    }

    char *input_file = argv[1];
    char *output_file = argv[2];

    input = fopen(input_file, "rb");
    if (!input) {
        fprintf(stderr, "Error opening input file: %s\n", input_file);
        return EXIT_FAILURE;
    }

    
    // Decompress DBC data
    data_buffer = dbc_DecompressMemory(input, &data_size);

    fclose(input);

    if (!data_buffer) {
        fprintf(stderr, "Decompression failed\n");
        return EXIT_FAILURE;
    }
    
    // Open DBF from memory
    p_dbf = dbf_OpenFromMemory(data_buffer, data_size);
    if (!p_dbf) {
        fprintf(stderr, "Error opening DBF from memory\n");
        free(data_buffer);
        return EXIT_FAILURE;
    }

    // start = clock();

    // Write Parquet file
    if (write_Parquet(p_dbf, output_file) != EXIT_SUCCESS) {
        fprintf(stderr, "Error writing Parquet file\n");
        dbf_Close(p_dbf);
        return EXIT_FAILURE;
    }

    // end = clock();

    // cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;

    //printf("Tempo decorrido: %f segundos\n", cpu_time_used);

    printf("Converted: %s -> %s\n", input_file, output_file);
    dbf_Close(p_dbf);
    
    return EXIT_SUCCESS;
}