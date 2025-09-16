/*****************************************************************************
 * @file main.cpp
 * @brief Main entry point for the DBC to Parquet file converter.
 *
 * This file contains the main() function and supporting functions
 * for the DBC to Parquet converter application. It handles command-line
 * arguments, file I/O, and orchestrates the conversion process.
 *
 * @author Raicy Augusto
 * @copyright Copyright (C) 2025 Raicy Augusto
 * @version 1.0
 * @date September 2025
 *
 * This file is part of the dbc2parquet_cpp project.
 * Licensed under the Apache License, Version 2.0.
****************************************************************************/

#ifdef _WIN32
#include <io.h>
#define isatty _isatty
#define fileno _fileno
#else
#include <unistd.h>
#endif


#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <chrono>
#include <string>
#include "dbf_reader.hpp"
#include "parquet_write.hpp"
#include <arrow/status.h>


/**
 * @brief Waits for user input only in interactive terminals.
 */
void wait_if_interactive() {
    if (isatty(fileno(stdin))) {
        std::cout << "\nPress any key to exit...";
        std::cin.get();
    }
}

/**
 * @brief Generates an output filename for the Parquet file based on the input filename.
 *
 * This function takes the input filename, removes its extension (if any),
 * and appends ".parquet" to create the output filename.
 *
 * @param input_file The input filename (including path if applicable).
 * @return std::string The generated output filename for the Parquet file.
 */
std::string generate_output_filename(const std::string& input_file) {
    std::string output = input_file;
    size_t pos = output.find_last_of('.');
    if (pos != std::string::npos) {
        output = output.substr(0, pos);
    }
    output += ".parquet";
    return output;
}

/**
 * @brief Main entry point for the DBC to Parquet converter application.
 *
 * This function handles command-line arguments, reads the input DBC file,
 * converts it to Parquet format, and writes the output file. It also
 * measures and reports the time taken for the conversion process.
 *
 * @param argc The number of command-line arguments.
 * @param argv An array of command-line argument strings.
 * @return int Returns 0 on successful execution, -1 on error.
 */
int main(const int argc, char **argv){
    std::cout << "DBC to Parquet Converter v1.0\n";
    std::cout << "Author: Raicy Augusto | github.com/RaicyAugusto/dbc2parquet\n";
    std::cout << "==============================\n\n";

    FILE* input = nullptr;
    DBF dbf;

    // Início da medição
    auto start = std::chrono::high_resolution_clock::now();

    const char *input_file;
    const char *output_file;


    if(argc == 2){
        input_file = argv[1];
        static std::string output_name = generate_output_filename(input_file);
        output_file = output_name.c_str();
        std::cout << "Input: " << input_file << std::endl;
        std::cout << "Output: " << output_file << std::endl;
    }
    else if(argc == 3){
        input_file = argv[1];
        output_file = argv[2];

        if(std::strstr(argv[1],".dbc") == nullptr || std::strstr(argv[2],".parquet") == nullptr){
            std::cerr << "Usage: " << argv[0] << " input.dbc output.parquet\n";
            return -1;
        }
    }
    else {
        std::cerr << "Usage: " << argv[0] << " input.dbc [output.parquet]\n";
        return -1;
    }

    std::cout << "\nStarting conversion...\n";

    input = fopen(input_file, "rb");
    if (!input) {
        std::cerr << "Error opening input file: " << input_file << "\n";
        wait_if_interactive();
        return -1;
    }


    // Decompress DBC data
    if (!dbc_load_dbf(input, dbf)) {
        std::cerr << "Error loading DBC data\n";
        std::fclose(input);
        wait_if_interactive();
        return -1;
    }

    std::fclose(input);

    // Write Parquet file
    auto status = write_parquet(dbf, output_file);
    if (!status.ok()) {
        std::cerr << "Error: Failed to write Parquet file\n";
        wait_if_interactive();
        return -1;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    std::cout << "\n Conversion completed successfully!\n";
    std::cout << "Time elapsed: " << duration_sec.count() << " seconds\n";
    std::cout << "Output saved to: " << output_file << "\n";

    wait_if_interactive();

    return 0;
}