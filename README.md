# dbc2parquet

![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)
![Language](https://img.shields.io/badge/Language-C-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Linux-lightgrey.svg)
![Apache Arrow](https://img.shields.io/badge/Powered%20by-Apache%20Arrow-orange.svg)

**Author:** Raicy Augusto  
**E-mail:** mailto:raicy.augusto.rodrigues@gmail.com  
**License:** Apache License 2.0

## Introduction

A command-line tool to convert DBC (compressed DBF) files to Parquet format. DBC files are commonly used by DATASUS (Brazilian Public Health System) to distribute compressed database files.

## Features

- Fast in-memory decompression of DBC files
- Direct conversion to efficient Parquet format
- Preserves data types and encoding information
- Memory-efficient processing for large datasets

## Usage

```bash
./dbc2parquet input.dbc output.parquet
```

### Example
```bash
make test 
# or manually:
./dbc2parquet ERSC2504.dbc output.parquet
```

## Building

### Prerequisites
- GCC compiler
- Apache Arrow GLib (C wrapper for Apache Arrow)
- Apache Parquet GLib
- GLib development libraries

### Compilation
```bash
make
```

## File Format Support

- **Input**: DBC files (compressed DBF format)
- **Output**: Apache Parquet format
- **Encoding**: Automatic detection of CP437, CP850, CP852, CP1252

## Test Data

The included `ERSC2504.dbc` file is sample data from DATASUS used for testing purposes only.

## Technical Details

This tool implements in-memory processing of DBC files, decompressing the data and parsing DBF structures before converting to Parquet format. The architecture allows for efficient handling of large datasets without temporary file creation.

## Dependencies

This project uses and adapts code from several open source projects:

- **libdbf** by Björn Berg - DBF file format handling (adapted for memory-based processing)
  - Original source: dbf.berlios.de (discontinued)
  - License: Permission granted with copyright notice
  - Repository: https://github.com/quentindemetz/libdbf

- **blast decompression library** by Mark Adler - PKWare DCL decompression
  - Part of zlib project
  - Used for DBC file decompression
  - Repository: https://github.com/madler/zlib/tree/master/contrib/blast

- **blast-dbf** by Pablo Fonseca - Reference implementation for DBC handling
  - Repository: https://github.com/eaglebh/blast-dbf
  - Inspiration for DBC file structure understanding

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

Note: This project incorporates code from multiple sources with different licenses. See individual source files for specific licensing information.
