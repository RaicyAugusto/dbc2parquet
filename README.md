
# dbc2parquet

![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)
![Language](https://img.shields.io/badge/Language-C++-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows-lightgrey.svg)
![Apache Arrow](https://img.shields.io/badge/Powered%20by-Apache%20Arrow-orange.svg)

**Author:** Raicy Augusto  
**E-mail:** mailto:raicy.augusto.rodrigues@gmail.com  
**License:** Apache License 2.0

## Introduction

A command-line tool to convert DBC (compressed DBF) files to the Apache Parquet format. DBC files are a compressed variant of dBase files, commonly used by DATASUS (the Brazilian Public Health System) to distribute public health data.

This tool provides a fast, memory-efficient way to make this data accessible in modern data analysis ecosystems.

## Features

- Fast in-memory decompression of DBC files (no temporary files).
- Direct conversion to the efficient, columnar Parquet format.
- Automatic data type mapping (string, integer, double, date, boolean).
- Handles character encoding conversion to UTF-8.
- Memory-efficient batch processing for large datasets.
- Cross-platform support (Windows and Linux).

## Usage

The tool is executed from the command line, providing the input DBC file and the desired output Parquet file path.

```bash
# Basic syntax
dbc2parquet <input_file.dbc> <output_file.parquet>
```

### Example

```bash
# Convert a sample DBC file to Parquet
./dbc2parquet ERSC2504.dbc output.parquet
```

*(Note: If you compiled the project, the executable might be inside a `build/` or `build/Release` directory.)*

## Building from Source

This project uses CMake and can be compiled on Windows and Linux. The instructions vary depending on your operating system.

### 1. Windows

Compiling on Windows requires a C++ compiler and the `vcpkg` package manager to handle dependencies.

**Step 1: Install Build Tools**

You need the Microsoft C++ (MSVC) compiler. The easiest way is to install the **Visual Studio Build Tools**.

1.  Go to the [Visual Studio Downloads page](https://visualstudio.microsoft.com/downloads/).
2.  Scroll down to **"Tools for Visual Studio"** and download **"Build Tools for Visual Studio"**.
3.  Run the installer. In the "Workloads" tab, select **"Desktop development with C++"**. This will install the compiler, CMake, and the necessary Windows SDKs.

**Step 2: Install Dependencies with vcpkg**

1.  Clone and set up `vcpkg` (we recommend a path like `C:\vcpkg`):
```bash
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg
./bootstrap-vcpkg.bat
```

2.  Install the Apache Arrow library (this will also install Parquet):
```bash
# This command installs the 64-bit shared libraries (DLLs)
./vcpkg install arrow[parquet]:x64-windows
```

**Step 3: Compile the Project**

1.  Open a **new** Command Prompt or PowerShell window (to ensure the build tools are in the PATH).
3.  Navigate to the `dbc2parquet` project directory.
3.  Run CMake to configure the build, pointing it to the `vcpkg` toolchain file. **Replace `C:\vcpkg` with your actual `vcpkg` path.**
```bash
# Configure the project
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake

# Compile the project
cmake --build build --config Release
```

4. The executable will be located at `build/Release/dbc2parquet.exe`.

**Important:** To run the executable, you must copy the required `.dll` files from the `vcpkg` installation directory (`C:\vcpkg\installed\x64-windows\bin`) to the same folder as `dbc2parquet.exe`.

### 2. Linux (Debian/Ubuntu)

On Linux, dependencies can be installed using the system's package manager.

**Step 1: Install Build Tools and Dependencies**

1.  Install the essential build tools (compiler, CMake, etc.):
```bash
sudo apt update
sudo apt install -y build-essential cmake
```

2.  Install the Apache Arrow and Parquet development libraries. Follow the [official instructions](https://arrow.apache.org/install/) to add the Arrow repository first, then install the packages:
```bash
# Example commands after setting up the Arrow repository
sudo apt install -y libarrow-dev libparquet-dev
```

**Step 2: Compile the Project**

1.  Navigate to the `dbc2parquet` project directory.

2.  Configure and build the project with CMake:
```bash
# Configure the project for a Release build
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# Compile
cmake --build build
```

3.  The executable will be located at `build/dbc2parquet`.

## Project Structure

```
src/
├── libs/
│   └── fast-float/     # Header-only library for fast float parsing
├── blast.c             # Decompression logic (from zlib contrib)
├── blast.h
├── dbf_reader.cpp      # Logic for reading DBF structure from memory
├── dbf_reader.hpp
├── parquet_write.cpp   # Logic for converting records and writing Parquet files
├── parquet_write.hpp
└── main.cpp            # Main application entry point
```

## File Format Support

- **Input**: DBC files (compressed DBF format).
- **Output**: Apache Parquet (using ZSTD compression by default).
- **Encoding**: Automatic conversion to UTF-8 from common legacy encodings (CP437, CP850, CP1252, etc.).

## Test Data

The included `ERSC2504.dbc` file is sample data from DATASUS, used for testing purposes only.

## Technical Details

This tool implements in-memory processing of DBC files. The `blast.c` library decompresses the file into a memory buffer. The `dbf_reader` parses the DBF headers from this buffer, and `parquet_write` reads records in batches, converts them to the Arrow format, and writes them to a Parquet file. This architecture is highly efficient and avoids creating temporary files on disk.

## Code Attribution

This project incorporates code from the following sources:

- **fast-float** by Daniel Lemire - High-performance C++ library for number parsing.
  - Repository: https://github.com/fastfloat/fast_float

- **blast decompression library** by Mark Adler - PKWare DCL decompression
    - Files: `blast.h`, `blast.c` (used as-is)
    - Part of the zlib project: https://github.com/madler/zlib/tree/master/contrib/blast

- **libdbf** by Björn Berg - DBF structure definitions (adapted and modified)
    - Original source: `dbf.berlios.de` (discontinued)
    - A modern fork: https://github.com/quentindemetz/libdbf

- **blast-dbf** by Pablo Fonseca - Reference for understanding the DBC file format
    - Repository: https://github.com/eaglebh/blast-dbf

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.

Note: This project incorporates code from multiple sources with different licenses. See individual source files for specific licensing information.