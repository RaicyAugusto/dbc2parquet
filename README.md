# dbc2parquet

![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)
![Language](https://img.shields.io/badge/Language-C++-blue.svg)
![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20Windows-lightgrey.svg)
![Apache Arrow](https://img.shields.io/badge/Powered%20by-Apache%20Arrow-orange.svg)

Convert DBC files (compressed DBF, used by DATASUS) to Apache Parquet.

Fast, in-memory, no temporary files. Output is ZSTD-compressed Parquet with
automatic type mapping and UTF-8 conversion from legacy encodings.

## Download

Standalone binaries on the [Releases](../../releases) page — no dependencies to
install:

- **Linux:** `dbc_parquet`
- **Windows:** `dbc_parquet.exe`

## Usage

```
dbc_parquet input.dbc               # output: input.parquet
dbc_parquet input.dbc output.parquet
```

Add `--no-wait` when calling from a script (skips the exit prompt).

## Build

**Linux**:

```sh
podman run -it --rm -v "$PWD":/work:Z -w /work alpine:latest sh
apk add --no-cache build-base cmake ninja git bash linux-headers openssl-dev openssl-libs-static perl

git clone --depth 1 --branch apache-arrow-23.0.0 https://github.com/apache/arrow /tmp/arrow
cmake -B /tmp/arrow/cpp/build -S /tmp/arrow/cpp \
  -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
  -DARROW_DEPENDENCY_USE_SHARED=OFF -DARROW_DEPENDENCY_SOURCE=BUNDLED \
  -DARROW_PARQUET=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_SNAPPY=OFF \
  -DARROW_MIMALLOC=OFF -DCMAKE_INSTALL_PREFIX=/opt/arrow-static
cmake --build /tmp/arrow/cpp/build -j$(nproc) --target install

cmake -B build -S . -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH=/opt/arrow-static -DCMAKE_EXE_LINKER_FLAGS="-static"
cmake --build build -j$(nproc)
```

**Windows** (MSVC):

```bat
git clone --depth 1 --branch apache-arrow-23.0.0 https://github.com/apache/arrow C:\arrow
cmake -B C:\arrow\cpp\build -S C:\arrow\cpp -G Ninja ^
  -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF ^
  -DARROW_DEPENDENCY_USE_SHARED=OFF -DARROW_DEPENDENCY_SOURCE=BUNDLED ^
  -DARROW_PARQUET=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_SNAPPY=OFF ^
  -DARROW_MIMALLOC=OFF -DARROW_USE_STATIC_CRT=ON ^
  -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded -DCMAKE_INSTALL_PREFIX=C:/arrow-install
cmake --build C:\arrow\cpp\build --target install

cmake -B build -S . -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=C:/arrow-install
cmake --build build
```

## Credits

- [fast_float](https://github.com/fastfloat/fast_float) — Daniel Lemire
- [blast](https://github.com/madler/zlib/tree/master/contrib/blast) — Mark Adler
- libdbf — Björn Berg
- [blast-dbf](https://github.com/eaglebh/blast-dbf) — Pablo Fonseca

## License

Apache 2.0 — Raicy Augusto. See `LICENSE`.
