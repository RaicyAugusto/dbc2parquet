# Versão mínima do CMake e nome do projeto
cmake_minimum_required(VERSION 3.16)

project(dbc2parquet LANGUAGES C CXX)


set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()


if(DEFINED ENV{VCPKG_ROOT} AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")
endif()

find_package(Arrow REQUIRED COMPONENTS Parquet)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Usando Arrow: ${Arrow_DIR}")

set(SOURCES
        ../dbc_parquet_exes/g/src/main.cpp
        ../dbc_parquet_exes/g/src/dbf_reader.hpp
        ../dbc_parquet_exes/g/src/dbf_reader.cpp
        ../dbc_parquet_exes/g/src/parquet_write.hpp
        ../dbc_parquet_exes/g/src/parquet_write.cpp
        ../dbc_parquet_exes/g/src/blast.c
)

add_executable(dbc_parquet ${SOURCES})

target_include_directories(dbc_parquet PRIVATE
        ../dbc_parquet_exes/g/src
        ../dbc_parquet_exes/g/src/libs
)

target_link_libraries(dbc_parquet PRIVATE
        Arrow::arrow_shared
        Arrow::parquet_shared
        Threads::Threads
)

if(WIN32)
    target_compile_definitions(dbc_parquet PRIVATE
            WIN32_LEAN_AND_MEAN
            NOMINMAX
            _CRT_SECURE_NO_WARNINGS
    )
endif()
