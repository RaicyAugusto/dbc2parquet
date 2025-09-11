/*****************************************************************************
 * parquet_write.h
 *
 * Author: Raicy Augusto
 * Copyright (C) 2025 Raicy Augusto
 * Version: 1.0
 * Date: September 2025
 *
 * This file is part of the dbc2parquet project.
 * Interface for parquet_write.c
 *
 * Licensed under the Apache License, Version 2.0. 
 * See LICENSE file for details.
 ****************************************************************************/


#ifndef PARQUET_WRITE_H
#define PARQUET_WRITE_H

#include "dbf_reader.h"
#include <glib.h>

// ================================ FUNCTION PROTOTYPES ================================

/* write_Parquet()
 * Converts and saves the DBF data to a Parquet file.
 */
int write_Parquet(P_DBF *p_dbf, gchar *path);

#endif
