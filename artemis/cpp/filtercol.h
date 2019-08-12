/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

#include <arrow/api.h>

#include <string>
#include <vector>

/*
 * Filter columns by column name
 * 
 * Parameters
 * ----------
 * rb : arrow::RecordBatch (required)
 *    Input record batch.
 * columns : std::vector<std::string> (required)
 *    Keep only columns with these names.
 * invert: bool, default=false
 *    If true, changes meaning of columns: remove these columns and keep the others instead.
 *
 * Returns
 * -------
 * arrow::RecordBatch
 *    Record batch object stripped of specified columns.
 */
std::shared_ptr<arrow::RecordBatch> filter_columns(
        std::shared_ptr<arrow::RecordBatch> rb, std::vector<std::string> columns,
        bool invert=false);
