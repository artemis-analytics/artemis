/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
