/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

#include "filtercol.h"

std::shared_ptr<arrow::RecordBatch> filter_columns(std::shared_ptr<arrow::RecordBatch> rb, 
                                                   std::vector<std::string> columns, 
                                                   bool invert) {
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
    std::vector<std::shared_ptr<arrow::Array> > new_arrays;
    std::vector<std::shared_ptr<arrow::Field> > new_fields;

    bool match_found;
    for (int i=0; i < rb->num_columns(); i++) {
        match_found = false;
        for (int j=0; j < columns.size(); j++) {
            if (rb->column_name(i).compare(columns[j]) == 0) {
                if (!invert) {
                    // Add to array vector
                    new_fields.push_back(rb->schema()->field(i));
                    new_arrays.push_back(rb->column(i));
                }
                match_found = true;
                columns.erase(columns.begin() + j);
                break;
            }
        }
        if (invert && !match_found) {
            new_fields.push_back(rb->schema()->field(i));
            new_arrays.push_back(rb->column(i));
        }
    }
    std::shared_ptr<arrow::Schema> new_schema = std::make_shared<arrow::Schema>(new_fields); 
    return arrow::RecordBatch::Make(new_schema, rb->num_rows(), new_arrays);
}
