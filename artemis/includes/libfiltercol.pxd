# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.
from pyarrow.lib cimport shared_ptr, CRecordBatch
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector
from libcpp.string cimport string as c_string

cdef extern from "../cpp/filtercol.h":
    cdef shared_ptr[CRecordBatch] CFilterColumns" filter_columns"(
            shared_ptr[CRecordBatch], vector[c_string], c_bool)
