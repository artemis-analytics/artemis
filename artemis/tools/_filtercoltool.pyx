# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# distutils: language = c++
# distutils: sources = "artemis/cpp/filtercol.cpp"
# cython: language_level = 3

from artemis.includes.libfiltercol cimport CFilterColumns
from pyarrow import RecordBatch
from pyarrow.compat import frombytes, tobytes
from pyarrow.lib cimport (pyarrow_wrap_batch, pyarrow_unwrap_batch)
from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector


cdef struct filter_options:
    vector[c_string] columns
    c_bool invert

cdef class Filter:
    cdef:
        filter_options options

    def __init__(self, columns, invert=False):
        self.columns = columns
        self.invert = invert

    @property
    def columns(self):
        return [frombytes(x) for x in self.options.columns]

    @columns.setter
    def columns(self, value):
        self.options.columns = [tobytes(x) for x in value]

    @property
    def invert(self):
        return self.options.invert

    @invert.setter
    def invert(self, value):
        self.options.invert = value
    
    def filter_columns(self, rb):
        if type(rb) != RecordBatch:
            raise Exception('variable is not of type pyarrow.lib.RecordBatch')
        return pyarrow_wrap_batch(CFilterColumns(pyarrow_unwrap_batch(rb), 
                                  self.options.columns, self.options.invert))   
