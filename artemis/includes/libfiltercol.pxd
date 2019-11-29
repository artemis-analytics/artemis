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
from pyarrow.lib cimport shared_ptr, CRecordBatch
from libcpp cimport bool as c_bool
from libcpp.vector cimport vector
from libcpp.string cimport string as c_string

cdef extern from "../cpp/filtercol.h":
    cdef shared_ptr[CRecordBatch] CFilterColumns" filter_columns"(
            shared_ptr[CRecordBatch], vector[c_string], c_bool)
