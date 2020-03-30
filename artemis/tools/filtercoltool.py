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

from artemis.tools._filtercoltool import Filter
from artemis.decorators import iterable
from artemis.core.tool import ToolBase

"""
"""


@iterable
class FilterColToolOptions:
    pass


class FilterColTool(ToolBase):
    def __init__(self, name, **kwargs):
        """
        Parameters. Configured once.
        Remove specified columns from record batch.
        Default mode: keep only columns matching given names.
        If invert=True, remove only columns matching given names.
        """
        self.options = dict(FilterColToolOptions())
        self.options.update(kwargs)

        super().__init__(name, **self.options)
        self.__logger.info("%s: __init__ FilterColTool", self.name)
        self.__logger.info("Options: %s", self.options)

        if "invert" not in self.options:
            self.options["invert"] = False
        if "columns" not in self.options:
            self.options["columns"] = None
            self.__logger.warning(
                "No columns option provided. " "Returning original record batches."
            )
        else:
            # Set options and convert to C/C++ types only once
            self.filter = Filter(self.options["columns"], self.options["invert"])

    def initialize(self):
        pass

    def execute(self, record_batch):
        """
        Filter columns by column name

        Parameters
        ----------
        rb : arrow::RecordBatch (required)
            Input record batch.
        columns : std::vector<std::string> (required)
            Keep only columns with these names.
        invert: bool, default=false
            If true, changes meaning of columns:
            remove these columns and keep the others instead.

        Returns
        -------
        arrow::RecordBatch
        Record batch object stripped of specified columns.
        """
        # If no columns are specified, return original batch
        if self.options["columns"] is None:
            return record_batch
        try:
            return self.filter.filter_columns(record_batch)
        except Exception:
            raise Exception("Error filtering columns.")

    def finalize(self):
        pass
