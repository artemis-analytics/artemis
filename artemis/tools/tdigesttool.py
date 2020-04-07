#! /usr/bin/env python
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

"""
The purpose of this tool will be to add a
distribution generator tool to the artemis toolset
Used for validation purpose
"""

from artemis_externals.tdigest.tdigest import TDigest
from artemis_base.utils.decorators import iterable
from artemis_base.core.tool import ToolBase


@iterable
class TDigestToolOptions:
    #  Add user-degined options for Artemis.distributiontool
    pass


class TDigestTool(ToolBase):
    def __init__(self, name, **kwargs):

        super().__init__(name, **kwargs)

    def initialize(self):
        self.__logger.info(
            "%s properties: %s", self.__class__.__name__, self.properties
        )

    def execute(self, record_batch):
        """

        This tool will read in a py arrow record batch or
        a read from a csv file and run a t-digest
        analysis in a distributed fashion

        Parameters
        ----------
        file_name: the name of the file that will
        be read from and digest created, the file
        will be assumed to be in the 'examples/

        Returns
        ---------
        None

        Although this returns none for now, it will be possible
        to return a pyarrow record bactch or simmilar data structure
        as it goes to a record batch and then writes directly to a csv file
        """

        # Print the schema of the record batch that this tool has recived
        # Get the schema from the record batch data
        # and use this to extract the names of the
        batch_schema_names = record_batch.schema.names

        # Declare a map that will be returned by the digests
        digest_map = {}

        columns = record_batch.columns

        # Add the columns that are numerical ie: float, double, int
        # Create the tdigets and the map that will be returned
        for i in range(len(columns)):
            if (
                columns[i].type == "double"
                or columns[i].type == "float"
                or columns[i].type == "int"
            ):
                try:
                    digest = TDigest()

                    digest.batch_update(columns[i].to_pandas(use_threads=True))

                    digest_map[batch_schema_names[i]] = digest
                except Exception:
                    self.__logger.error("Unable to update TDigest")
                    raise

        return digest_map
