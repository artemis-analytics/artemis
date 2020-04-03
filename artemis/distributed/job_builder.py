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

"""
import dask

from artemis.artemis import Artemis
from artemis_format.pymodels.artemis_pb2 import JobInfo as JobInfo_pb


class JobBuilder:
    """
    Class the simulate functionality of Artemis
    """

    def __init__(
        self,
        dirpath,
        store_name,
        store_uuid,
        menu_uuid,
        config_uuid,
        dataset_uuid,
        parentset_uuid,
        job_id,
    ):

        self.job = JobInfo_pb()
        self.job.name = "arrowproto"
        self.job.job_id = "example"
        self.job.store_path = dirpath
        self.job.store_id = store_uuid
        self.job.store_name = store_name
        self.job.menu_id = menu_uuid
        self.job.config_id = config_uuid
        self.job.dataset_id = dataset_uuid
        self.job.parentset_id = parentset_uuid
        self.job.job_id = str(job_id)

    def execute(self):
        bow = Artemis(self.job, loglevel="INFO")
        bow.control()
        buf = bow.gate.store[self.job.dataset_id].dataset.SerializeToString()
        return buf


@dask.delayed
def runjob(
    dirpath,
    store_name,
    store_uuid,
    menu_uuid,
    config_uuid,
    dataset_uuid,
    parentset_uuid,
    job_id,
):

    runner = JobBuilder(
        dirpath,
        store_name,
        store_uuid,
        menu_uuid,
        config_uuid,
        dataset_uuid,
        parentset_uuid,
        job_id,
    )
    msg = runner.execute()
    return msg
