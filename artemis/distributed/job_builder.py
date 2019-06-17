#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import dask

from artemis.artemis import Artemis
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb


class JobBuilder():
    '''
    Class the simulate functionality of Artemis
    '''
    def __init__(self, dirpath,
                 store_name,
                 store_uuid,
                 menu_uuid,
                 config_uuid,
                 dataset_uuid,
                 job_id):

        self.job = JobInfo_pb()
        self.job.name = 'arrowproto'
        self.job.job_id = 'example'
        self.job.store_path = dirpath
        self.job.store_id = store_uuid
        self.job.store_name = store_name
        self.job.menu_id = menu_uuid
        self.job.config_id = config_uuid
        self.job.dataset_id = dataset_uuid
        self.job.job_id = str(job_id)

    def execute(self):
        bow = Artemis(self.job, loglevel='INFO')
        bow.control()
        buf = bow._jp.store[self.job.dataset_id].dataset.SerializeToString()
        return buf


@dask.delayed
def runjob(dirpath,
           store_name,
           store_uuid,
           menu_uuid,
           config_uuid,
           dataset_uuid,
           job_id):

    runner = JobBuilder(dirpath,
                        store_name,
                        store_uuid,
                        menu_uuid,
                        config_uuid,
                        dataset_uuid,
                        job_id)
    msg = runner.execute()
    return msg
