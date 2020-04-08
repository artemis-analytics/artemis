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
Test protobuf model implementation
"""

import unittest
import tempfile
import uuid
import logging

# from collections import OrderedDict
from artemis.artemis import Artemis
from cronus.core.cronus import BaseObjectStore
from artemis_format.pymodels.cronus_pb2 import (
    MenuObjectInfo,
    ConfigObjectInfo,
    TableObjectInfo,
)
from artemis_format.pymodels.table_pb2 import Table
from artemis.generators.simutablegen import SimuTableGen

from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.core.gate import ArtemisGateSvc
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis_format.pymodels.artemis_pb2 import JobInfo as JobInfo_pb


logging.getLogger().setLevel(logging.INFO)


class SimuTableTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def test_simutablegen(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store = BaseObjectStore(dirpath, "artemis")

            g_dataset = store.register_dataset()
            store.new_partition(g_dataset.uuid, "generator")
            job_id = store.new_job(g_dataset.uuid)

            # define the schema for the data
            g_table = Table()
            g_table.name = "EvolveModel"
            g_table.uuid = str(uuid.uuid4())
            schema = g_table.info.schema.info
            field = schema.fields.add()
            field.name = "Name"
            field.info.type = "String"
            field.info.length = 10
            field.info.aux.generator.name = "name"

            tinfo = TableObjectInfo()
            store.register_content(
                g_table,
                tinfo,
                dataset_id=g_dataset.uuid,
                job_id=job_id,
                partition_key="generator",
            )

            generator = SimuTableGen(
                "generator",
                nbatches=1,
                num_rows=10000,
                file_type=1,
                table_id=g_table.uuid,
            )

            generator.gate.meta.parentset_id = g_dataset.uuid
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            for batch in generator:
                print(batch)

    def test_simutable_artemis(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)
        with tempfile.TemporaryDirectory() as dirpath:
            mb = MenuFactory("csvgen")
            msgmenu = mb.build()
            menuinfo = MenuObjectInfo()
            menuinfo.created.GetCurrentTime()

            store = BaseObjectStore(dirpath, "artemis")

            g_dataset = store.register_dataset()
            store.new_partition(g_dataset.uuid, "generator")
            job_id = store.new_job(g_dataset.uuid)

            # define the schema for the data
            g_table = Table()
            g_table.name = "EvolveModel"
            g_table.uuid = str(uuid.uuid4())
            schema = g_table.info.schema.info

            field1 = schema.fields.add()
            field1.name = "record_id"
            field1.info.type = "String"
            field1.info.length = 10

            field2 = schema.fields.add()
            field2.name = "Name"
            field2.info.type = "String"
            field2.info.length = 10
            field2.info.aux.generator.name = "name"

            field3 = schema.fields.add()
            field3.name = "SIN"
            field3.info.type = "String"
            field3.info.length = 10
            field3.info.aux.generator.name = "ssn"

            field4 = schema.fields.add()
            field4.name = "StreetNumber"
            field4.info.type = "String"
            field4.info.length = 40
            field4.info.aux.generator.name = "building_number"

            field5 = schema.fields.add()
            field5.name = "Street"
            field5.info.type = "String"
            field5.info.length = 40
            field5.info.aux.generator.name = "street_name"

            field6 = schema.fields.add()
            field6.name = "City"
            field6.info.type = "String"
            field6.info.length = 40
            field6.info.aux.generator.name = "city"

            field7 = schema.fields.add()
            field7.name = "Province"
            field7.info.type = "String"
            field7.info.length = 40
            field7.info.aux.generator.name = "province"

            field8 = schema.fields.add()
            field8.name = "PostalCode"
            field8.info.type = "String"
            field8.info.length = 40
            field8.info.aux.generator.name = "postcode"

            field9 = schema.fields.add()
            field9.name = "DOB"
            field9.info.type = "DateTime"
            field9.info.length = 40
            field9.info.aux.generator.name = "date"

            field10 = schema.fields.add()
            field10.name = "PhoneNum"
            field10.info.type = "String"
            field10.info.length = 11
            field10.info.aux.generator.name = "phone_number"

            tinfo = TableObjectInfo()
            store.register_content(
                g_table,
                tinfo,
                dataset_id=g_dataset.uuid,
                job_id=job_id,
                partition_key="generator",
            )

            store.save_store()
            config = JobConfigFactory(
                "csvgen",
                msgmenu,
                jobname="arrowproto",
                generator_type="simutable",
                filehandler_type="csv",
                nbatches=10,
                # num_cols=20,
                num_rows=10000,
                table_id=g_table.uuid,
                linesep="\r\n",
                delimiter=",",
                max_buffer_size=10485760,
                max_malloc=2147483648,
                write_csv=True,
                output_repo=dirpath,
                seed=42,
            )
            config.configure()
            config.add_algos(mb.algos)
            configinfo = ConfigObjectInfo()
            configinfo.created.GetCurrentTime()

            menu_uuid = store.register_content(msgmenu, menuinfo).uuid
            config_uuid = store.register_content(config._msg, configinfo).uuid

            dataset = store.register_dataset(menu_id=menu_uuid, config_id=config_uuid)
            job_id = store.new_job(dataset.uuid)
            store.save_store()

            job = JobInfo_pb()
            job.name = "arrowproto"
            job.store_id = store.store_uuid
            job.store_name = store.store_name
            job.store_path = dirpath
            job.menu_id = menu_uuid
            job.config_id = config_uuid
            job.dataset_id = dataset.uuid
            job.parentset_id = g_dataset.uuid
            job.job_id = str(job_id)
            bow = Artemis(job, loglevel="INFO")
            bow.control()
            bow.gate.store.save_store()
            store = BaseObjectStore(dirpath, store.store_name, store_uuid=job.store_id)


if __name__ == "__main__":
    unittest.main()
