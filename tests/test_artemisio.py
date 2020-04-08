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
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
import tempfile
import uuid
import itertools
import urllib

import dask.delayed

from artemis.artemis import Artemis, ArtemisFactory
from artemis.core.singleton import Singleton
from artemis.core.gate import ArtemisGateSvc
from artemis.core.datastore import ArrowSets
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from cronus.core.cronus import BaseObjectStore
from artemis_format.pymodels.artemis_pb2 import JobInfo as JobInfo_pb
from artemis_format.pymodels.cronus_pb2 import MenuObjectInfo, ConfigObjectInfo
from artemis_format.pymodels.cronus_pb2 import (
    FileObjectInfo,
    TableObjectInfo,
    DatasetObjectInfo,
)
from artemis_format.pymodels.table_pb2 import Table
from artemis_format.pymodels.configuration_pb2 import Configuration

from artemis.distributed.job_builder import JobBuilder, runjob

logging.getLogger().setLevel(logging.INFO)

# Improve temporary outputs and context handling
# stackoverflow 3223604
def module_exists(module_name, object_name):
    try:
        __import__(module_name, fromlist=[object_name])
    except ImportError:
        print("fails to import %s" % object_name)
        return False
    else:
        print("imported module %s" % object_name)
        return True


class ArtemisTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)

    def test_fileio(self):
        """
        Write csv to disk
        Read back in artemis
        """
        with tempfile.TemporaryDirectory() as dirpath:
            mb = MenuFactory("csvgen")
            msgmenu = mb.build()
            menuinfo = MenuObjectInfo()
            menuinfo.created.GetCurrentTime()

            store = BaseObjectStore(dirpath, "artemis")

            config = JobConfigFactory(
                "csvio",
                msgmenu,
                jobname="arrowproto",
                generator_type="file",
                filehandler_type="csv",
                nbatches=1,
                num_rows=10000,
                max_file_size=1073741824,
                write_csv=True,
                # input_repo=dirpath,
                input_glob=".csv",
                # output_repo=dirpath
            )

            config.configure()
            config.add_algos(mb.algos)
            configinfo = ConfigObjectInfo()
            configinfo.created.GetCurrentTime()

            menu_uuid = store.register_content(msgmenu, menuinfo).uuid
            config_uuid = store.register_content(config._msg, configinfo).uuid

            g_dataset = store.register_dataset()
            store.new_partition(g_dataset.uuid, "generator")
            job_id = store.new_job(g_dataset.uuid)

            # define the schema for the data
            g_table = Table()
            g_table.name = "generator"
            g_table.uuid = str(uuid.uuid4())
            g_table.info.schema.name = "csv"
            g_table.info.schema.uuid = str(uuid.uuid4())

            fields = list(itertools.islice(GenCsvLikeArrow.generate_col_names(), 20))
            for f in fields:
                field = g_table.info.schema.info.fields.add()
                field.name = f

            tinfo = TableObjectInfo()
            tinfo.fields.extend(fields)
            store.register_content(
                g_table,
                tinfo,
                dataset_id=g_dataset.uuid,
                job_id=job_id,
                partition_key="generator",
            )

            generator = GenCsvLikeArrow(
                "generator",
                nbatches=1,
                num_cols=20,
                num_rows=10000,
                suffix=".csv",
                prefix="testio",
                path=dirpath,
                table_id=g_table.uuid,
            )

            generator.gate.meta.parentset_id = g_dataset.uuid
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            generator.write()

            dataset = store.register_dataset(menu_uuid, config_uuid)
            job_id = store.new_job(dataset.uuid)
            store.save_store()

            job = JobInfo_pb()
            job.name = "arrowproto"
            job.job_id = "example"
            job.store_path = dirpath
            job.store_id = store.store_uuid
            job.store_name = store.store_name
            job.menu_id = menu_uuid
            job.config_id = config_uuid
            job.dataset_id = dataset.uuid
            job.job_id = str(job_id)
            # print(job)
            bow = Artemis(job, loglevel="INFO")
            bow.control()

            # print(bow.gate.store[dataset.uuid])
            # print(bow.gate.store[g_dataset.uuid])

    def test_distributed(self):
        with tempfile.TemporaryDirectory() as dirpath:
            mb = MenuFactory("csvgen")
            msgmenu = mb.build()
            menuinfo = MenuObjectInfo()
            menuinfo.created.GetCurrentTime()

            store = BaseObjectStore(dirpath, "artemis")

            config = JobConfigFactory(
                "csvio",
                msgmenu,
                jobname="arrowproto",
                generator_type="file",
                filehandler_type="csv",
                nbatches=1,
                num_rows=10000,
                max_file_size=1073741824,
                write_csv=True,
                input_glob=".csv",
            )

            config.configure()
            config.add_algos(mb.algos)
            configinfo = ConfigObjectInfo()
            configinfo.created.GetCurrentTime()

            menu_uuid = store.register_content(msgmenu, menuinfo).uuid
            config_obj = store.register_content(config._msg, configinfo)
            config_uuid = config_obj.uuid

            g_dataset = store.register_dataset()
            store.new_partition(g_dataset.uuid, "generator")
            job_id = store.new_job(g_dataset.uuid)

            # define the schema for the data
            g_table = Table()
            g_table.name = "generator"
            g_table.uuid = str(uuid.uuid4())
            g_table.info.schema.name = "csv"
            g_table.info.schema.uuid = str(uuid.uuid4())

            fields = list(itertools.islice(GenCsvLikeArrow.generate_col_names(), 20))
            for f in fields:
                field = g_table.info.schema.info.fields.add()
                field.name = f

            tinfo = TableObjectInfo()
            tinfo.fields.extend(fields)
            store.register_content(
                g_table,
                tinfo,
                dataset_id=g_dataset.uuid,
                job_id=job_id,
                partition_key="generator",
            )

            generator = GenCsvLikeArrow(
                "generator",
                nbatches=10,
                num_cols=20,
                num_rows=1000,
                suffix=".csv",
                prefix="testio",
                path=dirpath,
                table_id=g_table.uuid,
            )

            generator.gate.meta.parentset_id = g_dataset.uuid
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            generator.write()

            dataset = store.register_dataset(menu_uuid, config_uuid)
            job_id = store.new_job(dataset.uuid)
            store.save_store()

            #######################################
            inputs = store.list(prefix=g_dataset.uuid, suffix="csv")

            store_name = store._name
            store_uuid = store.store_uuid
            dataset_uuid = dataset.uuid
            ds_results = []
            for datum in inputs:
                job_id = store.new_job(dataset.uuid)
                url_data = urllib.parse.urlparse(datum.address)
                dpath = urllib.parse.unquote(url_data.path)
                print(datum)
                config = Configuration()
                store.get(config_uuid, config)
                for p in config.input.generator.config.properties.property:
                    if p.name == "glob":
                        p.value = dpath.split(".")[-2] + ".csv"
                store._put_message(config_uuid, config)
                store.get(config_uuid, config)
                print(config)
                ds_results.append(
                    runjob(
                        dirpath,
                        store_name,
                        store_uuid,
                        menu_uuid,
                        config_uuid,
                        dataset_uuid,
                        g_dataset.uuid,
                        job_id,
                    )
                )

            results = dask.compute(*ds_results, scheduler="single-threaded")
            # Workaround to fix error in dataset merging
            store.new_partition(dataset.uuid, "seqY")
            # Update the dataset
            for buf in results:
                ds = DatasetObjectInfo()
                ds.ParseFromString(buf)
                store.update_dataset(dataset.uuid, buf)
            # Save the store, reload
            store.save_store()
            print(store[dataset.uuid].dataset)

    @unittest.skipUnless(
        module_exists("artemis.tools.fwftool", "FwfTool"), "mftool not installed"
    )
    def test_mfartemis(self):

        # IO tests are no longer required
        # generated data is written out in batches
        with tempfile.TemporaryDirectory() as dirpath:
            mb = MenuFactory("legacygen")
            msgmenu = mb.build()
            store = BaseObjectStore(dirpath, "artemis")
            menuinfo = MenuObjectInfo()
            menuinfo.created.GetCurrentTime()

            g_dataset = store.register_dataset()
            job_id = store.new_job(g_dataset.uuid)
            store.new_partition(g_dataset.uuid, "generator")
            # define the schema for the data
            # g_table = Table()
            # g_table.name = 'generator'
            # g_table.uuid = str(uuid.uuid4())
            # g_table.info.schema.name = 'csv'
            # g_table.info.schema.uuid = str(uuid.uuid4())

            intconf0 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
            intuconf0 = {"utype": "uint", "length": 6, "min_val": 0, "max_val": 10}
            strconf0 = {"utype": "str", "length": 4}
            # Schema definition.
            # Size of chunk to create.
            # Create a generator objected, properly configured.

            config = JobConfigFactory(
                "legacygen",
                msgmenu,
                generator_type="legacy",
                nbatches=1,
                num_rows=10000,
                header="header",
                header_offset=20,
                footer="footer",
                footer_size=20,
                delimiter="\r\n",
                nrecords_per_block=4095,
                max_file_size=1073741824,
                write_csv=True,
                output_repo=dirpath,
                skip_rows=0,
                column_names=["column_a", "column_b", "column_c"],
                field_widths=[10, 6, 4],
            )
            #  Passes the schema for new data layout
            config.configure(column_a=intconf0, column_b=intuconf0, column_c=strconf0)

            config.add_algos(mb.algos)

            configinfo = ConfigObjectInfo()
            configinfo.created.GetCurrentTime()

            menu_uuid = store.register_content(msgmenu, menuinfo).uuid
            config_uuid = store.register_content(config._msg, configinfo).uuid

            dataset = store.register_dataset(menu_id=menu_uuid, config_id=config_uuid)
            job_id = store.new_job(dataset.uuid)
            store.save_store()
            # msg = config.job_config  # Or retrieve from a DB

            #  For each subjob, requires a new JobInfo
            #  Driver application generates new JobInfo object per input datum
            #  All input metadata resides in memory
            #  Define menu and config
            #  Retrieve menu and config from either existing message or from DB
            #  Create new JobInfo message
            #  Set job properties
            #  Create instance of Artemis
            #  Update input and output repo properties before instance creatio
            #  Distribute processes
            #  Collect output messages
            #  Serialize and store

            msg = config.job_config
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
    test = ArtemisTestCase()
    # test.test_distributed()
    test.test_fileio()
