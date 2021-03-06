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

import unittest
import logging
import tempfile
import os
import uuid

from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.artemis import Artemis, ArtemisFactory
from artemis.core.gate import ArtemisGateSvc

from artemis.tools.mftool import MfTool

# from artemis.tools.fwftool import FwfTool
from artemis.generators.legacygen import GenMF

from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb

from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.cronus_pb2 import MenuObjectInfo, ConfigObjectInfo
from artemis.io.protobuf.cronus_pb2 import FileObjectInfo, TableObjectInfo

from artemis.io.protobuf.table_pb2 import Table

logging.getLogger().setLevel(logging.INFO)


def module_exists(module_name, object_name):
    try:
        __import__(module_name, fromlist=[object_name])
    except ImportError:
        print("fails to import %s" % object_name)
        return False
    else:
        print("imported module %s" % object_name)
        return True


def get_legacy_record_layout():
    fields = {
        "column_a": {"utype": "str", "length": 1},
        "column_b": {"utype": "uint", "length": 9, "min_val": 0, "max_val": 10},
        "column_c": {"utype": "str", "length": 2},
        "column_d": {"utype": "uint", "length": 4, "min_val": 0, "max_val": 10},
        "column_e": {"utype": "uint", "length": 9, "min_val": 0, "max_val": 10},
        "column_f": {"utype": "str", "length": 2},
        "column_g": {"utype": "uint", "length": 4, "min_val": 0, "max_val": 10},
        "column_h": {"utype": "uint", "length": 3, "min_val": 0, "max_val": 10},
        "column_i": {"utype": "str", "length": 10},  # date
        "column_j": {"utype": "uint", "length": 6, "min_val": 0, "max_val": 10},
        "column_k": {"utype": "int", "length": 8, "min_val": 0, "max_val": 10},
        "column_l": {"utype": "int", "length": 13, "min_val": 0, "max_val": 10},
        "column_m": {"utype": "int", "length": 13, "min_val": 0, "max_val": 10},
        "column_n": {"utype": "int", "length": 8, "min_val": 0, "max_val": 10},
        "column_o": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_p": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_q": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_r": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_s": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_t": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_u": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_v": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_x": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_y": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_z": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_aa": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ab": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ac": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ad": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ae": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_af": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ag": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ah": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ai": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_aj": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ak": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_al": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_am": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_an": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ao": {"utype": "str", "length": 10},  # date
        "column_ap": {"utype": "str", "length": 2},  # unknown
        "column_aq": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_au": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_av": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ax": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ay": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_az": {"utype": "str", "length": 10},  # date
        "column_ba": {"utype": "str", "length": 30},
        "column_bb": {"utype": "str", "length": 30},
        "column_bc": {"utype": "str", "length": 30},
        "column_bd": {"utype": "str", "length": 30},
        "column_be": {"utype": "str", "length": 30},
        "column_bf": {"utype": "str", "length": 27},
        "column_bg": {"utype": "str", "length": 2},
        "column_bh": {"utype": "str", "length": 2},
        "column_bi": {"utype": "str", "length": 9},
        "column_bj": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bk": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bl": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bm": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bn": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bo": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bp": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bq": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_br": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bs": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bt": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bu": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bv": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bx": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_by": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_bz": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_ca": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_cb": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_cc": {"utype": "uint", "length": 4, "min_val": 0, "max_val": 1},
        "column_cd": {"utype": "uint", "length": 2, "min_val": 0, "max_val": 1},
        "column_ce": {"utype": "int", "length": 13, "min_val": 0, "max_val": 10},
        "column_cf": {"utype": "int", "length": 13, "min_val": 0, "max_val": 10},
        "column_cg": {"utype": "int", "length": 13, "min_val": 0, "max_val": 10},
        "column_ch": {"utype": "int", "length": 13, "min_val": 0, "max_val": 10},
        "column_ci": {"utype": "int", "length": 14, "min_val": 0, "max_val": 10},
        "column_cj": {"utype": "int", "length": 14, "min_val": 0, "max_val": 10},
        "column_ck": {"utype": "int", "length": 9, "min_val": 0, "max_val": 10},
        "column_cl": {"utype": "int", "length": 9, "min_val": 0, "max_val": 10},
        "column_cm": {"utype": "int", "length": 8, "min_val": 0, "max_val": 10},
        "column_cn": {"utype": "uint", "length": 2, "min_val": 0, "max_val": 10},
        "column_co": {"utype": "uint", "length": 1, "min_val": 0, "max_val": 1},
        "column_cp": {"utype": "str", "length": 8},
    }  # Empty column padding 8 bytes
    return fields


class Test_MF_Reader(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)

    def tearDown(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)

    def test_mf_reader(self):
        """
        This test simply tests the reader function of the code.
        """

        # Field definitions.
        intconf0 = {"utype": "int", "length": 10}
        intconf1 = {"utype": "uint", "length": 6}
        strconf0 = {"utype": "str", "length": 4}
        # Schema definition for all fields.
        schema = [intconf0, strconf0, intconf1]
        # Test data block.
        block = (
            "012345678AABCD012345012345678BABCD012345"
            + "012345678CABC 012345012345678DABCD012345"
            + "012345678EABCD012345012345678FABCD012345"
            + "012345678AABC 012345012345678BABCD012345"
            + "012345678CABCD012345012345678DABCD012345"
            + "012345678EABC 012345012345678FABCD012345"
            + "012345678AABCD012345012345678BABCD012345"
            + "012345678CABC 012345"
        )
        # Show block in unencoded format.
        print("Block: ")
        print(block)
        # Encode in EBCDIC format.
        block = block.encode(encoding="cp500")
        # Show block in encoded format.
        print("Encoded block: ")
        print(block)
        # Create MfTool object. It is configured.
        mfreader = MfTool("reader", ds_schema=schema)
        # Run the reader on the data block.
        mfreader.execute(block)

    def test_mf_gen_read(self):
        """
        This test takes input from the mf data generator and
        feeds it to the mf data reader.
        """

        # Field definitions.
        intconf0 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intuconf0 = {"utype": "uint", "length": 6, "min_val": 0, "max_val": 10}
        strconf0 = {"utype": "str", "length": 4}
        # Schema definition.
        schema = [intconf0, intuconf0, strconf0]
        # Size of chunk to create.
        size = 10
        # Create a generator objected, properly configured.
        my_gen = GenMF("test", ds_schema=schema, num_rows=size, loglevel="INFO")
        # Create a data chunk.
        chunk = my_gen.gen_chunk()
        # Create MfTool object, properly configured.
        my_read = MfTool("reader", ds_schema=schema)
        # Read generated data chunk.
        batch = my_read.execute(chunk)
        print("Batch columns %i, rows %i" % (batch.num_columns, batch.num_rows))
        print(batch.schema)

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

    """
    def test_mfartemisio(self):
        mb = MenuFactory('legacygen')
        with tempfile.TemporaryDirectory() as dirpath:
            try:
                msgmenu = mb.build()
            except Exception:
                raise

            intconf0 = {'utype': 'int',
                        'length': 10, 'min_val': 0, 'max_val': 10}
            intuconf0 = {'utype': 'uint',
                         'length': 6, 'min_val': 0, 'max_val': 10}
            strconf0 = {'utype': 'str', 'length': 4}
            # Schema definition.
            # Size of chunk to create.
            # Create a generator objected, properly configured.
            generator = GenMF('generator',
                              column_a=intconf0,
                              column_b=intuconf0,
                              column_c=strconf0,
                              header='header',
                              header_offset=20,
                              footer='footer',
                              footer_size=20,
                              num_rows=10000,
                              nbatches=1,
                              suffix='.txt',
                              prefix='testio',
                              path=dirpath,
                              loglevel='INFO')

            generator.write()
            config = JobConfigFactory('legacyio', msgmenu,
                                      generator_type='file',
                                      filehandler_type='legacy',
                                      nbatches=1,
                                      num_rows=10000,
                                      delimiter='\r\n',
                                      nrecords_per_block=4095,
                                      max_file_size=1073741824,
                                      write_csv=True,
                                      input_repo=dirpath,
                                      input_glob='testio*.txt',
                                      output_repo=dirpath,
                                      seed=42
                                      )
            config.configure(column_a=intconf0,
                             column_b=intuconf0,
                             column_c=strconf0)

            msg = config.job_config

            job = JobInfo_pb()
            job.name = 'mftestio'
            job.job_id = 'example'
            job.output.repo = dirpath
            job.config.CopyFrom(msg)

            bow = ArtemisFactory(job, 'INFO')
            bow.control()
            
            nrecords = 0
            for table in bow.gate.meta.summary.tables:
                nrecords += table.num_rows

            assert(nrecords == 10000)

    def test_legacyds(self):

        mb = MenuFactory('legacygen')

        try:
            msgmenu = mb.build()
        except Exception:
            raise

        fields = get_legacy_record_layout()

        with tempfile.TemporaryDirectory() as dirpath:
            generator = GenMF('generator',
                              num_rows=10000, 
                              nbatches=1,
                              header='header',
                              header_offset=475,
                              footer='footer',
                              footer_size=475,
                              suffix='.txt',
                              prefix='legacyio',
                              path=dirpath,
                              loglevel='INFO',
                              seed=42,
                              **fields)
            generator.write()
            config = JobConfigFactory('legacyio', msgmenu,
                                      generator_type='file',
                                      filehandler_type='legacy',
                                      nbatches=1,
                                      num_rows=10000,
                                      delimiter='\r\n',
                                      nrecords_per_block=4095,
                                      max_file_size=1073741824,
                                      write_csv=True,
                                      input_repo=dirpath,
                                      input_glob='legacyio*.txt',
                                      output_repo=dirpath,
                                      seed=42
                                      )
            config.configure(**fields)

            msg = config.job_config

            job = JobInfo_pb()
            job.name = 'mftestio'
            job.job_id = 'example'
            job.output.repo = dirpath
            atom = job.input.atom
            atom.name = 'test'
            atom.repo = dirpath
            atom.glob = 'legacyio*.txt'
            job.config.CopyFrom(msg)
            bow = ArtemisFactory(job, 'INFO')
            bow.control()
            nrecords = 0
            for table in bow.gate.meta.summary.tables:
                nrecords += table.num_rows

            assert(nrecords == 10000)
    """


if __name__ == "__main__":
    unittest.main()
