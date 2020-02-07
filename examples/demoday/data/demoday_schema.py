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
import os
import uuid
import logging
# from collections import OrderedDict
from artemis.artemis import Artemis, ArtemisFactory
from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.cronus_pb2 import MenuObjectInfo, ConfigObjectInfo, \
    FileObjectInfo, TableObjectInfo
from artemis.io.protobuf.table_pb2 import Table
from artemis.generators.simutable.synthesizer import Synthesizer
from artemis.io.protobuf.simutable_pb2 import SimuTable
from artemis.generators.simutablegen import SimuTableGen
from artemis.io.protobuf.table_pb2 import Table

from artemis.core.singleton import Singleton
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.core.gate import ArtemisGateSvc 
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb


logging.getLogger().setLevel(logging.INFO)


class SimuTableTestCase(unittest.TestCase):

    def test(self):
        model = Table()
        model.name = 'SalesData'
        schema = model.info.schema.info
        field = schema.fields.add()
        field.name = 'Name'
        field.info.type = 'String'
        field.info.length = 10
        field.info.aux.generator.name = 'name'
        print(model)

    def test_gen_from_proto(self):

        model = Table()
        model.name = 'SalesData'
        schema = model.info.schema.info
        field = schema.fields.add()
        field.name = 'Name'
        field.info.type = 'String'
        field.info.length = 10
        field.info.aux.generator.name = 'name'

        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        print(s2.generate())

    def test_simutablegen(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store = BaseObjectStore(dirpath, 'artemis')
            
            g_dataset = store.register_dataset()
            store.new_partition(g_dataset.uuid, 'generator')
            job_id = store.new_job(g_dataset.uuid)
            
            # define the schema for the data
            g_table = Table()
            g_table.name = 'SalesData'
            g_table.uuid = str(uuid.uuid4())
            schema = g_table.info.schema.info
            field = schema.fields.add()
            field.name = 'Name'
            field.info.type = 'String'
            field.info.length = 10
            field.info.aux.generator.name = 'name'
            
            tinfo = TableObjectInfo()
            store.register_content(g_table, 
                                   tinfo, 
                                   dataset_id=g_dataset.uuid,
                                   job_id=job_id,
                                   partition_key='generator')

            generator = SimuTableGen('generator',
                                     nbatches=1,
                                     num_rows=10000,
                                     file_type=1,
                                     table_id=g_table.uuid)

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
            mb = MenuFactory('csvgen')
            msgmenu = mb.build()
            menuinfo = MenuObjectInfo()
            menuinfo.created.GetCurrentTime()
                
            store = BaseObjectStore(dirpath, 'artemis')
            
            g_dataset = store.register_dataset()
            store.new_partition(g_dataset.uuid, 'generator')
            job_id = store.new_job(g_dataset.uuid)
            
            # define the schema for the data
            g_table = Table()
            g_table.name = 'SalesData'
            g_table.uuid = str(uuid.uuid4())
            schema = g_table.info.schema.info
                
            field1 = schema.fields.add()
            field1.name = 'record_id'
            field1.info.type = 'String'
            field1.info.length = 10

            field2 = schema.fields.add()
            field2.name = 'Name'
            field2.info.type = 'String'
            field2.info.length = 10
            field2.info.aux.generator.name = 'name'

            field3 = schema.fields.add()
            field3.name = 'StreetNumber'
            field3.info.type = 'String'
            field3.info.length = 40
            field3.info.aux.generator.name = 'building_number'

            field4 = schema.fields.add()
            field4.name = 'Street'
            field4.info.type = 'String'
            field4.info.length = 40
            field4.info.aux.generator.name = 'street_name'

            field5 = schema.fields.add()
            field5.name = 'City'
            field5.info.type = 'String'
            field5.info.length = 40
            field5.info.aux.generator.name = 'city'

            field6 = schema.fields.add()
            field6.name = 'Province'
            field6.info.type = 'String'
            field6.info.length = 40
            field6.info.aux.generator.name = 'province'

            field7 = schema.fields.add()
            field7.name = 'PostalCode'
            field7.info.type = 'String'
            field7.info.length = 40
            field7.info.aux.generator.name = 'postcode'

            field8 = schema.fields.add()
            field8.name = 'PhoneNum'
            field8.info.type = 'String'
            field8.info.length = 11
            field8.info.aux.generator.name = 'phone_number'
            
            field9 = schema.fields.add()
            field9.name = 'Product'
            field9.info.type = 'String'
            field9.info.length = 20
            field9.info.aux.generator.name = 'name'
            
            field10 = schema.fields.add()
            field10.name = 'Desc0'
            field10.info.type = 'String'
            field10.info.length = 100
            field10.info.aux.generator.name = 'name'
            
            field11 = schema.fields.add()
            field11.name = 'Desc1'
            field11.info.type = 'String'
            field11.info.length = 100
            field11.info.aux.generator.name = 'name'
            
            field12 = schema.fields.add()
            field12.name = 'Desc2'
            field12.info.type = 'String'
            field12.info.length = 100
            field12.info.aux.generator.name = 'name'
            
            field13 = schema.fields.add()
            field13.name = 'Unit'
            field13.info.type = 'int'
            field13.info.length = 10
            field13.info.aux.generator.name = 'random_int'
            
            field14 = schema.fields.add()
            field14.name = 'SaleValue'
            field14.info.type = 'float'
            field14.info.length = 100
            field14.info.aux.generator.name = 'random_int'
            
            field15 = schema.fields.add()
            field15.name = 'UnitPrice'
            field15.info.type = 'float'
            field15.info.length = 10
            field15.info.aux.generator.name = 'random_int'
            
            field16 = schema.fields.add()
            field16.name = 'ProductCode'
            field16.info.type = 'int'
            field16.info.length = 13
            field16.info.aux.generator.name = 'ean'
            
            tinfo = TableObjectInfo()
            store.register_content(g_table, 
                                   tinfo, 
                                   dataset_id=g_dataset.uuid,
                                   job_id=job_id,
                                   partition_key='generator')

            store.save_store()
            config = JobConfigFactory('csvgen', msgmenu,
                                      jobname='arrowproto',
                                      generator_type='simutable',
                                      filehandler_type='csv',
                                      nbatches=10,
                                      #num_cols=20,
                                      num_rows=10000,
                                      table_id=g_table.uuid,
                                      linesep='\r\n',
                                      delimiter=",",
                                      max_buffer_size=10485760,
                                      max_malloc=2147483648,
                                      write_csv=True,
                                      output_repo=dirpath,
                                      seed=42
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

            msg = config.job_config
            job = JobInfo_pb()
            job.name = 'arrowproto'
            job.store_id = store.store_uuid
            job.store_name = store.store_name
            job.store_path = dirpath
            job.menu_id = menu_uuid
            job.config_id = config_uuid
            job.dataset_id = dataset.uuid
            job.parentset_id = g_dataset.uuid
            job.job_id = str(job_id) 
            bow = Artemis(job, loglevel='INFO')
            bow.control()
            bow.gate.store.save_store()
            store = BaseObjectStore(dirpath, 
                                    store.store_name, 
                                    store_uuid=job.store_id)
    
    def test_glm_proto(self):
        model = Table()
        schema = model.info.schema.info
        field1 = schema.fields.add()
        field1.name = 'Value1'
        field1.info.type = 'Float'
        field1.info.length = 10
        field1.info.aux.generator.name = 'random_int'
        field1.info.aux.dependent = 'Prediction'

        field2 = schema.fields.add()
        field2.name = 'Value2'
        field2.info.type = 'Float'
        field2.info.length = 10
        field2.info.aux.generator.name = 'random_int'
        field2.info.aux.dependent = 'Prediction'

        field3 = schema.fields.add()
        field3.name = 'Prediction'
        field3.info.type = 'Float'
        field3.info.length = 10
        field3.info.aux.generator.name = 'glm'

        beta1 = field3.info.aux.generator.parameters.add()
        beta1.name = 'beta1'
        beta1.value = 10
        beta1.type = 'int'
        beta2 = field3.info.aux.generator.parameters.add()
        beta2.name = 'beta2'
        beta2.value = 0.1
        beta2.type = 'float'
        beta3 = field3.info.aux.generator.parameters.add()
        beta3.name = 'beta3'
        beta3.value = 100
        beta3.type = 'int'
        sigma = field3.info.aux.generator.parameters.add()
        sigma.name = 'sigma'
        sigma.value = 1
        sigma.type = 'int'

        var1 = field3.info.aux.generator.parameters.add()
        var1.name = 'Value1'
        var1.type = 'Field'
        var1.variable.CopyFrom(field1)

        var2 = field3.info.aux.generator.parameters.add()
        var2.name = 'Value2'
        var2.type = 'Field'
        var2.variable.CopyFrom(field2)

        s2 = Synthesizer(model, 'en_CA')
        print(s2.generate())
    
    def test_xduplicates(self):

        model = Table()

        model.info.aux.duplicate.probability = 1
        model.info.aux.duplicate.distribution = 'uniform'
        model.info.aux.duplicate.maximum = 1
        schema = model.info.schema.info
        
        field1 = schema.fields.add()
        field1.name = 'record_id'
        field1.info.type = 'String'
        field1.info.length = 10

        field2 = schema.fields.add()
        field2.name = 'Name'
        field2.info.type = 'String'
        field2.info.length = 10
        field2.info.aux.generator.name = 'name'

        field3 = schema.fields.add()
        field3.name = 'UPC'
        field3.info.type = 'Integer'
        field3.info.length = 13
        field3.info.aux.generator.name = 'ean'

        parm = field3.info.aux.generator.parameters.add()
        parm.name = 'ndigits'
        parm.value = 13
        parm.type = 'int'

        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        print(s2.generate())
    
    def test_xmodifer(self):

        model = Table()
        schema = model.info.schema.info
        
        field1 = schema.fields.add()
        field1.name = 'record_id'
        field1.info.type = 'String'
        field1.info.length = 10

        field2 = schema.fields.add()
        field2.name = 'Name'
        field2.info.type = 'String'
        field2.info.length = 10
        field2.info.aux.generator.name = 'name'

        field3 = schema.fields.add()
        field3.name = 'SIN' 
        field3.info.type = 'String'
        field3.info.length = 10
        field3.info.aux.generator.name = 'ssn'

        field4 = schema.fields.add()
        field4.name = 'StreetNumber'
        field4.info.type = 'String'
        field4.info.length = 40
        field4.info.aux.generator.name = 'building_number'

        field5 = schema.fields.add()
        field5.name = 'Street'
        field5.info.type = 'String'
        field5.info.length = 40
        field5.info.aux.generator.name = 'street_name'

        field6 = schema.fields.add()
        field6.name = 'City'
        field6.info.type = 'String'
        field6.info.length = 40
        field6.info.aux.generator.name = 'city'

        field7 = schema.fields.add()
        field7.name = 'Province'
        field7.info.type = 'String'
        field7.info.length = 40
        field7.info.aux.generator.name = 'province'

        field8 = schema.fields.add()
        field8.name = 'PostalCode'
        field8.info.type = 'String'
        field8.info.length = 40
        field8.info.aux.generator.name = 'postcode'

        field9 = schema.fields.add()
        field9.name = 'DOB'
        field9.info.type = 'DateTime'
        field9.info.length = 40
        field9.info.aux.generator.name = 'date'

        field10 = schema.fields.add()
        field10.name = 'PhoneNum'
        field10.info.type = 'String'
        field10.info.length = 11
        field10.info.aux.generator.name = 'phone_number'

        model.info.aux.duplicate.probability = 1
        model.info.aux.duplicate.distribution = 'uniform'
        model.info.aux.duplicate.maximum = 5

        modifier = model.info.aux.record_modifier

        modifier.max_modifications_in_record = 1
        modifier.max_field_modifiers = 1
        modifier.max_record_modifiers = 1

        name_mod = modifier.fields.add()
        name_mod.selection = 0.1
        name_mod.name = 'Name'
        prob = name_mod.probabilities

        prob.insert = 0.1  # insert character in field
        prob.delete = 0.1  # delete character in field
        prob.substitute = 0.1  # substitute character in field
        prob.misspell = 0.  # use mispelling dictionary
        prob.transpose = 0.1  # transpose adjacent characters
        prob.replace = 0.1  # replace with another value of same fake
        prob.swap = 0.1  # swap two words/values in field
        prob.split = 0.1  # split a field
        prob.merge = 0.1  # merge a field
        prob.nullify = 0.1  # convert to null
        prob.fill = 0.1  # fill empty field with expected type

        street_mod = modifier.fields.add()
        street_mod.selection = 0.9
        street_mod.name = 'Street'
        prob2 = street_mod.probabilities

        prob2.insert = 0.1  # insert character in field
        prob2.delete = 0.1  # delete character in field
        prob2.substitute = 0.1  # substitute character in field
        prob2.misspell = 0.  # use mispelling dictionary
        prob2.transpose = 0.1  # transpose adjacent characters
        prob2.replace = 0.1  # replace with another value of same fake
        prob2.swap = 0.1  # swap two words/values in field
        prob2.split = 0.1  # split a field
        prob2.merge = 0.1  # merge a field
        prob2.nullify = 0.1  # convert to null
        prob2.fill = 0.1  # fill empty field with expected type
        s2 = Synthesizer(model, 'en_CA', idx=0, seed=4053)
        protorows = []
        for _ in range(10):
            protorows.append(s2.generate())
        print(protorows) 
    

if __name__ == '__main__':
    print('Unit Test: Faker')
    unittest.main()
    #test = SimuTableTestCase()
    #test.test_simutable_artemis()
    print('====================================')
