#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

import unittest

from artemis.tools.xlstool import XlsTool
from artemis.io.protobuf.cronus_pb2 import DatasetObjectInfo as Dataset
from artemis.io.protobuf.table_pb2 import Table

class Test_XLSReader(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
    
    def tearDown(self):
        pass

    def test_excel(self):
        # defines protobuf
        # read in excel and check if two instances are =
        location = './tests/data/Dataset_metastore.xlsx'
        inst = XlsTool('tool', location = location)
        ds = inst.execute(location)

        # Explicit definition
        d = Dataset()
        dh = d.aux.data_holding
        da = d.aux.data_asset
        dhd = dh.data_holding_detail
        pa = dh.provision_agreement

        dh.name = 'example dataset'
        dh.description = 'has 2 tables'
        dh.program_element = 'something'
        dh.sensitive_statistical_info = False
        dh.has_personal_identifiers = True
        dh.has_other_supporting_documentation = False
        dh.expected_medium.extend(['medium 1 ', 'medium 2', 'medium 3'])
        dh.usage.extend(('usage 1', 'usage 2'))
        dh.permission = 'part of stat can'
        dh.provider = 'another division'
        dh.provider_type = 1

        da.description = ''
        da.reference_period = 'a time'
        da.granularity_type = 'a type'
        da.state = 'a state'
        da.data_asset_category = 'a category'
        dr = da.data_retention

        dr.description = 'blah blah'
        dr.period = 'a period'
        dr.retention_trigger_date = 'a date'
        dr.retention_trigger = 'a trigger type'
        dr.type = 'a retention type'

        dhd.receptionFrequency = 'monthly'
        dhd.acquisition_stage = 'a stage'
        dhd.acquisition_cost = 1.11
        dhd. quality_evaluation_done_on_input = False

        pa.channel = 'a channel type'
        pa.statcan_act_section.append('section 12')
        pa.channel_detail = 'something something'
        pa.data_usage_type = 'usage type'
        pa.data_acquisition_type = 'acquisition type'

        t1 = Table()
        t1.name = 'example table 1'
        t1.info.schema.name = 'schema 1'
        s1 = t1.info.schema.info
        s1.aux.description = 'this is a description for a table'
        
        f1 = s1.fields.add()
        f1.name = 'field1'
        f1.info.type = 'string'
        f1.info.length = 10
        f1.info.nullable = False
        f1.info.aux.description = 'this is field 1'
        c1 = f1.info.aux.codeset
        c1.name = 'set 1'
        c1.version = '2016v1'
        for i in range(2):
            for j in ['a','b']:
                temp = c1.codevalues.add()
                temp.code = str(i+1) + j
                temp.description = str(i+1) + j + ' stands for this'
                temp.lable = 'lable' + str(i+1) + j
        f1.info.aux.meta['Info 1'].description = 'this metadata is used for'
        f1.info.aux.meta['Info 1'].bool_val = True

        f2 = s1.fields.add()
        f2.name = 'field2'
        f2.info.type = 'int'
        f2.info.length = 3
        f2.info.nullable = True
        f2.info.aux.description = 'field 2 description'
        c2 = f2.info.aux.codeset
        c2.name = 'set 2'
        c2.version = 'version 2'
        for i in range(4):
            temp = c2.codevalues.add()
            temp.code = str(i)
            temp.description = 'means this'
            temp.lable = 'lable' + str(i)
        f2.info.aux.meta['Info 1'].description = 'this metadata is used for'
        f2.info.aux.meta['Info 1'].bool_val = False
        
        ds.dataset.aux.data_asset.ClearField('creation_time') # Cannot compare current time
        print ("=======Dataset=======")
        print (ds.dataset)
        print ("=======Tables=======")
        print (ds.tables)
        self.assertEqual(ds.dataset, d)
        self.assertEqual(ds.tables[0], t1)

if __name__ == '__main__':
    unittest.main()