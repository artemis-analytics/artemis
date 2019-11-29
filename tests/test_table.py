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

import os
import unittest
import logging
import tempfile
import os, shutil

from artemis.io.protobuf.table_pb2 import Table


class TableTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_table(self):
        table = Table()
        table.name = 'Attachment'
        #table.uuid = str(uuid.uuid4())

        schema = table.info.schema.info
        schema.aux.frequency = 3
        schema.aux.description = "This table is for ..."

        field1 = schema.fields.add()
        field1.name = 'record_id'
        field1.info.type = 'String'
        field1.info.length = 10

        field2 = schema.fields.add()
        field2.name = 'field2'
        field2.info.type = 'String'
        field2.info.length = 20
        aux2 = field2.info.aux
        aux2.generator.name = 'name'
        aux2.meta['Bool1'].bool_val = True
        aux2.meta['Bool2'].bool_val = False
        aux2.meta['String1'].string_val = 'System'
        aux2.description = 'Blah'

        field3 = schema.fields.add()
        field3.name = 'fieldl3'
        field3.info.type = 'String'
        field3.info.length = 24
        aux3 = field3.info.aux
        aux3.generator.name = 'province'
        code = aux3.codeset
        code.name = "Codeset Name"
        code.version = "2016VR1"
        value1 = code.codevalues.add()
        value1.code = "1A"
        value1.description = "what 1a stands for"
        value2 = code.codevalues.add()
        value2.code = "2A"
        value2.description = "What 2a stands for"
        value2.lable = 'lable for 2a'
        aux3.meta['Bool1'].bool_val = True
        aux3.meta['Bool2'].bool_val = True
        aux3.description = 'Blah blah blah'
        aux3.meta['String1'].string_val = 'Rule for variable population'

        tem2 = table.SerializeToString()
        print(tem2)
        table2 = Table()
        table2.ParseFromString(tem2)
        print(table2)

if __name__ == '__main__':
    unittest.main()
