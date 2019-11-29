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
import unittest
import logging
import tempfile
from collections import OrderedDict

from artemis.io.collector import Collector
from artemis.core.tree import Tree, Node, Element
from artemis.core.datastore import ArrowSets
from artemis.core.singleton import Singleton
from artemis.io.writer import BufferOutputWriter 
from artemis.core.gate import ArtemisGateSvc 
from artemis.meta.cronus import BaseObjectStore
from artemis.meta.Directed_Graph import Directed_Graph
from artemis.meta.Directed_Graph import GraphMenu 
from artemis.meta.Directed_Graph import Node as Node_pb2

import pyarrow as pa
from pyarrow.csv import read_csv
logging.getLogger().setLevel(logging.INFO)


class CollectorTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        Singleton.reset(ArrowSets)
        Singleton.reset(ArtemisGateSvc)
    
    def getmenu(self):
        seq1 = Node_pb2(["initial"], ("alg1", "alg2"), "seq1")
        seq2 = Node_pb2(["initial"], ("alg1", "alg2"), "seq2")
        seq3 = Node_pb2(["seq1", "seq2"], ("alg3",), "seq3")
        seq4 = Node_pb2(["seq3"], ("alg4",), "seq4")
        dummyChain1 = Directed_Graph("dummy1")
        dummyChain1.add(seq1)
        dummyChain1.add(seq4)
        dummyChain1.add(seq3)
        dummyChain1.add(seq2)
        dummyChain1.build()

        testmenu = GraphMenu("test")
        testmenu.add(dummyChain1)
        testmenu.build()
        msg = testmenu.to_msg()
        
        test_tree = Tree('My_test_tree')
        tree_menu = OrderedDict()
        
        for graph in msg.graphs:
            for node in graph.nodes:
                # Create the nodes for the tree
                if node.name == 'initial':
                    test_tree.root = Node(node.name, [])
                    test_tree.add_node(test_tree.root)
                else:
                    test_tree.add_node(Node(node.name, node.parents))
        test_tree.update_parents()
        test_tree.update_leaves()
        jp = ArtemisGateSvc() 
        jp.tree = test_tree

    def setupStore(self, dirpath):
        store = BaseObjectStore(dirpath, 'artemis')
        g_dataset = store.register_dataset()
        store.new_partition(g_dataset.uuid, 'generator')
        job_id = store.new_job(g_dataset.uuid)
        
        return store, g_dataset.uuid, job_id 


    def tearDown(self):
        Singleton.reset(ArrowSets)
        Singleton.reset(ArtemisGateSvc)

        pass
    
    def test_initialize(self):
        '''
        Create menu
        Convert to msg
        Build tree from msg
        Add ipc msg to tree element payload
        '''
         
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id = self.setupStore(dirpath)
            self.getmenu()        
            jp = ArtemisGateSvc() 
            jp.store = store
            jp.meta.dataset_id = ds_id
            
            rows = b"a,b,c\n1,2,3\n4,5,6\n"
            table = read_csv(pa.py_buffer(rows))
            schema = pa.schema([('a', pa.int64()),
                                ('b', pa.int64()),
                                ('c', pa.int64())])
            assert table.schema == schema
            assert table.to_pydict() == {
                'a': [1, 4],
                'b': [2, 5],
                'c': [3, 6],
                }
            batches = table.to_batches()
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])

            collector = Collector('collect', job_id=job_id, path='')
            writer = BufferOutputWriter('bufferwriter')
            jp.config.tools[writer.name].CopyFrom(writer.to_msg())
            collector.initialize()

            for leaf in jp.tree.leaves:
                name = jp.tools.get('writer_'+leaf).name
                self.assertEqual(name, 'writer_'+leaf)

            self.assertEqual(ArrowSets().is_empty(), True)

    def test_collect(self):
        
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id = self.setupStore(dirpath)
            self.getmenu() 
            jp = ArtemisGateSvc() 
            jp.store = store
            jp.meta.dataset_id = ds_id
            rows = b"a,b,c\n1,2,3\n4,5,6\n"
            table = read_csv(pa.py_buffer(rows))
            schema = pa.schema([('a', pa.int64()),
                                ('b', pa.int64()),
                                ('c', pa.int64())])
            assert table.schema == schema
            assert table.to_pydict() == {
                'a': [1, 4],
                'b': [2, 5],
                'c': [3, 6],
                }
            batches = table.to_batches()
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])

            collector = Collector('collect', job_id='job', path='')

            writer = BufferOutputWriter('bufferwriter')
            jp.config.tools[writer.name].CopyFrom(writer.to_msg())
            collector.initialize()
            with self.assertRaises(IndexError):
                collector._collect()
            
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])
            
            collector._collect()
            self.assertEqual(ArrowSets().is_empty(), True)

            rows = b"d,e,f\n1,2,3\n4,5,6\n"
            table = read_csv(pa.py_buffer(rows))
            batches = table.to_batches()
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])

            with self.assertRaises(ValueError):
                collector._collect()
    
    def test_execute(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id = self.setupStore(dirpath)
            self.getmenu()
            jp = ArtemisGateSvc() 
            jp.store = store
            jp.meta.dataset_id = ds_id
        
            rows = b"a,b,c\n1,2,3\n4,5,6\n"
            table = read_csv(pa.py_buffer(rows))
            schema = pa.schema([('a', pa.int64()),
                                ('b', pa.int64()),
                                ('c', pa.int64())])
            assert table.schema == schema
            assert table.to_pydict() == {
                'a': [1, 4],
                'b': [2, 5],
                'c': [3, 6],
                }
            batches = table.to_batches()
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])

            collector = Collector('collect', job_id='job', path='', max_malloc=0)

            writer = BufferOutputWriter('bufferwriter')
            jp.config.tools[writer.name].CopyFrom(writer.to_msg())
            collector.initialize()
            with self.assertRaises(IndexError):
                collector.execute()
            
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])
            
            collector._collect()
            self.assertEqual(ArrowSets().is_empty(), True)

            rows = b"d,e,f\n1,2,3\n4,5,6\n"
            table = read_csv(pa.py_buffer(rows))
            batches = table.to_batches()
            for leaf in jp.tree.leaves:
                jp.tree.nodes[leaf].payload.append(Element('test'))
                jp.tree.nodes[leaf].payload[-1].add_data(batches[-1])

            with self.assertRaises(ValueError):
                collector.execute()


if __name__ == "__main__":
    unittest.main()
