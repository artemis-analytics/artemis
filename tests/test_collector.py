#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
import tempfile
from collections import OrderedDict

from artemis.io.collector import Collector
from artemis.core.dag import Sequence, Menu, Chain
from artemis.core.tree import Tree, Node, Element
from artemis.core.datastore import ArrowSets
from artemis.core.singleton import Singleton
from artemis.core.tool import ToolStore
from artemis.io.writer import BufferOutputWriter 
from artemis.core.properties import JobProperties
import pyarrow as pa
from pyarrow.csv import read_csv
logging.getLogger().setLevel(logging.INFO)


class CollectorTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        Singleton.reset(ToolStore)
        Singleton.reset(JobProperties)
        seq1 = Sequence(["initial"], ("alg1", "alg2"), "seq1")
        seq2 = Sequence(["initial"], ("alg1", "alg2"), "seq2")
        seq3 = Sequence(["seq1", "seq2"], ("alg3",), "seq3")
        seq4 = Sequence(["seq3"], ("alg4",), "seq4")
        dummyChain1 = Chain("dummy1")
        dummyChain1.add(seq1)
        dummyChain1.add(seq4)
        dummyChain1.add(seq3)
        dummyChain1.add(seq2)
        dummyChain1.build()

        testmenu = Menu("test")
        testmenu.add(dummyChain1)
        testmenu.generate()
        msg = testmenu.to_msg()
        
        test_tree = Tree('My_test_tree')
        tree_menu = OrderedDict()

        for node in msg.tree.nodes:
            # Create the nodes for the tree
            if node.name == 'initial':
                test_tree.root = Node(node.name, [])
                test_tree.add_node(test_tree.root)
            else:
                test_tree.add_node(Node(node.name, node.parents))
        test_tree.update_parents()
        test_tree.update_leaves()



    def tearDown(self):
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        pass
    
    def test_initialize(self):
        '''
        Create menu
        Convert to msg
        Build tree from msg
        Add ipc msg to tree element payload
        '''
       
        
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
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])

        collector = Collector('collect', job_id='job', path='')
        with self.assertRaises(KeyError):
                collector.initialize()

        writer = BufferOutputWriter('bufferwriter')
        msg = JobProperties().meta.config.tools.add()
        msg.CopyFrom(writer.to_msg())
        collector.initialize()

        for leaf in Tree().leaves:
            name = ToolStore().get('writer_'+leaf).name
            self.assertEqual(name, 'writer_'+leaf)

        self.assertEqual(ArrowSets().is_empty(), True)

    def test_collect(self):
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
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])

        collector = Collector('collect', job_id='job', path='')

        writer = BufferOutputWriter('bufferwriter')
        msg = JobProperties().meta.config.tools.add()
        msg.CopyFrom(writer.to_msg())
        collector.initialize()
        with self.assertRaises(IndexError):
            collector._collect()
        
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])
        
        collector._collect()
        self.assertEqual(ArrowSets().is_empty(), True)

        rows = b"d,e,f\n1,2,3\n4,5,6\n"
        table = read_csv(pa.py_buffer(rows))
        batches = table.to_batches()
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])

        with self.assertRaises(ValueError):
            collector._collect()
    
    def test_execute(self):
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
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])

        collector = Collector('collect', job_id='job', path='', max_malloc=0)

        writer = BufferOutputWriter('bufferwriter')
        msg = JobProperties().meta.config.tools.add()
        msg.CopyFrom(writer.to_msg())
        collector.initialize()
        with self.assertRaises(IndexError):
            collector.execute()
        
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])
        
        collector._collect()
        self.assertEqual(ArrowSets().is_empty(), True)

        rows = b"d,e,f\n1,2,3\n4,5,6\n"
        table = read_csv(pa.py_buffer(rows))
        batches = table.to_batches()
        for leaf in Tree().leaves:
            Tree().nodes[leaf].payload.append(Element('test'))
            Tree().nodes[leaf].payload[-1].add_data(batches[-1])

        with self.assertRaises(ValueError):
            collector.execute()


if __name__ == "__main__":
    unittest.main()
