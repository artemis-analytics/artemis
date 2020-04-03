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
import os, shutil
import sys
from pathlib import Path

from artemis.meta.cronus import BaseObjectStore, JobBuilder
from artemis.meta.Directed_Graph import Directed_Graph, Node, GraphMenu

from google.protobuf import text_format


class GraphTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        logging.getLogger().setLevel(logging.INFO)

    def tearDown(self):
        pass

    def test_sequence_node(self):
        node1 = Node(["initial"], ("alg1", "alg2"), "node1")
        node2 = Node(["initial", "node1"], ("alg1", "alg2"), "node2")
        node3 = Node(["initial", "node2"], ("alg1", "alg2"), "node3")
        node4 = Node(["initial", "node3"], ("alg1", "alg2"), "node4")

        print(node1.__str__())
        print(node2.__str__())
        print(node3.__str__())
        print(node4.__str__())

    def test_sequence_graph(self):
        node1 = Node(["initial"], ("genericalg"), "node1")
        node2 = Node(["initial", "node1"], ("genericalg"), "node2")
        node3 = Node(["initial", "node2", "node1", "node5"], ("genericalg"), "node3")
        node4 = Node(["initial", "node3"], ("genericalg"), "node4")
        node5 = Node(["initial", "node2"], ("genericalg"), "node5")
        node6 = Node(
            ["node1", "node2", "node3", "node4", "node5"], ("genericalg"), "node6"
        )
        node7 = Node(
            ["node1", "node2", "node3", "node4", "node5"], ("genericalg"), "node7"
        )
        node8 = Node(["initial", "node2"], ("genericalg"), "node8")
        node9 = Node(["initial", "node2"], ("genericalg"), "node9")

        print(node1.__str__())
        print(node2.__str__())
        print(node3.__str__())
        print(node4.__str__())
        print(node5.__str__())
        print(node6.__str__())
        print(node7.__str__())
        print(node8.__str__())
        print(node9.__str__())

        graph1 = Directed_Graph("graph1")
        graph1.add(node1)
        graph1.add(node2)
        graph1.add(node3)
        graph1.add(node4)
        graph1.add(node5)
        graph1.add(node6)
        graph1.add(node7)
        graph1.add(node8)
        graph1.add(node9)

        for node in graph1.nodes:
            print(node.id)

        print(graph1.build())
        # graph1.create_vis(terminal_print = False)
        graph1.create_vis()
        # print("LEAVES")
        # print(str(graph1.get_leaves()))
        # print("LEAVES")

        print("SORTED NODES")
        for node in graph1.nodes:
            print(node.id)
        print("SORTED NODES")

        """
        print("INTERNAL GRAPH")
        print(graph1.internal_graph())
        print("INTERNAL GRAPH")
        """

    def test_sequence_menu(self):
        """
        for this test we will make two dummy/test Directed_graphs
        these graphs will then be added to the Menu which will then be 
        """

        node1 = Node(["initial"], ("genericalg"), "node1")
        node2 = Node(["initial"], ("genericalg"), "node2")
        node3 = Node(["node1", "node2"], ("genericalg"), "node3")
        node4 = Node(["node3"], ("genericalg"), "node4")

        test_graph1 = Directed_Graph("graph1")
        test_graph1.add(node1)
        test_graph1.add(node2)
        test_graph1.add(node3)
        test_graph1.add(node4)

        # build the first test graph
        test_graph1.build()

        node5 = Node(["initial"], ("genericalg"), "node5")
        node6 = Node(["node5"], ("genericalg"), "node6")
        node7 = Node(["node6"], ("genericalg"), "node7")

        test_graph2 = Directed_Graph("graph2")
        test_graph2.add(node5)
        test_graph2.add(node6)
        test_graph2.add(node7)

        # build the second test graph
        test_graph2.build()

        print("===========X===============")
        print(test_graph1.leaf)
        print(test_graph2.leaf)
        nodeX = Node([test_graph1, test_graph2, "initial"], ("genericalg"), "nodeX")
        print(nodeX)
        graphX = Directed_Graph("graphX", [test_graph1, test_graph2])
        graphX.add(nodeX)
        graphX.build()
        print("===========Nodes===============")
        for node in graphX.nodes:
            print(node)
        print("===========Nodes===============")
        # graphX.create_vis()
        print("===========X===============")

        print("===========Y===============")
        print(graphX.leaf)
        nodeY = Node([graphX, "initial"], ("genericalg"), "nodeY")
        print(nodeY)
        graphY = Directed_Graph("graphY", [graphX])
        graphY.add(nodeY)
        graphY.build()
        print("===========Nodes===============")
        for node in graphY.nodes:
            print(node)
        print("===========Nodes===============")
        # graphY.create_vis()
        print("===========Y===============")

        test_menu = GraphMenu("test")
        test_menu.add(test_graph1)
        test_menu.add(test_graph2)
        test_menu.add(graphX)
        test_menu.add(graphY)

        print("===========Building menu===============")
        test_menu.build()
        print("===========Building menu===============")

        # print("===========Creating visual===============")
        # test_menu.create_vis()
        # print("===========Creating visual===============")

        msg = test_menu.to_msg()
        print(text_format.MessageToBytes(msg))
        print(text_format.MessageToString(msg))

        test_menu2 = GraphMenu("test1")
        test_menu2.to_menu_from_msg(msg)
        test_menu2.create_vis(terminal_print=True, prefix="test1")

        pass

    def test_menu_protobuf(self):
        node1 = Node(["initial"], ("genericalg"), "node1")
        node2 = Node(["node1", "node3"], ("genericalg"), "node2")
        node3 = Node(["initial"], ("genericalg"), "node3")
        node4 = Node(["node2", "node5"], ("genereicalg"), "node4")
        node5 = Node(["initial"], ("genericalg"), "node5")

        test_graph1 = Directed_Graph("graph1")
        test_graph1.add(node1)
        test_graph1.add(node2)
        test_graph1.add(node3)
        test_graph1.add(node4)
        test_graph1.add(node5)

        # build the first test graph
        test_graph1.build()

        print("SORTED NODES")
        for node in test_graph1.nodes:
            print(node.id)
        print("SORTED NODES")

        test_menu1 = GraphMenu("test")
        test_menu1.add(test_graph1)
        test_menu1.build()
        test_menu1.create_vis()
        print("==========LEAVES==========")
        print(str(test_menu1.get_leaves()))
        print("==========LEAVES==========")

        msg = test_menu1.to_msg()
        print(text_format.MessageToBytes(msg))
        print(text_format.MessageToString(msg))

        test_menu2 = GraphMenu("test1")
        test_menu2.to_menu_from_msg(msg)
        # test_menu2.create_vis(terminal_print=True)

        pass

    def test_schema(self):
        node1 = Node(["initial"], ("genericalg"), "Table")
        graph1 = Directed_Graph("graph1")
        graph1.add(node1)
        graph1.build()

        node2 = Node([graph1], ("genericalg"), "TableInfo")
        node3 = Node(["TableInfo"], ("genericalg"), "Schema")
        node4 = Node(["Schema"], ("genericalg"), "SchemaInfo")
        node5 = Node(["SchemaInfo"], ("genericalg"), "SchemaAuxInfo")
        node6 = Node(["SchemaAuxInfo"], ("genericalg"), "Frequency")
        node7 = Node(["SchemaInfo"], ("genericalg"), "Field")
        node8 = Node(["Field"], ("genericalg"), "FieldInfo")
        node9 = Node(["FieldInfo"], ("genericalg"), "FeildAuxInfo")
        node10 = Node(["FieldInfo"], ("genericalg"), "ArrowType")

        graph2 = Directed_Graph("graph2")
        graph2.add(node2)
        graph2.add(node3)
        graph2.add(node4)
        graph2.add(node5)
        graph2.add(node6)
        graph2.add(node7)
        graph2.add(node8)
        graph2.add(node9)
        graph2.add(node10)
        graph2.build()
        graph2.create_vis(prefix="test_graph_viz")


if __name__ == "__main__":
    unittest.main()
