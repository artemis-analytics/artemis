#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.


import unittest
from collections import OrderedDict, namedtuple

from artemis.core.tree import Tree, Node, Element
from artemis.core.singleton import Singleton


class TreeDummyCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        self.sequence = OrderedDict()
        Seq_prop = namedtuple('Seq_prop', 'algos parents')
        self.sequence['test0'] = (Seq_prop('algotest0', []))
        self.sequence['test1'] = (Seq_prop('algotest1', ['test0']))
        self.sequence['test2'] = (Seq_prop('algotest2', ['test0']))
        self.sequence['test3'] = (Seq_prop('algotest3', ['test0']))
        self.sequence['test4'] = (Seq_prop('algotest4', ['test1', 'test2', 'test3']))
        self.sequence['test5'] = (Seq_prop('algotest5', ['test1', 'test4']))
        self.sequence['test6'] = (Seq_prop('algotest6', ['test2']))
        self.sequence['test7'] = (Seq_prop('algotest7', ['test6']))
        self.sequence['test8'] = (Seq_prop('algotest8', ['test7']))
        self.sequence['test9'] = (Seq_prop('algotest9', ['test8']))
        self.sequence['test10'] = (Seq_prop('algotest10', ['test0', 'test2', 'test7']))
        self.sequence['test11'] = (Seq_prop('algotest11', ['test10']))
        self.sequence['test12'] = (Seq_prop('algotest12', ['test11']))

    def tearDown(self):
        Singleton.reset(Tree)

    def test_create_element(self):
        my_element = Element('testElement0')
        self.assertEqual(my_element.key, 'testElement0', msg='Element name should be testElement.')

    def test_element_lock(self):
        my_element = Element('testElement1')
        self.assertFalse(my_element.locked, msg='Element should not be locked.')
        my_element.locked = True
        self.assertTrue(my_element.locked, msg='Element should be locked.')

    def test_add_data(self):
        my_element = Element('testElement2')
        my_element.locked = True
        self.assertTrue(my_element.locked, msg='Element should be locked.')
        my_element.add_data('testData0')
        self.assertIsNone(my_element.get_data())
        self.assertEqual(len(my_element._store.arrow_dict), 1, msg='Length should be 1.')
        my_element.locked = False
        self.assertFalse(my_element.locked, msg='Element should not be locked.')
        my_element.add_data('testData1')
        self.assertEqual(my_element.get_data(), 'testData1', msg='Data should be testData1.')

    def test_get_data(self):
        my_element = Element('testElement3')
        my_element.add_data('testData2')
        self.assertEqual(my_element.get_data(), 'testData2', msg='Data should be testData2.')

    def test_create_node(self):
        my_node = Node('node0', [])
        self.assertEqual(my_node.key, 'node0', msg='Node key should be node0.')

    def test_node_add_data(self):
        my_node = Node('node1', [])
        self.assertEqual(my_node.key, 'node1', msg='Node key should be node1.')
        self.assertEqual(len(my_node.payload), 0, msg='Payload should be empty.')
        my_node.add_payload('testdata0')
        self.assertEqual(len(my_node.payload), 1, msg='Payload should be 1.')

    def test_create_tree(self):
        self.assertFalse(Singleton.exists(Tree))
        my_tree = Tree('tree0')
        self.assertTrue(Singleton.exists(Tree))

    def test_get_node_by_key(self):
        my_tree = Tree('tree1')
        my_node = Node('node2', [])
        self.assertEqual(len(my_tree.nodes), 0, msg='Nodes should be empty.')
        my_tree.add_node(my_node)
        self.assertEqual(len(my_tree.nodes), 1, msg='Nodes should be one.')
        self.assertEqual(my_tree.get_node_by_key('node2'), my_node, msg='Values should be identical.')

    def test_add_node(self):
        my_tree = Tree('tree2')
        my_node = Node('node3', [])
        self.assertEqual(len(my_tree.nodes), 0, msg='Nodes should be empty.')
        my_tree.add_node(my_node)
        self.assertEqual(len(my_tree.nodes), 1, msg='Nodes should be one.')

    def test_update_leaves(self):
        my_tree = Tree('tree3')
        self.assertEqual(len(my_tree.leaves), 0, msg='Leaves should be empty.')
        my_tree.add_node(Node('node4', []))
        self.assertEqual(len(my_tree.leaves), 0, msg='Leaves should be empty.')
        my_tree.update_leaves()
        self.assertEqual(len(my_tree.leaves), 1, msg='Leaves should be one.')
        my_tree.add_node(Node('node5', ['node4']))
        my_tree.add_node(Node('node6', ['node4']))
        self.assertEqual(len(my_tree.leaves), 1, msg='Leaves should be one.')
        my_tree.update_parents()
        my_tree.update_leaves()
        self.assertEqual(len(my_tree.leaves), 2, msg='Leaves should be two.')

    def test_update_parents(self):
        my_tree = Tree('tree4')
        my_tree.add_node(Node('node7', []))
        self.assertEqual(len(my_tree.nodes['node7'].children), 0, msg='Children should be zero.')
        my_tree.add_node(Node('node8', ['node7']))
        self.assertEqual(len(my_tree.nodes['node7'].children), 0, msg='Children should be zero.')
        my_tree.update_parents()
        my_tree.update_leaves()
        self.assertEqual(len(my_tree.nodes['node7'].children), 1, msg='Children should be one.')

    def test_flush(self):
        my_tree = Tree('tree5')
        my_node = Node('node9', [])
        my_tree.add_node(my_node)
        self.assertEqual(len(my_tree.nodes['node9'].payload), 0, msg='Payload should be empty.')
        my_tree.nodes['node9'].add_payload('testpayload1')
        self.assertEqual(len(my_tree.nodes['node9'].payload), 1, msg='Payload should be one.')
        my_tree.flush()
        self.assertEqual(len(my_tree.nodes['node9'].payload), 0, msg='Payload should be empty.')

    def test_tree(self):
        # Dummy to create Tree.
        self.test_tree = Tree('My_test_tree')
        self.assertEqual(self.test_tree.name, 'My_test_tree', msg='Tree name was not set properly.')
        self.assertIsNone(self.test_tree.root, msg='Value present in root.')
        self.assertEqual(len(self.test_tree.leaves), 0, msg='Length of leaves is not zero.')
        self.assertEqual(len(self.test_tree.nodes), 0, msg='Length of nodes is not zero.')

        # Test to assert Tree singleton.
        self.test_tree2 = Tree('My_test_tree2')
        self.assertIs(self.test_tree, self.test_tree2, msg='Trees are not singletons.')

        # Test to verify that test_node is empty.
        self.test_node = None
        self.assertIsNone(self.test_node, msg='Node is not empty.')

        # Dummy to create a node with specific properties.
        self.test_node = Node('test0', self.sequence['test0'].parents)
        self.assertEqual(self.test_node.key, 'test0', msg='Key is not set properly.')
        self.assertEqual(self.test_node.parents, [], msg='Parents is not set properly.')
        self.assertEqual(self.test_node.children, [], msg='Children is not set properly.')
        self.assertEqual(len(self.test_node.payload), 0, msg='Payload is not empty.')

        # Dummy to have a Node assigned to the root of the Tree.
        self.test_tree.root = self.test_node
        self.assertEqual(self.test_tree.root.key, self.test_node.key, msg='Root assign key is broken.')
        self.assertEqual(self.test_tree.root.children, self.test_node.children, msg='Root assign children is broken.')
        self.assertEqual(self.test_tree.root.parents, self.test_node.parents, msg='Root assign parent is broken.')
        self.assertEqual(self.test_tree.root.payload, self.test_node.payload, msg='Root assign payload is broken.')

        # Dummy regarding adding leaves.
        self.assertEqual(len(self.test_tree.nodes), 0, msg='Nodes list in tree contains node and should not.')
        self.assertEqual(len(self.test_tree.leaves), 0, msg='Leaves list should be empty.')
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(len(self.test_tree.leaves), 0, msg='Leaves list should be empty.')

        # Dummy adding the root to the list of nodes.
        self.test_tree.add_node(self.test_node)
        self.assertEqual(len(self.test_tree.nodes), 1, msg='Nodes should be length 1.')
        self.assertEqual(self.test_tree.nodes['test0'].key, self.test_tree.root.key, msg='Nodes should have the same key property.')

        # Dummy updating the nodes' parents and the tree's leaves.
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(self.test_tree.nodes[self.test_tree.leaves[0]], self.test_node, msg='Leaf does not equal stand alone node.')
        self.assertEqual(self.test_tree.nodes[self.test_tree.leaves[0]], self.test_tree.root, msg='Leaf does not equal tree root.')

        # Dummy to add multiple nodes without updating leaves.
        self.test_tree.add_node(Node('test1', self.sequence['test1'].parents))
        self.test_tree.add_node(Node('test2', self.sequence['test2'].parents))
        self.test_tree.add_node(Node('test3', self.sequence['test3'].parents))
        self.assertEqual(len(self.test_tree.nodes), 4, msg='Nodes should be length 4.')

        # Dummy to update leaves with multiple leaves.
        self.assertEqual(len(self.test_tree.leaves), 1, msg='Leaves should still be length 1.')
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(len(self.test_tree.leaves), 3, msg='Leaves should be length 4.')

        # Add node with all previous leaves as parent, update leaves.
        self.test_tree.add_node(Node('test4', self.sequence['test4'].parents))
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(len(self.test_tree.leaves), 1, msg='Leaves should be length 1.')

        # Element test.
        self.test_element = Element('el_test0')
        self.assertEqual(self.test_element.key, 'el_test0', msg='Element key should be el_test0.')
        self.assertEqual(self.test_element.locked, False, msg='Element should not be locked.')

        self.assertEqual(len(self.test_tree.nodes['test0'].payload), 0, msg='Payload should be empty.')
        self.test_tree.nodes['test0'].add_payload(self.test_element)
        self.assertEqual(len(self.test_tree.nodes['test0'].payload), 1, msg='Payload should be one.')
        self.test_tree.nodes['test0'].add_payload(Element('el_test1'))
        self.assertEqual(len(self.test_tree.nodes['test0'].payload), 2, msg='Payload should be two.')
        print('Test payload length before flush.')
        print(len(self.test_tree.nodes['test0'].payload))
        Tree().flush()
        print('Test payload length after flush.')
        print(len(self.test_tree.nodes['test0'].payload))
