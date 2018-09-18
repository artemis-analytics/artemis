import unittest
from collections import OrderedDict, namedtuple

from artemis.core.tree import Tree, Node
from artemis.core.algo import AlgoBase, TestAlgo

class TreeTestCase(unittest.TestCase):

    def setUp(self):
        self.sequence = OrderedDict()
        Seq_prop = namedtuple('Seq_prop', 'algos parents')
        self.sequence['test0'] = (Seq_prop(TestAlgo('test0'), []))
        self.sequence['test1'] = (Seq_prop(TestAlgo('test1'), ['test0']))
        self.sequence['test2'] = (Seq_prop(TestAlgo('test2'), ['test0']))
        self.sequence['test3'] = (Seq_prop(TestAlgo('test3'), ['test0']))
        self.sequence['test4'] = (Seq_prop(TestAlgo('test4'), ['test1','test2','test3']))
        self.sequence['test5'] = (Seq_prop(TestAlgo('test5'), ['test1','test4']))
        self.sequence['test6'] = (Seq_prop(TestAlgo('test6'), ['test2']))
        self.sequence['test7'] = (Seq_prop(TestAlgo('test7'), ['test6']))
        self.sequence['test8'] = (Seq_prop(TestAlgo('test8'), ['test7']))
        self.sequence['test9'] = (Seq_prop(TestAlgo('test9'), ['test8']))
        self.sequence['test10'] = (Seq_prop(TestAlgo('test10'), ['test0', 'test2', 'test7']))
        self.sequence['test11'] = (Seq_prop(TestAlgo('test11'), ['test10']))
        self.sequence['test12'] = (Seq_prop(TestAlgo('test12'), ['test11']))

    def tearDown(self):
        pass #Not implemented in Ryan's code.

    def test_control(self):
        self.test_tree = Tree()
        self.assertIsNone(self.test_tree.root, msg='Value present in root.')
        self.assertEqual(len(self.test_tree.leaves), 0, msg='Length of leaves is not zero.')
        self.assertEqual(len(self.test_tree.nodes), 0, msg='Length of nodes is not zero.')

        self.test_node = None
        self.assertIsNone(self.test_node, msg='Node is not empty.')

        self.test_node = Node('test0', self.sequence['test0'].parents)
        self.assertEqual(self.test_node.key, 'test0', msg='Key is not set properly.')
        self.assertEqual(self.test_node.parents, [], msg='Parents is not set properly.')
        self.assertEqual(self.test_node.children, [], msg='Children is not set properly.')
        self.assertIsNone(self.test_node.payload, msg='Payload is not empty.')
        
        self.test_tree.root = self.test_node
        self.assertEqual(self.test_tree.root.key, self.test_node.key, msg='Root assign key is broken.')
        self.assertEqual(self.test_tree.root.children, self.test_node.children, msg='Root assign children is broken.')
        self.assertEqual(self.test_tree.root.parents, self.test_node.parents, msg='Root assign parent is broken.')
        self.assertEqual(self.test_tree.root.payload, self.test_node.payload, msg='Root assign payload is broken.')
        #Finish tests regarding adding nodes and stuff.
        self.assertEqual(len(self.test_tree.nodes), 0, msg='Nodes list in tree contains node and should not.')
        self.assertEqual(len(self.test_tree.leaves), 0, msg='Leaves list should be empty.')
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(len(self.test_tree.leaves), 0, msg='Leaves list should be empty.')

        self.test_tree.add_node(self.test_node)
        self.assertEqual(len(self.test_tree.nodes), 1, msg='Nodes should be length 1.')
        self.assertEqual(self.test_tree.nodes['test0'].key, self.test_tree.root.key, msg='Nodes should have the same key property.')

        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(self.test_tree.nodes[self.test_tree.leaves[0]], self.test_node, msg='Leaf does not equal stand alone node.')
        self.assertEqual(self.test_tree.nodes[self.test_tree.leaves[0]], self.test_tree.root, msg='Leaf does not equal tree root.')

        self.test_tree.add_node(Node('test1', self.sequence['test1'].parents))
        self.test_tree.add_node(Node('test2', self.sequence['test2'].parents))
        self.test_tree.add_node(Node('test3', self.sequence['test3'].parents))
        self.assertEqual(len(self.test_tree.nodes), 4, msg='Nodes should be length 4.')

        self.assertEqual(len(self.test_tree.leaves), 1, msg='Leaves should still be length 1.')
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(len(self.test_tree.leaves), 3, msg='Leaves should be length 4.')

        self.test_tree.add_node(Node('test4', self.sequence['test4'].parents))
        self.test_tree.update_parents()
        self.test_tree.update_leaves()
        self.assertEqual(len(self.test_tree.leaves), 1, msg='Leaves should be length 1.')
