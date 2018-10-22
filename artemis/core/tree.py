#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

import pyarrow as pa

from .singleton import Singleton
from .datastore import ArrowSets


class Element:
    """
    Element class to generically contain whatever we want to put in the tree.
    Only important field is "key".
    """
    def __init__(self, key):
        self._key = key
        self._locked = False
        self._store = ArrowSets()
        self._store.book(key)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        if self.locked:
            pass
        else:
            self._key = value

    @property
    def locked(self):
        return self._locked

    @locked.setter
    def locked(self, status):
        if self.locked:
            pass
        else:
            self._locked = status

    def lock(self):
        self.locked = True

    def add_data(self, data):
        if self.locked:
            print('Cannot add data, element is locked.')
        else:
            self._store.add_to_dict(self.key, data)

    def get_data(self):
        return self._store.get_data(self.key)


class Node:
    """Stable container to hold Element objects and operate on them."""
    def __init__(self, key, parents):

        self.parents = parents
        self.children = []
        self.key = key
        self.payload = []

    def remove_from_parents(self):
        """Removes self from the lists of children of self's parents."""
        pass

    def __str__(self):
        """Allows pretty printing of Nodes."""
        return self.key

    def __repr__(self):
        """Allows pretty printing of Nodes."""
        return self.key

    def add_payload(self, element):
        self.payload.append(element)


class Tree(metaclass=Singleton):
    """Structure of Nodes. Metadata/job organisation."""
    def __init__(self, name):
        self.name = name
        self.root = None
        self.leaves = []  # Holds a list of keys of nodes that are leaves.
        self.nodes = {}  # Holds the actual nodes referenced by their keys.

    def merge_trees(self, source_tree, target_element_key):
        pass

    def get_node_by_key(self, key):
        return self.nodes[key]

    def add_node(self, node):
        self.nodes[node.key] = node

    def update_leaves(self):
        """Updates the list of leaves in the node's tree."""
        self.leaves = []
        for key, node in self.nodes.items():
            if len(node.children) == 0:
                self.leaves.append(key)

    def update_parents(self):
        for node in self.nodes.values():
            for parent in node.parents:
                self.nodes[parent].children.append(node.key)

    def flush(self):
        for node in self.nodes.values():
            node.payload = []
        Singleton.reset(ArrowSets)


def main():
    # Create tree and nodes structure for testing.
    my_tree = Tree("run1")
    my_node1 = Node("key1", [])
    my_node2 = Node("key2", ["key1"])
    my_node3 = Node("key3", ["key1"])
    my_node4 = Node("key4", ["key2", "key3"])
    my_node5 = Node("key5", ["key2", "key3", "key4"])
    my_node6 = Node("key6", ["key5"])
    my_tree.add_node(my_node1)
    my_tree.add_node(my_node2)
    my_tree.add_node(my_node3)
    my_tree.add_node(my_node4)
    my_tree.add_node(my_node5)
    my_tree.add_node(my_node6)
    my_tree.update_leaves()
    # Create two elements per node.
    for node in my_tree.nodes.values():
        index = 0
        while index < 2:
            node.add_payload(Element(node.key + 'elem' + str(index)))
            index += 1
    # Create Arrow data.
    arr1 = pa.array([1, 2, 3, 4])
    arr2 = pa.array(['test1', 'test2', 'test3', 'test4'])
    data = [arr1, arr2]
    batch1 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

    print('Number of columns: ' + str(batch1.num_columns))
    print('Number of rows: ' + str(batch1.num_rows))
    print('Schema of batch: ' + str(batch1.schema))

    for i_batch in batch1:
        print('Batch print: ' + str(i_batch))

    arr3 = pa.array([11, 21, 31, 41])
    arr4 = pa.array(['test11', 'test21', 'test31', 'test41'])
    data2 = [arr3, arr4]
    batch2 = pa.RecordBatch.from_arrays(data2, ['f0', 'f1'])
    arr5 = pa.array([12, 22, 32, 42])
    arr6 = pa.array(['test12', 'test22', 'test32', 'test42'])
    data3 = [arr5, arr6]
    batch3 = pa.RecordBatch.from_arrays(data3, ['f0', 'f1'])
    arr7 = pa.array([13, 23, 33, 43])
    arr8 = pa.array(['test13', 'test23', 'test33', 'test43'])
    data4 = [arr7, arr8]
    batch4 = pa.RecordBatch.from_arrays(data4, ['f0', 'f1'])
    arr9 = pa.array([14, 24, 34, 44])
    arr10 = pa.array(['test14', 'test24', 'test34', 'test44'])
    data5 = [arr9, arr10]
    batch5 = pa.RecordBatch.from_arrays(data5, ['f0', 'f1'])
    arr11 = pa.array([15, 25, 35, 45])
    arr12 = pa.array(['test15', 'test25', 'test35', 'test45'])
    data6 = [arr11, arr12]
    batch6 = pa.RecordBatch.from_arrays(data6, ['f0', 'f1'])

    # Add data to tree.
    my_tree.nodes["key1"].payload[0].add_data(batch1)
    my_tree.nodes["key1"].payload[1].add_data(batch1)
    my_tree.nodes["key2"].payload[0].add_data(batch2)
    my_tree.nodes["key2"].payload[1].add_data(batch2)
    my_tree.nodes["key3"].payload[0].add_data(batch3)
    my_tree.nodes["key3"].payload[1].add_data(batch3)
    my_tree.nodes["key4"].payload[0].add_data(batch4)
    my_tree.nodes["key4"].payload[1].add_data(batch4)
    my_tree.nodes["key5"].payload[0].add_data(batch5)
    my_tree.nodes["key5"].payload[1].add_data(batch5)
    my_tree.nodes["key6"].payload[0].add_data(batch6)
    my_tree.nodes["key6"].payload[1].add_data(batch6)

    print('Printing my_tree')
    print(my_tree)
    print('Printing nodes in my_tree')
    print(my_tree.nodes)
    print('Printing the payload for each node.')
    print(my_tree.nodes["key1"].payload)
    print(my_tree.nodes["key2"].payload)
    print(my_tree.nodes["key3"].payload)
    print(my_tree.nodes["key4"].payload)
    print(my_tree.nodes["key5"].payload)
    print(my_tree.nodes["key6"].payload)
    test_store = ArrowSets()
    print('Printing the data store')
    print(test_store.arrow_dict)


if __name__ == "__main__":
    main()
