#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

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
        self._locked = status

    def lock(self):
        self.locked = True

    def add_data(self, data):
        if self.locked:
            print('Cannot add data, element is locked.')
        else:
            self._store.add_to_dict(self.key, data)

    def get_data(self, prefix=None):
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
