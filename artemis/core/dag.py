#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Dummy DAG creation script
Define all possible sequences
Apply a topological sort of each child to all possible parents

https://stackoverflow.com/questions/5287516/dependencies-tree-implementation
https://stackoverflow.com/questions/11557241/python-sorting-a-dependency-list

Or toposort in PyPi
https://bitbucket.org/ericvsmith/toposort
"""
import logging
from pprint import pformat
from toposort import toposort, toposort_flatten
from collections import OrderedDict, deque
from functools import reduce as _reduce

from artemis.core.algo import logged
from .tree import Tree, Node


@logged
class Sequence():
    '''
    inputELs = list of input ELement names
    or
    inputChains, if chains get last element name from chains

    algos = tuple of the function names to execute
    outputEL = unique output element name

    Sequence object should be an iterable, immutable tuple of length 3
    '''
    def __init__(self, parents, algos, element):
        
        self._parents = []  # of type list
        #TODO: enforce tuple, length 1 tuples may not be defined correctly due to trailing ,
        self._algos = algos  # of type tuple 
        self._element = element  # expect string
        
        self.__logger.info('%s: parent type %s' % (__class__.__name__,type(parents)))
        self.__logger.debug('%s: parent type %s' % (__class__.__name__,type(parents)))
        if not isinstance(parents, list):
            raise TypeError("Sequence input must be list")
        for item in parents:
            if isinstance(item, str):
                self._parents.append(item)
            elif isinstance(item, Chain):
                self._parents.append(item.leaf)
            else:
                self.__logger.error('%s: Input element is not str or Chain' % __class__.__name__)
                raise TypeError("Input element is not str or Chain")
        
        self._sequence = (self.parents, self.algos, self.element)
    
    def __str__(self):
        s = 'Parents: ' + ' '.join('{0}'.format(p) for p in self.parents) + '\n' 
        s += 'Algos: ' + ' '.join('{0}'.format(a) for a in self._algos) + '\n'
        s += 'Element: ' + '{0}'.format(self._element)
        return s

    def __repr__(self):
        pass
    
    @property
    def algos(self):
        '''
        tuple of function names which defines a specific execution order
        Describes a Business Process
        '''
        return self._algos

    @property
    def parents(self):
        '''
        List of the input parent elements in the dependency graph
        mutable list since inputs should be easily extended
        '''
        return self._parents

    @property
    def element(self):
        '''
        Represents the name of a unique element in a dependency graph
        '''
        return self._element

    def __len__(self):
        return len(self._sequence)

    def __getitem__(self, position):
        return self._sequence[position]

@logged
class Chain():
    '''
    List of sequences (or a chain of actions (sequences) which must occur in an order))
    A chain must start with the initial element
    Each sequence must relate to previous sequence by element
    A Chain must have a single leaf
    
    Note: Chain could originate from previous chain, or multiple chain
    allowing for predefined chains to used by others
    '''
    def __init__(self, item, root=["initial"]):
        # Item in a complete dag, represents a complete business process that can be easily scheduled from simply the item
        self._item = item  # type: str
        self._root = []  # type: List[str]
        self._leaf = ""  # type: List[str]
        self._sequences = []  # type: List[Sequence]

        if not isinstance(root, list):
            self.__logger.error('%s: Chain input must be list' % __class__.__name__)
            raise TypeError("Chain input must be list")
        for item in root:
            if isinstance(item, str):
                self._root.append(item)
            elif isinstance(item, Chain):
                self._root.append(item.leaf)
            else:
                self.__logger.error('%s: Input element is not str or Chain' % __class__.__name__)
                raise TypeError("Input element is not str or Chain")
        
    def __repr__(self):
        pass

    def __str__(self):
        '''
        print the chain in ASCII
        '''
        pass
        # https://codegolf.stackexchange.com/questions/11693/ascii-visualize-a-graph
        '''
        R=raw_input()
        V=' '.join(set(R)-set(' ,'))
        S=' '*len(V)
        for e in R.split():
            x,y=sorted(map(V.index,e[::2]));
            print S[:x]+'+'+'-'*(y-x-1)+'+'+S[y+1:];S=S[:x]+'|'+S[x+1:y]+'|'+S[y+1:];
            print S
        print V
        '''
    @property
    def item(self):
        '''
        Return the chain name
        '''
        return self._item

    @property
    def sequences(self):
        return self._sequences

    @property
    def leaf(self):
        return self._leaf

    @property
    def root(self):
        return self._root

    def __len__(self):
        return len(self._sequences)

    def __getitem__(self, position):
        return self._sequences[position]
    
    def add(self, sequence):
        self._sequences.append(sequence)
    
    def build(self):
        deptree = {}
        algmap = {}
        
        for seq in self._sequences:
            deptree[seq.element] = set(seq.parents)
            algmap[seq.element] = seq.algos
        
        # Obtain the roots and confirm
        root = _reduce(set.union, deptree.values()) - set(deptree.keys())
        if len(root - set(self._root)) != 0:
            self.__logger("%s: Error, extra item in tree" % __class__.__name__)
            return False
        
        # Toposort tree, ensure single leaf
        deptree_sorted = list(toposort(deptree))
        if(len(deptree_sorted[-1]) != 1):
            self.__logger("%s: Error, chain does not terminate to single leaf" % __class__.__name__)
            return False
        self._leaf = list(deptree_sorted[-1])[0]
        
        is_valid = True
        ordered_sequences = deque()
        for last in reversed(deptree_sorted):
            if len(last - set(self._root)) == 0:
                print("Initial node")
                continue
            for element in last:
                if element not in deptree:
                    print("Error, cannot retrieve inputs")
                    is_valid = False
                    continue
                if element not in algmap:
                    print("Error, cannot retrieve algs")
                    is_valid = False
                    continue
                seq = Sequence(list(deptree[element]), algmap[element], element)
                ordered_sequences.appendleft(seq)
        
        self._sequences = list(ordered_sequences)
        for i, seq in enumerate(self._sequences):
            self.__logger.debug("%s: Sequence %i %s" % (__class__.__name__, i, seq))
        return is_valid

@logged
class Menu():
    '''
    List of Chains which describe the various processing 
    for the given inputs

    Data structures

    Dependency graph
    Topological sorted list of elements
    Dictionary of Elements and list of algorithms which produce the element

    Final data structure is an OrderedDict
    OrderedDict menu = {}
    menu["Initial"] = (Algo_Create Initial Node Algo)"
    menu["Element"] = (Tuple of Algos)
    
    Dictionary of sequences
    OutputElement: (tuple of sequences) 
    '''
    def __init__(self, name):
        self._name = name
        self._chains = []
        self._sequence = OrderedDict()  # Should be the menu data structure, an iterable actually with lookup to get algos
        self._seq_tree = Tree()
    
    @property
    def chains(self):
        return self._chains

    @chains.setter
    def chains(self, chains):
        self._chains = chains
 
    @property
    def ordered_sequence(self):
        return self._sequence
    
    @ordered_sequence.setter
    def ordered_sequence(self, elements):
        self._sequences = elements

    @property
    def seq_tree(self):
        return self._seq_tree

    def add(self, chain):
        if chain.build() is True:
            self._chains.append(chain)
        else:
            self.__logger.error("%s: Error in validating chain %s" % 
                                (__class__.__name__, chain.item))

    def generate(self):
        '''
        Similar to Chain.build
        Get root --> expect to be initial
        Get leaves --> Can be multiple
        '''
        deptree = {}    # Build the tree
        algomap = {}
        for chain in self._chains:
            for seq in chain.sequences:
                deptree[seq.element] = set(seq.parents)
                algomap[seq.element] = set(seq.algos)
        
        root = list(_reduce(set.union, deptree.values()) - set(deptree.keys()))
        if(len(root) != 1):
            self.__logger.error("%s: Root node has multiple inputs" % __class__.__name__)
        elif root[0] != "initial":
            self.__logger.error("%s: Root node not set to initial" % __class__.__name__)
        else:
            self._root = "initial"
        
        tree = toposort_flatten(deptree)
        for element in tree:
            if element == "initial":
                self._sequence[element] = ("iorequest",)
            else:
                self._sequence[element] = algomap[element]
        
        if(logging.getLogger().isEnabledFor(logging.DEBUG) or
                self.__logger.isEnabledFor(logging.DEBUG)):
            self.__logger.debug("%s: generate %s " % 
                                (__class__.__name__, pformat(self._sequence)))
        
        #This build the tree with the Tree and Node classes.
        #This adds each node to the tree from the ordered dictionary created from the toposort tree.
        for key in self._sequence.keys():
            if key == 'initial':
                self._seq_tree.root = Node(key)
                self._seq_tree.add_node(self._seq_tree.root)
            else:
                self._seq_tree.add_node(Node(key))
        if(logging.getLogger().isEnabledFor(logging.DEBUG) or
                self.__logger.isEnabledFor(logging.DEBUG)):
            self.__logger.debug("%s: Nodes: %s" % 
                          (__class__.__name__,pformat(self._seq_tree.nodes)))
        
        #This goes through the chains to set the children and parents properly.
        for chain in self.chains:
            for seq in chain.sequences:
                self.__logger.debug('%s Node: %s Parents: %s' % 
                        (__class__.__name__, seq.element, seq.parents))
                self._seq_tree.nodes[seq.element].parents = seq.parents
                for parent in seq.parents:
                    self._seq_tree.nodes[parent].children.append(seq.element)
        self.__logger.debug('%s Initial node parent type: %s' % 
                            (__class__.__name__, type(self._seq_tree.nodes['initial'].parents)))
        for node in self._seq_tree.nodes.values():
            my_parents = node.parents
            my_children = node.children
            self.__logger.debug('%s: Node: %s Parents: %s Childern: %s' % 
                (__class__.__name__, node.key, my_parents, my_children))

class ChainDef():
    '''
    User defined class for generating the actual chain
    '''
    def __init__(self, items):
        '''
        Define all the possible algorithms to use
        '''
        self._items = items # Menu items
        pass

     
    def chain(item, inputElement="initial"):
        '''
        takes the menu item name and default list of inputs
        inputElement can be of type Chain:
            inputElement = chain.leaf()
        ''' 
        chain = Chain(item)
        # define sequences
        return chain


