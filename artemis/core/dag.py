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

from toposort import toposort, toposort_flatten
from collections import OrderedDict, deque
from functools import reduce as _reduce

from tree import Tree, Node


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
        
        print(type(parents))
        if not isinstance(parents, list):
            raise TypeError("Sequence input must be list")
        for item in parents:
            if isinstance(item, str):
                self._parents.append(item)
            elif isinstance(item, Chain):
                self._parents.append(item.leaf)
            else:
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
            raise TypeError("Chain input must be list")
        for item in root:
            if isinstance(item, str):
                self._root.append(item)
            elif isinstance(item, Chain):
                self._root.append(item.leaf)
            else:
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
            print("Error, extra item in tree")
            return False
        
        # Toposort tree, ensure single leaf
        deptree_sorted = list(toposort(deptree))
        if(len(deptree_sorted[-1]) != 1):
            print("Error, chain does not terminate to single leaf")
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
            print("Sequence {0}".format(i))
            print(seq)
        return is_valid

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
        self._sequence = OrderedDict() # Should be the menu data structure, an iterable actually with lookup to get algos
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
            print("Error in validating chain ", chain.item)

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
            print("Root node has multiple inputs")
        elif root[0] != "initial":
            print("Root node not set to initial")
        else:
            self._root = "initial"
        
        tree = toposort_flatten(deptree)
        for element in tree:
            if element == "initial":
                self._sequence[element] = ("iorequest",)
            else:
                self._sequence[element] = algomap[element]
        print(self._sequence)
        
        #This build the tree with the Tree and Node classes.
        #This adds each node to the tree from the ordered dictionary created from the toposort tree.
        for key in self._sequence.keys():
            if key == 'initial':
                self._seq_tree.root = Node(key)
                self._seq_tree.add_node(self._seq_tree.root)
            else:
                self._seq_tree.add_node(Node(key))
        print(self._seq_tree.nodes)
        
        #This goes through the chains to set the children and parents properly.
        for chain in self.chains:
            for seq in chain.sequences:
                print('Node: ' + seq.element + ' and parents: ' + str(seq.parents))
                self._seq_tree.nodes[seq.element].parents = seq.parents
                for parent in seq.parents:
                    self._seq_tree.nodes[parent].children.append(seq.element)
        print(type(self._seq_tree.nodes['initial'].parents))
        for node in self._seq_tree.nodes.values():
            my_parents = node.parents
            my_children = node.children
            print('Node: ' + node.key +  ', the parents: ' + str(my_parents) + ' and children: ' + str(my_children))

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


def test_basic():
    # First run the dummy example, then run our use of Sequence, Chain, Menu
    sequence1 = (["initial"], ("alg1", "alg2"), "seq1")
    sequence2 = (["initial"], ("alg1", "alg2"), "seq2")
    sequence3 = (["seq1", "seq2"], ("alg1", "alg2"), "seq3")
    sequence4 = (["seq3"], ("alg1", "alg2"), "seq4")

    dummydag1 = [sequence1, sequence2, sequence3, sequence4]

    #Chain1 = dummydag1

    sequence5 = (["initial"], ("alg1", "alg2"), "seq5")
    sequence6 = (["seq5"], ("alg1", "alg2"), "seq6")
    sequence7 = (["seq5", "seq3"], ("alg1"), "seq7")
    dummydag2 = [sequence7, sequence6, sequence5]

    #Chain2 = dummydag2

    #sequenceX = ([Chain1, Chain2], ("algs"), "outputEL")

    dags = [dummydag1, dummydag2]

    elements_unsorted = {}
    # Actually have only 1 dag from initial
    # Each subdag is just independent from other subdag from initial
    # Need to get all subdags 
    for dag in dags:
        # Loop over list of sequences in a dag
        for seq in dag:
            elements_unsorted[seq[2]] = set(seq[0])

    print(elements_unsorted)
    print(list(toposort(elements_unsorted)))
    print(toposort_flatten(elements_unsorted))

def test_sequence():
    pass

if __name__ == "__main__":

    # Testing with actual classes
    seq1 = Sequence(["initial"], ("alg1", "alg2"), "seq1")
    seq2 = Sequence(["initial"], ("alg1", "alg2"), "seq2")
    seq3 = Sequence(["seq1", "seq2"], ("alg3",), "seq3")
    seq4 = Sequence(["seq3"], ("alg4",), "seq4")
 
    print("===========Sequence===============")
    print(seq1)
    print("===========Sequence===============")
    dummyChain1 = Chain("dummy1")
    dummyChain1.add(seq1)
    dummyChain1.add(seq4)
    dummyChain1.add(seq3)
    dummyChain1.add(seq2)
    #print(dummyChain1._graph)
    #dummyChain1._validate()
    #for seq in dummyChain1.sequences:
    #    print(seq[0],seq[1],seq[2])
    #dummyChain1._validate_chain()
    dummyChain1.build()

    seq5 = Sequence(["initial"], ("alg1", "alg2"), "seq5")
    seq6 = Sequence(["seq5"], ("alg1", "alg2"), "seq6")
    seq7 = Sequence(["seq6"], ("alg1",), "seq7")

    dummyChain2 = Chain("dummy2")
    dummyChain2.add(seq5)
    dummyChain2.add(seq6)
    dummyChain2.add(seq7)

    #dummyChain2._validate()
    dummyChain2.build()

    seqX = Sequence([dummyChain1, dummyChain2], ("algX",), "seqX")
    print(seqX)
    dummyChainX = Chain("dummyX", [dummyChain1, dummyChain2])
    dummyChainX.add(seqX)


    testmenu = Menu("test")
    testmenu.add(dummyChain1)
    testmenu.add(dummyChain2)
    testmenu.add(dummyChainX)
    testmenu.generate()
