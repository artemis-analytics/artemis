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

'''

Computation graph data structure for persisting a business process model as a Directed Acyclic Graph. 

Includes a topological sorting algorithm for generating Directed Acyclic Graphs 
given a set of inputs and an output in Menu data types used in Artemis and Cronos.

Defines all possible Sequences or Buisness processes as Nodes and each
chain of sequences as a Directed_Graph datatype

A topological sort is applied to these data structures

We  used an implementation of Khan's algorithim to
solve the topological sorting problem that this Class addresses
https://www.geeksforgeeks.org/topological-sorting-indegree-based-solution/
https://en.wikipedia.org/wiki/Topological_sorting

https://stackoverflow.com/questions/5287516/dependencies-tree-implementation
https://stackoverflow.com/questions/11557241/python-sorting-a-dependency-list

Or toposort in PyPi
https://bitbucket.org/ericvsmith/toposort

Overall, this class uses the mypy library to enforce type saftey here

mypy library: http://mypy-lang.org/


'''

import pygraphviz as pgv
from collections import OrderedDict, deque, namedtuple
from functools import reduce as _reduce
from queue import Queue
from typing import List, Tuple

from artemis.io.protobuf.menu_pb2 import Menu as Menu_pb
from artemis.logger import Logger


@Logger.logged
class Node():
    '''
    Node (prev. Sequence) class

    input ids = list of input element ids
    or
    input Directed_graphs, if node get last element
    name from a previous Girected_Graph

    algos = tuple of the function names to execute

    Sequence object should be an iterable, immutable object
    '''

    def __init__(self, parents: List, algos: Tuple[str], id: str) -> None:
        self._parents = []  # type: list
        self._algos = algos  # of type tuple
        if not isinstance(id, str):
            raise TypeError("Id input must be of type str")
        self._id = id  # expect string

        self._node = (self._parents, self._algos, self._id)

        if isinstance(parents, list):
            for parent in parents:
                if isinstance(parent, str):
                    self._parents.append(parent)
                elif isinstance(parent, Directed_Graph):
                    '''
                    if the parent of this node is a Directed_Graph object,
                    we take the leaves of this graph as a string and
                    add those leaves as strings to
                    _parents which is a list(str)
                    '''
                    for leaf in parent.leaf:
                        self._parents.append(leaf)
                else:
                    # self.__logger.error("%s Input id is not str or Graph" %
                    #                    self.__class__.name)
                    raise TypeError("Input id is not str or Graph")

            '''
            This is a tuple that is the internal representation of the Node
            '''

    def __str__(self) -> str:
        return_string = "Parents: " + "".join(' {0}'.format(p)
                                              for p in self._parents) + "\n"
        return_string += "Algos: " + "".join('{0}'.format(a)
                                             for a in self._algos) + "\n"
        return_string += "ID: " + '{0}'.format(self._id)
        return return_string

    @property
    def parents(self) -> List:
        '''
        list of input parent ids in the dependency graphs
        mutable list since inputs should easily extended
        '''
        return self._parents

    @property
    def algos(self) -> Tuple[str]:
        return self._algos

    @property
    def id(self) -> str:
        '''
        Represents the name of a unique id in the dependency graph
        This is the name of the Node in this context
        '''
        return self._id

    def change_id(self, new_id: str) -> None:
        self._id = new_id
        pass

    def __len__(self) -> int:
        return len(self._node)

    '''
    the return type of this type is ambiguous
    '''
    def __getitem__(self, position: int):
        return self._node[position]


@Logger.logged
class Directed_Graph():
    '''
    List of nodes (or a "chain" of action nodes(sequences)
    which must occur in an order))

    A Directed_Graph must start with the initial element
    Each sequence must relate to previous sequence by element

    A Directed graph can have many leafs,
    this is because the leaf is a list of strings
    which represent the nodes that it terminates to

    Note: Chain could originate from previous chain, or multiple chain
    allowing for predefined chains to used by others
    '''

    def __init__(self, id: str, root: List = ["initial"]) -> None:
        self._id = id  # type: str
        self._root = []  # type: list[str]
        # "one to many" relationship of this graph means
        # that we can have many leaves (ie: a list of leaves)
        # when creating this graph
        self._leaf = []  # type: list[str]
        self._nodes = []  # type: list[Node]
        self._initial_node = Node(parents=["none"],
                                  algos=("none"), id="initial")
        # this is a dictionary whose keys are the nodes of the graph.
        # For each key, the corresponding value is a list containing
        # the nodes that are connected by a direct arc from this node.
        # this will implicitly be a directed graph as the keys will be
        # parents and the values in the list will be the children
        self._internal_graph = {}

        self._attempted_built = False

        if not isinstance(root, list):
            raise TypeError("Direct_Graph input must be lis")

        # ensure that the root is of type Directed_Graph or string
        for item in root:
            if item == "initial":
                self._root.append(item)
                self._nodes.append(self._initial_node)
            elif isinstance(item, str):
                self._root.append(item)
            elif isinstance(item, Directed_Graph):
                for leaf in item.leaf:
                    self._root.append(leaf)
            else:
                raise TypeError("Input id is nor str or Directed_Graph")

    def __repr__(self) -> None:
        pass

    def __str__(self) -> None:
        pass

    @property
    def id(self) -> str:
        '''
        Return the chain name
        '''
        return self._id

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    @property
    def leaf(self) -> List[str]:
        '''
        Returns the list(str) of the leaves of this Directed_Graph
        '''
        return self._leaf

    @property
    def root(self) -> str:
        return self._root

    def change_id(self, new_id: str) -> None:
        self._id = new_id
        pass

    def __len__(self) -> int:
        return len(self._nodes)

    '''
    the return type of this type is ambiguous
    '''
    def __getitem__(self, position: int):
        return self._nodes[position]

    def add(self, node: Node) -> None:
        self._nodes.append(node)

    
    # def internal_graph(self):
    #     return self._internal_graph
    

    def attempted_built(self) -> bool:
        return self._attempted_built

    '''
    Creates a pygraphviz from the tree of buisness processes
    We may have to move the following code depending on what graph
    (either sorted or unsorted we want to visualize)

    We begin by using the unsorted graph and then adding
    all of the parent/child relationships this way
    '''
    def create_vis(self, terminal_print=False, prefix=None) -> None:

        output_graph = pgv.AGraph(strict=False, directed=True)

        if self._attempted_built is not True:
            self.__logger.error("Error: unable to create visualization")
            return

        for node in self._nodes:
            for parent in node.parents:
                if parent != "none" and parent != "":
                    output_graph.add_edge(parent, node.id)

        if terminal_print is True:
            print(output_graph.string())

        output_graph.layout(prog='dot')
        if prefix is None:
            output_graph.draw(str(hash(self)) + '.png')
        else:
            output_graph.draw(prefix + '.png')
        return

    def build(self) -> bool:
        '''
        Builds the graph from the private member information
        '''

        if self._attempted_built is True:
            print("Error: this graph has already been built")

        # set attempted_built to true as we have called build
        self._attempted_built = True

        # initialize the is_valid boolean which is returned by this function
        is_valid = True

        # initalize the dependency_tree and algorithim_map
        dependency_tree = {}
        algorithim_map = {}

        '''
        Updates the self._leaf field once the Directed_graph has been built

        We add the leaves (Nodes with no children)
        to self._leaf and populate that feild

        This is accomplished by making a set of all nodes
        and a set of nodes with children and the diffrence
        between those sets is the set of Nodes without children,
        it: leaves as desired
        '''

        have_children = set()
        all_nodes = set()

        for node in self._nodes:
            dependency_tree[node.id] = node.parents
            algorithim_map[node.id] = node.algos
            all_nodes.add(node.id)

            for parent in node.parents:
                if parent != "none" or parent != "":
                    have_children.add(parent)

        self._leaf = list(all_nodes - have_children)

        
        # initialize the internal_graph dictionary
        # for node in self._nodes:
        #    self._internal_graph[node.id] = []

        # populate the internal_graph dictionary
        # having "none" as the parent means that it is a root
        # having an empty list for the children means that the id is a leaf
        # for node in self._nodes:
        #     for parent in node.parents:
        #         if parent != "none":
        #             self._internal_graph[parent].append(node.id)

        # check for the leaves of this graph
        # for node in self._nodes:
        #     if self._internal_graph[node.id] == []:
        #         self._leaf.append(node.id)

        # apply a topological sort to the tree
        # we use Kahn's algorithim for this
        # https://www.geeksforgeeks.org/topological-sorting-indegree-based-solution/
        # https://en.wikipedia.org/wiki/Topological_sorting

        # define the topological sorting function
        def topological_sort(self):
            is_sortable = True
            indegree = {}
            visited_nodes = 0
            topological_sort = []

            for node in self._nodes:
                indegree[node.id] = 0
                for parent in node.parents:
                    if parent != "none":
                        indegree[node.id] += 1

            q = Queue()

            for node in self._nodes:
                if indegree[node.id] == 0:
                    q.put(node)

            while q.qsize() != 0:
                # get is deque for the queue library
                current_node = q.get()
                topological_sort.append(current_node)
                visited_nodes += 1

                for node in self._nodes:
                    if any(current_node.id in p for p in node.parents):
                        indegree[node.id] -= 1
                        if indegree[node.id] == 0:
                            # put is enque for this queue library
                            q.put(node)

            if visited_nodes != len(self._nodes):
                is_sortable = False

            return is_sortable, topological_sort

        # call the topological sorting function
        is_sortable, topological_sort_list = topological_sort(self)

        is_valid = is_sortable

        # ensure that the graph is a valid directed, acyclic graph
        #
        # print("TOPOLOGICALSORT LIST")
        # for item in topological_sort_list:
        #     print(item)
        # print("TOPOLOGICALSORT LIST")
        

        ordered_nodes = deque()
        for current_node in topological_sort_list:
            if current_node.id not in dependency_tree:
                print("Error, cannot retrieve inputs")
                is_valid = False
                continue
            if current_node.id not in algorithim_map:
                print("Error, cannot retrieve algorithims")
                is_valid = False
                continue

            new_node = Node(dependency_tree[current_node.id],
                            algorithim_map[current_node.id],
                            current_node.id)
            ordered_nodes.appendleft(new_node)

        return is_valid


@Logger.logged
class GraphMenu():
    '''
    List of Chains (Directed_Graphs) which describe the various processing
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

    def __init__(self, name: str) -> None:
        self._name = name
        self._graphs = []
        # Should be the menu data structure,
        # an iterable actually with lookup to get algos
        # sequence here refers to an ordered dict of algorithims or nodes
        self._ordered_sequence = OrderedDict()
        self._node_list = []

        # meta-information about the construction of the Menu
        self._attempted_built = False
        self._from_msg = False

    @property
    def graphs(self) -> List:
        return self._graphs

    @graphs.setter
    def graphs(self, graphs: List[Directed_Graph]) -> None:
        self._graphs = graphs

    @property
    def ordered_sequence(self) -> OrderedDict:
        return self._ordered_sequence

    @ordered_sequence.setter
    def ordered_sequence(self, ordered_sequence: OrderedDict) -> None:
        self._ordered_sequence = ordered_sequence

    def add(self, directed_graph: Directed_Graph) -> None:
        if directed_graph.attempted_built() is True:
            self.graphs.append(directed_graph)
        else:
            # self.__logger.error("%s: Error in validating directed_graph %s" %
            #        (self.__class__.__name__, directed_graph.item)
            pass

    def create_vis(self, terminal_print: bool = False, prefix=None) -> None:
        '''
        Creates a pygraphviz from the menu of buisness processes
        We may have to move the following code depending on what graph
        (either sorted or unsorted we want to visualize)
        We begin by using the unsorted graph and then
        adding all of the parent/child relationships this way
        '''
        output_graph = pgv.AGraph(strict=False, directed=True)

        
        #if self._from_msg != True or self._attempted_built != True:
        #    print("Error: unable to create visualization")
            #self.__logger.error("Error: unable to create visualization")
        #    return

        if self._from_msg is True:
            for key in self._ordered_sequence:
                for parent in self._ordered_sequence[key].parents:
                    if parent != "none" and parent != "":
                        output_graph.add_edge(parent, key)
        else:
            for node in self._node_list:
                for parent in node.parents:
                    if parent != "none" and parent != "":
                        output_graph.add_edge(parent, node.id)

        if terminal_print is True:
            print(output_graph.string())

        output_graph.layout(prog='dot')
        if prefix is None:
            output_graph.draw(str(hash(self)) + '.png')
        else:
            output_graph.draw(prefix + '.png')
        return

    def build(self) -> None:
        '''
        Similar to Chain.build
        Get root --> expect to be initial
        Get leaves --> Can be multiple
        "one to many" idea of the Directed_Graph and Menu classes
        '''

        Node_properties = namedtuple('Node_properties', 'algos parents')

        # set attempted_built to true as we have called build
        self._attempted_built = True

        # simmilar to the data structures used in Directed_Graph.build()
        # node_list is used for the topological sort later in the code
        dependancy_tree = {}
        algorithim_map = {}
        # node_list = []

        # populate the dependancy_tree and algorithim_map data
        # structures with evrey node from evrey Directed_Graph in the Menu
        for graph in self._graphs:
            for node in graph.nodes:
                dependancy_tree[node.id] = set(node.parents)
                algorithim_map[node.id] = node.algos
                self._node_list.append(node)

        # ensure that there are no repeated elements
        # in the nodes in the list of nodes internally
        unique_set = set()
        for node in self._node_list:
            if node.id in unique_set:
                self._node_list.remove(node)
            else:
                unique_set.add(node.id)

        root = list(_reduce(set.union, dependancy_tree.values())
                    - set(dependancy_tree.keys()))

        if(len(root) != 1):
            # self.__logger.error("%s: Root node has multiple inputs" %
            #                    self.__class__.__name__)
            pass
        elif root[0] != "initial":
            # self.__logger.error("%s: Root node not set to initial" %
            #                    self.__class__.__name__)
            pass
        else:
            self._root = "initial"

        # we now implement a topological
        # sort of the graph simmilar
        # to how it is done in Directed_Graph
        # define the topological sorting function
        def topological_sort(self):
            is_sortable = True
            indegree = {}
            visited_nodes = 0
            topological_sort = []

            for node in self._node_list:
                indegree[node.id] = 0
                for parent in node.parents:
                    if parent != "none":
                        indegree[node.id] += 1

            q = Queue()

            for node in self._node_list:
                if indegree[node.id] == 0:
                    q.put(node)

            while q.qsize() != 0:
                # get is deque for the queue library
                current_node = q.get()
                topological_sort.append(current_node)
                visited_nodes += 1

                for node in self._node_list:
                    if any(current_node.id in p for p in node.parents):
                        indegree[node.id] -= 1
                        if indegree[node.id] == 0:
                            # put is enque for this queue library
                            q.put(node)

            if visited_nodes != len(self._node_list):
                is_sortable = False

            return is_sortable, topological_sort

        # call the topological sorting function
        is_sortable, topological_sort_list = topological_sort(self)

        for element in topological_sort_list:
            if element.id == "initial":
                self._ordered_sequence[element.id] = \
                    Node_properties(("iorequest",), [])
            else:
                self._ordered_sequence[element.id] = \
                    Node_properties(algorithim_map[element.id],
                                    dependancy_tree[element.id])

        # self.__logger.debug(pformat(self._ordered_sequence))

    def get_leaves(self) -> List:
        '''
        This fucntion returns a list of the leaf nodes of a Directed_Graph

        Input: self object
        Return: List of str, each bieng the id of a leaf node,
        in no specified order
        '''

        if not self._attempted_built:
            raise ValueError("Cannot get leaves from unbuilt graph")

        leaves = []

        # A set for each node that is a parent node
        parents = set()

        # Iterate over each of the nodes in the
        # graph and populate the parent set
        for key in self._ordered_sequence:
            for parent in self._ordered_sequence[key].parents:
                parents.add(parent)

        for key in self._ordered_sequence:
            if key not in parents:
                leaves.append(key)

        return leaves

    def to_graph(self) -> OrderedDict:
        '''
        The name of this function is misleading, this function
        is used for steering and returns a
        dictionary of node -> algorithims used

        Generates ordered dictionary of node and algorithms
        Execution graph for Steering

        Perhaps this is what is called in steering
        '''

        graph_dictionary = OrderedDict()
        for key in self._ordered_sequence:
            name_list = []
            for algorithm in self._ordered_sequence[key].algos:
                try:
                    name_list.append(algorithm.name)
                except Exception:
                    if isinstance(algorithm, str):
                        # self.__logger.warning("%s: Algo type<str>" % algo)
                        name_list.append(algorithm)
                graph_dictionary[key] = name_list

        return graph_dictionary

    def to_tree(self) -> OrderedDict:
        '''
        Generates the dictionary of children and parents
        I am not sure where this is used
        '''
        tree_dictionary = OrderedDict()
        for key in self._ordered_sequence:
            print(key)
            print(self._ordered_sequence[key].parents)
            tree_dictionary[key] = list(self._ordered_sequence[key].parents)

        return tree_dictionary

    def to_msg(self) -> Menu_pb:
        '''
        Writes the Map to a protocol buffer message
        '''

        msg = Menu_pb()
        msg.name = self._name
        graph = msg.graphs.add()

        for key in self._ordered_sequence:
            node = graph.nodes.add()
            node.name = key
            node.parents.extend(self._ordered_sequence[key].parents)

            if isinstance(self._ordered_sequence[key].algos, tuple):
                for algo in self._ordered_sequence[key].algos:
                    node.algos.append(algo)
            elif isinstance(self._ordered_sequence[key].algos, str):
                node.algos.append(self._ordered_sequence[key].algos)
            else:
                # self.__logger.warning("Unable to add algo: %s" % algo)
                pass

        return msg

    def to_menu_from_msg(self, msg: Menu_pb) -> None:
        '''
        Reads in a menu from a protocol buffer message

        Due to how the menus are represented in protocol buffers,
        it is only possible to create the
        _ordered_sequence from the protocol buffers
        '''

        if not isinstance(msg, Menu_pb):
            # self.__logger.warning("Unable to add read message")
            raise TypeError("Error: attempted to read "
                            "protobuf message from non-message object")
            return

        self._from_msg = True

        if self._attempted_built is True:
            self.__logger.error("Error reading from msg to Menu: "
                                "Menu has already been built")
            return

        Node_properties = namedtuple('Node_properties', 'algos parents')

        # read from the protocol buffer message that we are passed
        for graph in msg.graphs:
            for node in graph.nodes:
                current_name = node.name
                current_parents = []
                current_algos = tuple()

                for parent in node.parents:
                    current_parents.append(parent)

                self._ordered_sequence[current_name] = \
                    Node_properties(current_algos, current_parents)

        return


@Logger.logged
class Directed_GraphDef():
    '''
    User defined class for generating the actual graph

    Ported over from dag.py

    Unsure of it's true use but keeping for futureproof/legacy reasons

    Seems to be deprecated or an unfinished prototype
    '''

    def __init__(self, items):
        '''
        Define all the possible algorithims to use
        '''
        self._items = items  # Menu items
        pass

    def graph(item, inputElement="initial"):
        '''
        takes the menu item name and default list of inputs
        inputElement can be of type Chain:
        inputElement = chain.leaf()
        '''
        graph = Directed_Graph(item)
        # define sequences
        return graph
