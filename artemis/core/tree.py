class Element:
    """
    Element class to generically contain whatever we want to put in the tree.
    Only important field is "key".
    """
    def __init__(self, key):
        self.key = key

class Node:
    """Stable container to hold Element objects and operate on them."""
    def __init__(self, key, parents):

        self.parents = parents
        self.children = [] 
        self.key = key
        self.payload = None

    def remove_from_parents(self):
        """Removes self from the lists of children of self's parents."""
        pass

    def __str__(self):
        """Allows pretty printing of Nodes."""
        return self.key

    def __repr__(self):
        """Allows pretty printing of Nodes."""
        return self.key

class Tree:
    """Structure of Nodes. Metadata/job organisation."""
    def __init__(self):
        self.root = None
        self.leaves = [] #Holds a list of keys of nodes that are leaves.
        self.nodes = {} #Holds the actual nodes referenced by their keys.

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
