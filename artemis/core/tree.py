class Element:
    """
    Element class to generically contain whatever we want to put in the tree.
    Only important field is "key".
    """
    def __init__(self, key):
        self.key = key

class Node:
    """Stable container to hold Element objects and operate on them."""
    def __init__(self, key):

        self.parents = []
        self.children = []
        self.key = key
        self.payload = None
        '''
        Old way, add payload parameter if you want to use. Should be element.
        self.payload = payload
        '''

    def with_parents(key, payload, parents):
        mynode = Node(key,payload)
        mynode.parents = parents
        for parent in mynode.parents:
            parent.children.append(mynode)
        return mynode

    def add_to_parents(self):
        #TOFIX THIS IS COMPLETELY BORKED. FYI.
        """Adds self to the lists of children of self's parents."""
        for parent in self.parents.values():
            parent.children.append(self)

    def remove_from_parents(self):
        """Removes self from the lists of children of self's parents."""
        for parent in self.parents:
            if self.key in parent.children:
                del parent.children[self]
    
    def update_leaves(self, tree): #Move this to Tree?
        """Updates the list of leaves in the node's tree."""
        for parent in self.parents:
            if parent.key in tree.leaves:
                del tree.leaves[parent.key]
        tree.leaves[self.key] = self

    def remove_from_leaves(self, tree): #Move this to Tree?
        tree.leaves.remove(self)

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
        self.leaves = {}
        self.nodes = {}

        '''
        Old way, add root parameter, should be node.
        self.root =  root
        self.leaves = {root.key : self.root}
        self.nodes = {root.key : self.root}
        '''

    def merge_trees(self, source_tree, target_element_key):
        target_node = self.get_node_by_key(target_element_key)
        target_node.remove_from_leaves(self)
        self.leaves.append(source_tree.leaves)

    def get_node_by_key(self, key):
        return self.nodes[key]

    def add_node(self, node):
        self.nodes[node.key] = node
