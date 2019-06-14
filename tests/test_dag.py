#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
import tempfile
import os

from toposort import toposort, toposort_flatten

from artemis.core.dag import Sequence, Chain, Menu
from google.protobuf import text_format

class DagTestCase(unittest.TestCase):
    
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        logging.getLogger().setLevel(logging.DEBUG)

    def tearDown(self):
        logging.getLogger().setLevel(logging.INFO)
    
    '''
    def test_sequence(self):
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

    def test_menu(self):

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
        with tempfile.TemporaryDirectory() as dirpath:
            fname = os.path.join(dirpath, 'testmenu.json')
            testmenu.to_json(fname)

            msg = testmenu.to_msg()
            print(text_format.MessageToString(msg))        
    '''


if __name__ == '__main__':
    unittest.main()
