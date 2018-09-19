#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Module for test data generation
"""

from random import random
from array import array
import csv
import io
import sys
import logging

from artemis.core.algo import logged

@logged
class GenCsvLike:
    
    '''
    Creates data in CSV format and sends bytes.
    '''
    #__logger = logging.getLogger(__name__)
    def gen_chunk(self, ncolumn, unit, size):
        
        # Create a chunk of data of ncolumns, of size <size> in <unit>.
        
        units = {
                'b': 1, 
                'k': 1000, 
                'm': 1000000, 
                'g': 1000000000, 
                'B': 1, 
                'K': 1000, 
                'M': 1000000, 
                'G': 1000000000}
        
        # Based off tests of random floats from random.random. 
        float_size = 20  

        # Total number of floats needed according to supplied criteria.
        nfloats = int((size * units[unit] / float_size))  

        # Total number of rows based off number of floats and required columns
        # nrows = int(nfloats / ncolumn)  
        chunk = ''
        floats = array('d', (random() for i in range(nfloats)))
        csv_rows = []
        csv_row = []
        i = 0
        j = 0
        
        # Initialize all variables above to avoid null references.

        while i < nfloats:
            # Generates list of rows.
            csv_row = []
            j = 0
            while j < ncolumn and i < nfloats:
                # Generates columns in each row. 
                csv_row.append(floats[i])
                j += 1
                i += 1
            csv_rows.append(csv_row)

        # Use StringIO as an in memory file equivalent 
        # (instead of with...open construction).
        output = io.StringIO()  
        sio_f_csv = csv.writer(output)
        sio_f_csv.writerows(csv_rows)

        # Encodes the csv file as bytes.
        chunk = bytes(output.getvalue(), encoding='utf_8')  
        return chunk
    
    def generate(self):  
        print('Generate')
        self.__logger.info("%s: Producing Data" % (__class__.__name__))
        self.__logger.debug("%s: Producing Data" % (__class__.__name__))
        i = 0
        mysum = 0
        mysumsize = 0
        while i < 10:
            getdata = self.gen_chunk(20, 'm', 10)
            self.__logger.debug('%s: type data: %s' % (__class__.__name__,type(getdata)))  # Should be bytes.
            mysumsize += sys.getsizeof(getdata)
            mysum += len(getdata)
            i += 1
            yield getdata
        
        # Helped to figure out the math for an average float size.
        self.__logger.debug('%s: Average of total: %2.1f' %
                            (_class__.__name__, mysum/i)) 
        # Same as previous.
        self.__logger.debug('%s: Average of size: %2.1f' %
                            (__class__.__name__, mysumsize/i))  
