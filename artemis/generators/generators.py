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


class GenCsvLike:
    
    '''
    Creates data in CSV format and sends bytes.
    '''

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
        print("Producing Data")
        i = 0
        mysum = 0
        mysumsize = 0
        while i < 10:
            getdata = self.gen_chunk(20, 'm', 10)
            print(type(getdata))  # Should be bytes.
            mysumsize += sys.getsizeof(getdata)
            mysum += len(getdata)
            i += 1
            yield getdata
        
        # Helped to figure out the math for an average float size.
        print('Average of total: ' + str(mysum/i)) 
        # Same as previous.
        print('Average of size: ' + str(mysumsize/i))  
