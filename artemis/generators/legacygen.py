#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Classes for generating legacy (mainframe) like data
"""
import string

from artemis.generators.common import GeneratorBase


class GenMF(GeneratorBase):
    '''
    Generator for mainframe style data.

    Generates specific number of records and columns.
    '''

    def __init__(self, name, **kwargs):
        '''
        Generator parameters. Configured once per instantiation.
        '''
        self._defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            self._defaults[key] = kwargs[key]

        # Set the properties with the full configuration
        super().__init__(name, **self._defaults)
        self.ds_schema = None
        if 'ds_schema' in self._defaults.keys():
            self.ds_schema = self._defaults['ds_schema']
        else:
            self.ds_schema = []
            for key in self._defaults:
                if 'column' in key:
                    self.ds_schema.append(self._defaults[key])

        self._nbatches = self.properties.nbatches
        self.num_rows = self.properties.num_rows

        # Specific characters used for encoding signed integers.
        self.pos_char = self.properties.pos_char
        self.neg_char = self.properties.neg_char

    def _set_defaults(self):
        #  pos_char, neg_char
        #  Specific characters used for encoding signed integers.
        defaults = {'seed': 42,
                    'nbatches': 1,
                    'num_rows': 10,
                    'pos_char': {'0': '{', '1': 'a',
                                 '2': 'b', '3': 'c', '4': 'd',
                                 '5': 'e', '6': 'f', '7': 'g',
                                 '8': 'h', '9': 'i'},
                    'neg_char': {'0': 'j', '1': 'k', '2': 'l',
                                 '3': 'm', '4': 'n',
                                 '5': 'o', '6': 'p', '7': 'q',
                                 '8': 'r', '9': 's'}
                    }
        return defaults

    def gen_column(self, dataset, size):
        '''
        Creates a column of data. The number of records is size.
        '''
        rand_col = []

        #  Create data of specific unit types.
        if dataset['utype'] == 'int':
            # Creates a column of "size" records of integers.
            for i in range(size):
                dpoint = self.random_state.\
                    randint(dataset['min_val'], dataset['max_val'])
                if dpoint < 0:
                    # Convert negative integers.
                    dpoint = str(dpoint)
                    dpoint = dpoint.replace('-', '')
                    dpoint = dpoint.replace(dpoint[-1],
                                            self.neg_char[dpoint[-1:]])
                else:
                    # Convert positive integers.
                    dpoint = str(dpoint)
                    dpoint = dpoint.replace(dpoint[-1],
                                            self.pos_char[dpoint[-1:]])
                # Print to be converted to logger if appropriate.
                self.__logger.debug('Data pointi: ' + dpoint)
                dpoint = ('0' * (dataset['length'] - len(dpoint))) + dpoint
                self.__logger.debug('Data pointiw: ' + dpoint)
                rand_col.append(dpoint)
        elif dataset['utype'] == 'uint':
            # Creates a column of "size" records of unsigned ints.
            for i in range(size):
                dpoint = self.random_state.randint(dataset['min_val'],
                                                   dataset['max_val'])
                dpoint = str(dpoint)
                self.__logger.debug('Data pointu: ' + dpoint)
                dpoint = ('0' * (dataset['length'] - len(dpoint))) + dpoint
                self.__logger.debug('Data pointuw: ' + dpoint)
                rand_col.append(dpoint)
        else:
            # Creates a column of "size" records of strings.
            # Characters allowed in the string.
            source = string.ascii_lowercase\
                   + string.ascii_uppercase\
                   + string.digits\
                   + string.punctuation
            source = list(source)
            for i in range(size):
                dpoint = ''.join(self.random_state.choice(source,
                                                          dataset['length']))
                self.__logger.debug('Data pointc: ' + dpoint)
                dpoint = dpoint + (' ' * (dataset['length'] - len(dpoint)))
                self.__logger.debug('Data pointcw: ' + dpoint)
                rand_col.append(dpoint)

        self.__logger.debug(rand_col)
        return rand_col

    def gen_chunk(self):
        '''
        Generates a chunk of data as per configured instance.
        '''
        chunk = ''
        cols = []

        # Creates a column of data for each field.
        for dataset in self.ds_schema:
            cols.append(self.gen_column(dataset, self.num_rows))

        i = 0

        # Goes through the columns to create records.
        while i < self.num_rows:
            for column in cols:
                chunk = chunk + column[i]
            i = i + 1

        self.__logger.info('Chunk: %s', chunk)
        # Encode data chunk in cp500.
        # Might want to make this configurable.
        chunk = chunk.encode(encoding='cp500')
        self.__logger.info('Chunk ebcdic: %s', chunk)

        return chunk

    def generate(self):
        while self._nbatches > 0:
            self.__logger.info("%s: Generating datum " %
                               (self.__class__.__name__))
            data = self.gen_chunk()
            self.__logger.debug('%s: type data: %s' %
                                (self.__class__.__name__, type(data)))
            yield data
            self._nbatches -= 1
            self.__logger.debug("Batch %i", self._nbatches)
