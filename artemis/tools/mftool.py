#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

"""
Tool that reads mainframe files encoded in the EBCDIC format.
"""

import pyarrow as pa
from artemis.core.tool import ToolBase


class MfTool(ToolBase):
    '''
    The class that deals with mainframe files.
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
        self.ds_schema = []
        self.col_names = []
        if 'ds_schema' in self._defaults.keys():
            self.ds_schema = self._defaults['ds_schema']
            for i, column in enumerate(self.ds_schema):
                self.col_names.append('column_' + str(i))

        else:
            for key in self._defaults:
                if 'column' in key:
                    self.ds_schema.append(self._defaults[key])
                    name = key.split('_')[-1]
                    self.col_names.append(name)

        self.nrecords = len(self.ds_schema)
        self.rsize = 0
        for ds in self.ds_schema:
            self.rsize = self.rsize + ds['length']

        # Specific characters used for encoding signed integers.
        # Need to swap key,value from generator dict
        self.pos_char = dict((v, k)
                             for k, v in self.properties.pos_char.items())
        self.neg_char = dict((v, k)
                             for k, v in self.properties.neg_char.items())

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
    
    @property
    def record_size(self):
        return self.rsize

    @property
    def columns(self):
        return self.col_names

    def execute(self, block):
        '''
        Reads a block of data with the initialized MfTool object.
        '''

        # The block is decoded from the cp500 code page.
        block = block.decode('cp500')

        isize = len(block)
        self.__logger.debug("Block size to process %i", isize)
        odata = []
        arrowodata = []
        nrecords = len(self.ds_schema)

        # Create a list of empty lists for the number of columns.
        for field in self.ds_schema:
            odata.append([])

        ccounter = 0
        ncounter = 0
        fcounter = 0

        while ccounter < isize:
            # Extract record.
            rdata = block[ccounter: (ccounter + self.rsize)]
            while ncounter < nrecords:
                # Extract field.
                field = rdata[fcounter:
                              (fcounter + self.ds_schema[ncounter]['length'])]
                # Processes each field according to the datatype.
                if self.ds_schema[ncounter]['utype'] == 'int':
                    # Replacing the end character with a proper digit requires
                    # differentiating between negative and positive numbers.
                    if field[-1:] in self.pos_char:
                        # Padding zeroes are taken removed by type conversion.
                        field = int(field.replace(field[-1:],
                                                  self.pos_char[field[-1:]]))
                    else:
                        field = field.replace(field[-1:],
                                              self.neg_char[field[-1:]])
                        field = int('-' + field)
                    odata[ncounter].append(field)
                elif self.ds_schema[ncounter]['utype'] == 'str':
                    # Removes padding spaces from the data.
                    odata[ncounter].append(field.strip())
                elif self.ds_schema[ncounter]['utype'] == 'uint':
                    odata[ncounter].append(int(field))
                fcounter = fcounter + self.ds_schema[ncounter]['length']
                ncounter = ncounter + 1
            ncounter = 0
            fcounter = 0
            ccounter = ccounter + self.rsize

        # Creates apache arrow dataset.
        for my_list in odata:
            arrowodata.append(pa.array(my_list))

        self.__logger.debug('Output data lists.')
        self.__logger.debug(odata)

        self.__logger.debug('Output data arrow arrays.')
        self.__logger.debug(arrowodata)

        try:
            rbatch = pa.RecordBatch.from_arrays(arrowodata, self.col_names)
        except Exception:
            self.__logger.error("Cannot convert arrays to batch")
            raise
        return rbatch
