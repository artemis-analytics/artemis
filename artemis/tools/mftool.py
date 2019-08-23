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
import numpy as np
from artemis.externals import ebcdic

from artemis.decorators import iterable
from artemis.core.tool import ToolBase


@iterable
class MfToolOptions:
    '''
    Class to hold dictionary of required options
    '''
    pos_char = {'0': '{', '1': 'A',
                '2': 'B', '3': 'C', '4': 'D',
                '5': 'E', '6': 'F', '7': 'G',
                '8': 'H', '9': 'I'}
    neg_char = {'0': '}', '1': 'J', '2': 'K',
                '3': 'L', '4': 'M',
                '5': 'N', '6': 'O', '7': 'P',
                '8': 'Q', '9': 'R'}
    codec = 'cp500'


class MfTool(ToolBase):
    '''
    The class that deals with mainframe files.
    '''

    def __init__(self, name, **kwargs):
        '''
        Generator parameters. Configured once per instantiation.
        '''
        options = dict(MfToolOptions())
        options.update(kwargs)

        super().__init__(name, **options)

        self.col_names = []
        if hasattr(self.properties, 'ds_schema'):
            self.ds_schema = self.properties.ds_schema
            for i, column in enumerate(self.ds_schema):
                self.col_names.append('column_' + str(i))
        else:
            self.ds_schema = []
            for key in options:
                if 'column' in key:
                    self.ds_schema.append(options[key])
                    name = key.split('_')[-1]
                    self.col_names.append(name)

        self.nfields = len(self.ds_schema)
        self.rsize = 0
        for ds in self.ds_schema:
            self.rsize = self.rsize + ds['length']

        # Specific characters used for encoding signed integers.
        # Need to swap key,value from generator dict
        self.pos_char = dict((v, k)
                             for k, v in self.properties.pos_char.items())
        self.neg_char = dict((v, k)
                             for k, v in self.properties.neg_char.items())

        self.codec = self.properties.codec

        self._nbatches = 0

    def initialize(self):
        self.__logger.info("Ignored codecs")
        self.__logger.info(ebcdic.ignored_codec_names())

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
        self.__logger.debug("Processing batch %i", self._nbatches)
        # The block is decoded from the cp500 code page.
        block = block.decode(self.codec)

        isize = len(block)
        self.__logger.debug("Block size to process %i", isize)
        odata = []
        arrowodata = []

        # Create a list of empty lists for the number of columns.
        for field in self.ds_schema:
            odata.append([])

        ccounter = 0
        ncounter = 0
        fcounter = 0

        while ccounter < isize:
            # Extract record.
            rdata = block[ccounter: (ccounter + self.rsize)]
            while ncounter < self.nfields:
                # Extract field.
                field = rdata[fcounter:
                              (fcounter + self.ds_schema[ncounter]['length'])]
                # Processes each field according to the datatype.
                if self.ds_schema[ncounter]['utype'] == 'int':
                    # Replacing the end character with a proper digit requires
                    # differentiating between negative and positive numbers.
                    if field[-1:] in self.pos_char:
                        # Padding zeroes are taken removed by type conversion.
                        try:
                            cnvfield = int(field.replace(field[-1:],
                                           self.pos_char[field[-1:]]))
                        except Exception:
                            self.__logger.error("Cannot parse int field")
                            self.__logger.error("Record %i Field %i Value %s ",
                                                ccounter, ncounter, field)
                            self.__logger.error("Record %i: %s",
                                                ccounter, rdata)
                            raise
                    else:
                        try:
                            cnvfield = field.replace(field[-1:],
                                                     self.neg_char[field[-1:]])
                        except Exception:
                            self.__logger.error("Cannot parse int field")
                            self.__logger.error("Record %i Field %i Value %s ",
                                                ccounter, ncounter, field)
                            self.__logger.error("Record %i: %s",
                                                ccounter, rdata)
                            raise
                        try:
                            cnvfield = int('-' + cnvfield)
                        except Exception:
                            self.__logger.error("Cannot parse int field")
                            self.__logger.error("Record %i Field %i Value %s ",
                                                ccounter, ncounter, field)
                            self.__logger.error("Record %i: %s",
                                                ccounter, rdata)
                            raise
                    cnvfield = float(cnvfield)
                    odata[ncounter].append(cnvfield)
                elif self.ds_schema[ncounter]['utype'] == 'str':
                    # Removes padding spaces from the data.
                    try:
                        cnvfield = field.strip()
                    except Exception:
                        self.__logger.error("Cannot parse str field")
                        self.__logger.error("Record %i Field %i Value %s ",
                                            ccounter, ncounter, field)
                        self.__logger.error("Record %i: %s", ccounter, rdata)
                        raise

                    odata[ncounter].append(cnvfield)
                elif self.ds_schema[ncounter]['utype'] == 'uint':
                    try:
                        cnvfield = int(field)
                        cnvfield = float(field)
                    except ValueError:
                        self.__logger.debug("Cannot parse uint field")
                        self.__logger.debug("Record %i Field %i Value %s ",
                                            ccounter, ncounter, field)
                        try:
                            cnvfield = str(field)
                        except Exception:
                            self.__logger.error("Cannot parse uint as str")
                            self.__logger.eror("Record %i Field %i Value %s ",
                                               ccounter, ncounter, field)
                            self.__logger.error("Record %i: %s",
                                                ccounter, rdata)
                            raise
                        if cnvfield.isspace():
                            self.__logger.debug("null, convert to zero")
                            #  TODO determine correct value for empty fields???
                            cnvfield = np.nan
                    except Exception:
                        self.__logger.error("Cannot parse uint field")
                        self.__logger.error("Record %i Field %i Value %s ",
                                            ccounter, ncounter, field)
                        self.__logger.error("Record %i: %s", ccounter, rdata)
                        raise
                    odata[ncounter].append(cnvfield)
                fcounter = fcounter + self.ds_schema[ncounter]['length']
                ncounter = ncounter + 1
            ncounter = 0
            fcounter = 0
            ccounter = ccounter + self.rsize

        # Validate lists
        if len(odata) != self.nfields:
            self.__logger.error("Number of parsed fields not equal schema")
            raise ValueError

        #  TODO
        #  Pass the number of records per block to parse
        #  How to handle last block?
        #  Or validate equal arrays before passing to arrow?
        # for i, my_list in enumerate(odata):
        #     if len(my_list) != block_size:
        #         self.__logger.error("Field column has
        #  incorrect number of records Field %i length %i", i, len(my_list))

        # Creates apache arrow dataset.

        for my_list in odata:
            arr = pa.array(my_list)
            if arr.type == pa.null():
                self.__logger.warning("Null array recast as float")
                arr = pa.array(my_list, type=pa.float64())
            arrowodata.append(arr)

        self.__logger.debug('Output data lists.')
        self.__logger.debug(odata)

        self.__logger.debug('Output data arrow arrays.')
        self.__logger.debug(arrowodata)

        try:
            rbatch = pa.RecordBatch.from_arrays(arrowodata, self.col_names)
        except Exception:
            self.__logger.error("Cannot convert arrays to batch")
            raise
        self._nbatches += 1
        return rbatch
