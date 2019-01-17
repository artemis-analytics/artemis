#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

"""

"""

import pyarrow as pa
from artemis.core.tool import ToolBase

class MfTool(ToolBase):

    def __init__(self, name, **kwargs):
        defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            defaults[key] = kwargs[key]
        super().__init__(name, **defaults)
        self.__logger.info(defaults)
        _ropts = self._update_opts(ReadOptions, **defaults)
        _popts = self._update_opts(ParseOptions, **defaults)
        self.__logger.info("Configured ReadOptions: ")
        self.__logger.info(_ropts)
        self.__logger.info("Configured ParseOptions: ")
        self.__logger.info(_popts)
        self._readopts = ReadOptions(**_ropts)
        self._parseopts = ParseOptions(**_popts)
        self._convertopts = None # Coming in 0.12
        self.__logger.info('%s: __init__FileHandlerTool' % self.name)

    def _update_opts(self, cls, **kwargs):
        _updates = self._get_opts(cls())
        # Drop escape char from defaults
        if 'escape_char' in _updates.keys():
            del _updates['escape_char']
        for key in _updates:
            _updates[key] = kwargs[key]
        return _updates

    def _get_opts(self, opts):
        defaults = {}
        for attr in dir(opts):
            if attr.startswith("__"):
                continue
            defaults[attr] = getattr(opts, attr)
        return defaults

    def _set_defaults(self):
        ropts = self._get_opts(ReadOptions()) # Retrieve defaults from pyarrow
        popts = self._get_opts(ParseOptions())
        self.__logger.info("Default Read options")
        self.__logger.info(ropts)
        self.__logger.info("Default Parse options")
        self.__logger.info(popts)
        # Remove escape_char option, required to be None (False)
        del popts['escape_char']

        defaults = {**ropts, **popts}

        return defaults

    def initialize(self):
        self.__logger.info("%s properties: #s",
                           self.__class__.__name__,
                           self.properties)

    def execute(self, block):
        rsize = 20
        nrecords = 3
        csize = rsize * nrecords
        idata = "012345678aabcd01234m012345678babcd01234m012345678cabcd01234m012345678dabcd01234m012345678eabcd01234m012345678fabcd01234m012345678aabcd01234m012345678babcd01234m012345678cabcd01234m012345678dabcd01234m012345678eabcd01234m012345678fabcd01234m012345678aabcd01234m012345678babcd01234m012345678cabcd01234m"
        isize = len(idata)
        print(isize)
        schema = [10, 4, 6]
        odata = []
        arrowodata = []
        data_types = ['signedint', 'string', 'signedint']
        pos_chars = {'{':'0', 'a':'1', 'b':'2', 'c':'3', 'd':'4', 'e':'5', 'f':'6', 'g':'7', 'h':'8', 'i':'9'}
        neg_chars = {'j':'0', 'k':'1', 'l':'2', 'm':'3', 'n':'4', 'o':'5', 'p':'6', 'q':'7', 'r':'8', 's':'9'}

        for field in schema:
            odata.append([])

        icounter = 0
        ccounter = 0
        ncounter = 0
        fcounter = 0

        while icounter < isize:
            cdata = idata[icounter:(icounter + csize)] # Extract chunk.
            while ccounter < csize:
                rdata = cdata[ccounter: (ccounter + rsize)] # Extract record.
                while ncounter < nrecords:
                    record = rdata[fcounter:(fcounter + schema[ncounter])] # Extract field.
                    if data_types[ncounter] == 'signedint':
                        if record[-1:] in pos_chars:
                            record = int(record.replace(record[-1:], pos_chars[record[-1:]]))
                        else:
                            record = record.replace(record[-1:], neg_chars[record[-1:]])
                            record = int('-' + record)
                        odata[ncounter].append(record)
                    elif data_types[ncounter] == 'string':
                        odata[ncounter].append(record)
                    fcounter = fcounter + schema[ncounter]
                    ncounter = ncounter + 1
                ncounter = 0
                fcounter = 0
                ccounter = ccounter + rsize
            icounter = icounter + csize
            ccounter = 0

        counter = 0
        for my_list in odata:
            if data_types[counter] == 'signedint':
                arrowodata.append(pa.array(my_list))
            else:
                arrowodata.append(pa.array(my_list))
            counter = counter + 1

        print('Output data lists.')
        print(odata)

        print('Output data arrow arrays.')
        print(arrowodata)
        
