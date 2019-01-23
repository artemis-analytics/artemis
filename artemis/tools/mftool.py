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

    def execute(self, block):
        rsize = 20
        nrecords = 3
        csize = rsize * nrecords
        idata = "012345678aabcd01234m012345678babcd01234m"\
            + "012345678cabcd01234m012345678dabcd01234m"\
            + "012345678eabcd01234m012345678fabcd01234m"\
            + "012345678aabcd01234m012345678babcd01234m"\
            + "012345678cabcd01234m012345678dabcd01234m"\
            + "012345678eabcd01234m012345678fabcd01234m"\
            + "012345678aabcd01234m012345678babcd01234m"\
            + "012345678cabcd01234m"
        isize = len(idata)
        print(isize)
        schema = [10, 4, 6]
        odata = []
        arrowodata = []
        data_types = ['signedint', 'string', 'signedint']
        pos_char = {'{': '0', 'a': '1', 'b': '2', 'c': '3', 'd': '4',
                    'e': '5', 'f': '6', 'g': '7', 'h': '8', 'i': '9'}
        neg_char = {'j': '0', 'k': '1', 'l': '2', 'm': '3', 'n': '4',
                    'o': '5', 'p': '6', 'q': '7', 'r': '8', 's': '9'}

        for field in schema:
            odata.append([])

        icounter = 0
        ccounter = 0
        ncounter = 0
        fcounter = 0

        while icounter < isize:
            # Extract chunk.
            cdata = idata[icounter:(icounter + csize)]
            while ccounter < csize:
                # Extract record.
                rdata = cdata[ccounter: (ccounter + rsize)]
                while ncounter < nrecords:
                    # Extract field.
                    record = rdata[fcounter:(fcounter + schema[ncounter])]
                    if data_types[ncounter] == 'signedint':
                        if record[-1:] in pos_char:
                            record = int(record.replace(record[-1:],
                                                        pos_char[record[-1:]]))
                        else:
                            record = record.replace(record[-1:],
                                                    neg_char[record[-1:]])
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
