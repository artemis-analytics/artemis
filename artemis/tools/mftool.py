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

    def execute(self, ds_schema, block):
        idata = block
        isize = len(idata)
        print(isize)
        odata = []
        arrowodata = []
        test_ds = ds_schema
        nrecords = len(test_ds)
        rsize = 0
        for ds in test_ds:
            rsize = rsize + ds['length']
        csize = rsize * nrecords

        pos_char = {'{': '0', 'a': '1', 'b': '2', 'c': '3', 'd': '4',
                    'e': '5', 'f': '6', 'g': '7', 'h': '8', 'i': '9'}
        neg_char = {'j': '0', 'k': '1', 'l': '2', 'm': '3', 'n': '4',
                    'o': '5', 'p': '6', 'q': '7', 'r': '8', 's': '9'}

        for field in test_ds:
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
                    record = rdata[fcounter:
                                   (fcounter + test_ds[ncounter]['length'])]
                    if test_ds[ncounter]['utype'] == 'int':
                        if record[-1:] in pos_char:
                            record = int(record.replace(record[-1:],
                                                        pos_char[record[-1:]]))
                        else:
                            record = record.replace(record[-1:],
                                                    neg_char[record[-1:]])
                            record = int('-' + record)
                        odata[ncounter].append(record)
                    elif test_ds[ncounter]['utype'] == 'str':
                        odata[ncounter].append(record.strip())
                    fcounter = fcounter + test_ds[ncounter]['length']
                    ncounter = ncounter + 1
                ncounter = 0
                fcounter = 0
                ccounter = ccounter + rsize
            icounter = icounter + csize
            ccounter = 0

        for my_list in odata:
            arrowodata.append(pa.array(my_list))

        print('Output data lists.')
        print(odata)

        print('Output data arrow arrays.')
        print(arrowodata)
