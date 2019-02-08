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
import datetime
from google.protobuf import text_format
from physt.io.protobuf import read
from physt.io.protobuf.histogram_pb2 import HistogramCollection

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

from artemis.logger import Logger
from artemis.protos.menu_pb2 import JobInfo


@Logger.logged
class HCollections():
    '''
    Reads Artemis protobuf histograms
    Converts to a multipage Pdf
    '''
    def __init__(self, fname):
        self.collection = HistogramCollection()
        self.jobinfo = JobInfo()
        self.filename = fname

        try:
            self._get_data()
        except Exception:
            self.__logger.error("Cannot open file")
            raise
        self.collection.CopyFrom(self.jobinfo.summary.collection)
        print(text_format.MessageToString(self.jobinfo))

        self.hists = self._unpack_collection()
        self.hgroups = self._create_hgroups()

    def _get_data(self):
        try:
            with open(self.filename, 'rb') as f:
                self.jobinfo.ParseFromString(f.read())
        except IOError:
            self.__logger.error("Cannot read collections")
        except Exception:
            raise

    def _unpack_collection(self):
        hists = {name: read(value) for name, value in
                 self.collection.histograms.items()}

        return hists

    def _create_hgroups(self):

        hgroups = {}
        for key in self.hists.keys():
            algo = key.split('.')[0]
            if algo in hgroups.keys():
                hgroups[algo].append(key)
            else:
                hgroups[algo] = []
                hgroups[algo].append(key)

        print(hgroups)
        return hgroups

    def create_pages(self):
        oname = self.filename + '.pdf'
        with PdfPages(oname) as pdf:
            for key in self.hgroups:
                nplots = len(self.hgroups[key])
                for i in range(nplots):
                    i += 1
                    if nplots % i == 0:
                        break
                if(i == 1):
                    i += 1
                ncols = i
                nrows = nplots // ncols
                nrows += nplots % ncols
                print(nrows, ncols)
                plt.rc('text', usetex=True)
                # fig, axes = plt.subplots(nrows, ncols)
                fig = plt.figure(1)
                pos = range(1, nplots+1)
                for k, item in enumerate(self.hgroups[key]):
                    print(k, self.hists[item])
                    axe = fig.add_subplot(nrows, ncols, pos[k])
                    self.hists[item].plot(ax=axe)
                    axe.set_title(str(item).replace('_', ''))
                plt.title(key)
                pdf.savefig(fig)  # can pass a Figure object to pdf.savefig
                plt.close()

            # We can also set the file's metadata via the PdfPages object:
            d = pdf.infodict()
            d['Title'] = 'Artemis Monitoring'
            d['Author'] = u'Ryan M White'
            d['Subject'] = 'Data monitoring and validation'
            d['Keywords'] = 'PdfPages multipage keywords author title subject'
            d['CreationDate'] = datetime.datetime(2018, 10, 31)


if __name__ == '__main__':
    fname = 'arrowproto_info.dat'

    collections = HCollections(fname)
    collections.create_pages()
