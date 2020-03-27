#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""

"""
import datetime
import sys

from physt.io.protobuf import read
from physt.io.protobuf.histogram_pb2 import HistogramCollection

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

from artemis.logger import Logger
from artemis.io.protobuf.artemis_pb2 import JobInfo


@Logger.logged
class HCollections:
    """
    Reads Artemis protobuf histograms
    Converts to a multipage Pdf
    """

    def __init__(self, fname, hname):
        self.collection = HistogramCollection()
        self.jobinfo = JobInfo()
        self.filename = fname
        self.hname = hname

        try:
            self._get_data()
        except Exception:
            self.__logger.error("Cannot open file")
            raise
        # self.collection.CopyFrom(self.jobinfo.summary.collection)
        # print(text_format.MessageToString(self.jobinfo))

        self.hists = self._unpack_collection()
        self.hgroups = self._create_hgroups()

    def _get_data(self):
        try:
            with open(self.filename, "rb") as f:
                self.jobinfo.ParseFromString(f.read())
        except IOError:
            print("Cannot read collections")
        except Exception:
            raise
        try:
            with open(self.hname, "rb") as f:
                self.collection.ParseFromString(f.read())
        except IOError:
            print("Cannot read collections")
        except Exception:
            raise

        print("Retrieve message")

    def _create_report(self):
        print("Generate Report")
        nrecords = 0
        for table in self.jobinfo.summary.tables:
            nrecords += table.num_rows
        text = "Job Summary"
        text += "\nTotal bytes processed: "
        text += str(self.jobinfo.summary.processed_bytes)
        text += "\nTotal files processed: "
        text += str(self.jobinfo.summary.processed_ndatums)
        text += "\nTotal output files produced "
        text += str(len(self.jobinfo.summary.tables))
        text += "\nTotal records "
        text += str(nrecords)
        text += "\nJob time " + str(self.jobinfo.summary.job_time.seconds)
        print(text)
        return text

    def _unpack_collection(self):

        hists = {
            name: read(value) for name, value in self.collection.histograms.items()
        }
        print("Unpacked collection")
        return hists

    def _create_hgroups(self):

        hgroups = {}
        for key in self.hists.keys():
            algo = key.split(".")[0]
            if algo in hgroups.keys():
                hgroups[algo].append(key)
            else:
                hgroups[algo] = []
                hgroups[algo].append(key)
        return hgroups

    def create_pages(self):
        oname = self.filename + ".pdf"
        with PdfPages(oname) as pdf:

            # Job Summary page
            # fig = plt.figure(figsize=(8.5, 11))
            # fig.text(0., 0.85, self._create_report(), size=10)
            # pdf.savefig(fig)
            # plt.close()
            for key in self.hgroups:
                nplots = len(self.hgroups[key])
                for i in range(nplots):
                    i += 1
                    if nplots % i == 0:
                        break
                if i == 1:
                    i += 1
                ncols = i
                nrows = nplots // ncols
                nrows += nplots % ncols
                # print(nrows, ncols)
                plt.rc("text", usetex=True)
                # fig, axes = plt.subplots(nrows, ncols)
                fig = plt.figure(figsize=(8.5, 11))
                pos = range(1, nplots + 1)
                for k, item in enumerate(self.hgroups[key]):
                    title = str(item).replace("_", "")
                    # print(k, item, title)
                    axe = fig.add_subplot(nrows, ncols, pos[k])
                    try:
                        self.hists[item].plot(ax=axe)
                    except Exception:
                        print("Problem with %s", item)
                    axe.set_title(title)
                pdf.savefig(fig)  # can pass a Figure object to pdf.savefig
                plt.close()

            # We can also set the file's metadata via the PdfPages object:
            d = pdf.infodict()
            d["Title"] = "Artemis Monitoring"
            d["Author"] = u"Ryan M White"
            d["Subject"] = "Data monitoring and validation"
            d["Keywords"] = "PdfPages multipage keywords author title subject"
            d["CreationDate"] = datetime.datetime(2018, 10, 31)


if __name__ == "__main__":
    fname = sys.argv[1]
    hname = sys.argv[2]

    try:
        collections = HCollections(fname, hname)
    except Exception as e:
        print(e)
    try:
        collections.create_pages()
    except Exception as e:
        print(e)
