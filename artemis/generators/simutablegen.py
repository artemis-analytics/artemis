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
Generator algo for SimuTable

"""
import io
import pyarrow as pa

from artemis_format.pymodels.cronus_pb2 import FileObjectInfo
from artemis_format.pymodels.table_pb2 import Table

from artemis_base.utils.logger import Logger
from artemis_base.utils.decorators import iterable

from artemis.generators.common import GeneratorBase

from dolos.simutable.synthesizer import Synthesizer


@iterable
class SimuTableGenOptions:
    nbatches = 1
    nsamples = 1
    num_rows = 10
    file_type = 1
    codec = "utf8"
    linesep = "\r\n"


@Logger.logged
class SimuTableGen(GeneratorBase):
    """
    """

    def __init__(self, name, **kwargs):
        print("Initialize SimuTableGen")
        options = dict(SimuTableGenOptions())
        options.update(kwargs)

        super().__init__(name, **options)
        self.table_id = self.properties.table_id
        self.table = Table()
        self.num_rows = self.properties.num_rows
        self.linesep = self.properties.linesep
        # self.header = self.properties.header
        self.nsamples = self.properties.nsamples
        self.file_type = self.properties.file_type
        self.codec = self.properties.codec

        self.synthesizer = None
        self.num_cols = None
        self.write_batch = None
        self.header = None

        # FWF
        self.pos_char = {
            "0": "{",
            "1": "A",
            "2": "B",
            "3": "C",
            "4": "D",
            "5": "E",
            "6": "F",
            "7": "G",
            "8": "H",
            "9": "I",
        }
        self.neg_char = {
            "0": "}",
            "1": "J",
            "2": "K",
            "3": "L",
            "4": "M",
            "5": "N",
            "6": "O",
            "7": "P",
            "8": "Q",
            "9": "R",
        }
        # header = ''
        self.header_offset = 0
        self.footer = ""
        self.footer_size = 0

        self.__logger.info("Initialized %s", self.__class__.__name__)
        self.__logger.info(
            "%s properties: %s", self.__class__.__name__, self.properties
        )
        print("Initialize SimuTableGen")

    @property
    def num_batches(self):
        return self._nbatches

    @num_batches.setter
    def num_batches(self, n):
        self._nbatches = n

    def initialize(self):
        self.__logger.info("Initialize SimuTableGenerator")
        self.gate.store.get(self.table_id, self.table)
        self.num_cols = len(self.table.info.schema.info.fields)
        names = []
        for field in self.table.info.schema.info.fields:
            names.append(field.name)
        self.header = names

        if hasattr(self.properties, "seed"):
            self.synthesizer = Synthesizer(
                self.table, "en_CA", idx=0, seed=self.properties.seed
            )
        else:
            self.synthesizer = Synthesizer(self.table, "en_CA", idx=0)

        if self.file_type == 1:
            self.write_batch = self.write_batch_csv
        elif self.file_type == 2:
            self.write_batch = self.write_batch_fwf
        elif self.file_type == 5:
            self.write_batch = self.write_batch_arrow

    def chunk(self):
        """
        Allow for concurrent generate during write
        """
        for _ in range(self.num_rows):
            try:
                yield tuple(self.synthesizer.generate())
            except TypeError:
                self.__logger.error("Generator function must return list")
                raise
            except Exception:
                self.__logger.error("Unknown error in chunk")

    def sampler(self):
        while self.nsamples > 0:
            self.__logger.info("%s: Generating datum " % (self.__class__.__name__))
            data = self.write_batch()
            self.__logger.debug(
                "%s: type data: %s" % (self.__class__.__name__, type(data))
            )
            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.partition = self.name
            job_id = f"{self.gate.meta.job_id}_sample_{self.nsamples}"
            ds_id = self.gate.meta.parentset_id
            id_ = self.gate.store.register_content(
                data, fileinfo, dataset_id=ds_id, partition_key=self.name, job_id=job_id
            ).uuid
            buf = pa.py_buffer(data)
            self.gate.store.put(id_, buf)
            yield id_
            self.nsamples -= 1
            self.__logger.debug("Batch %i", self.nsamples)

    def fwf_encode_row(self, row):
        record = ""
        #  Create data of specific unit types.
        fields = list(self.table.info.schema.fields)
        for i, dpoint in enumerate(row):
            # encode
            # pad to field width
            # append to record
            field_schema = fields[i]

            dpoint = str(dpoint)
            # signed integers require encoding
            # all other fields expected to be string-like
            if field_schema["utype"] == "int":
                if dpoint < 0:
                    # Convert negative integers.
                    dpoint = dpoint.replace("-", "")
                    dpoint = dpoint.replace(dpoint[-1], self.neg_char[dpoint[-1:]])
                else:
                    # Convert positive integers.
                    dpoint = dpoint.replace(dpoint[-1], self.pos_char[dpoint[-1:]])

            # ensure generated field is within schema length
            dpoint = dpoint[: field_schema["length"]]

            # pad up to required length
            if field_schema["utype"] == "int" or field_schema["utype"] == "uint":
                dpoint = ("0" * (field_schema["length"] - len(dpoint))) + dpoint
            else:
                dpoint = dpoint + (" " * (field_schema["length"] - len(dpoint)))

            # append field to record
            record += dpoint

        return record

    def write_batch_fwf(self):
        """
        Generate a batch of records
        convert rows to fixed width fields
        encode to ascii format in bytes
        """
        fwf = io.StringIO()
        for row in list(self.chunk()):
            if len(row) != len(self.header):
                raise ValueError
            fwf.write(self.fwf_encode_row(row))

        fwf = fwf.getvalue().encode(self.codec)
        return pa.py_buffer(fwf)

    def write_batch_csv(self):
        """
        Generate batch of records
        encode to csv in bytes
        """
        csv = io.StringIO()
        if self.header:
            csv.write(",".join(self.header))
            csv.write(self.linesep)
        for row in list(self.chunk()):
            csv.write(",".join(map(str, row)))
            csv.write(self.linesep)

        csv = csv.getvalue().encode(self.codec)
        return pa.py_buffer(csv)

    def write_batch_arrow(self):
        """
        Generate a batch of records
        convert to pyarrow arrays
        convert to RecordBatch
        """
        data = list(self.chunk())
        data = zip(*data)
        arrays = []
        for i, column in enumerate(data):
            arrays.append(pa.array(column, self.pa_schema[i].type))

        batch = pa.RecordBatch.from_arrays(arrays, names=self.pa_schema.names)
        return batch

    def write_csv(self):
        """
        Write n chunks to csv
        Write file to disk
        """
        csv = b""

        while (len(csv) // 1024 ** 2) < self.maxfilesize:
            csv += self.write_batch_csv()
            if self.checkcount():
                break
        return csv

    def write_fwf(self):
        """
        Write fwf with all records
        """
        fwf = b""

        while (len(fwf) // 1024 ** 2) < self.maxfilesize:
            fwf += self.write_batch_fwf()
            if self.checkcount():
                break
        return fwf

    def write_recordbatchfile(self):
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, self.pa_schema)

        batches_size = 0
        while (batches_size // 1024 ** 2) < self.maxfilesize:
            batch = self.write_batch_arrow()
            batches_size += pa.get_record_batch_size(batch)
            writer.write_batch(batch)
            if self.checkcount():
                break

        writer.close()
        buf = sink.getvalue()
        return buf

    def __next__(self):
        next(self._batch_iter)
        self.__logger.info("%s: Generating datum " % (self.__class__.__name__))
        data = self.write_batch()
        self.__logger.debug("%s: type data: %s" % (self.__class__.__name__, type(data)))
        fileinfo = FileObjectInfo()
        fileinfo.type = 1
        fileinfo.partition = self.name
        job_id = f"{self.gate.meta.job_id}_batch_{self._batchidx}"
        ds_id = self.gate.meta.parentset_id
        id_ = self.gate.store.register_content(
            data, fileinfo, dataset_id=ds_id, partition_key=self.name, job_id=job_id
        ).uuid
        try:
            self.gate.store.put(id_, data)
        except IOError:
            self.__logger.error("Unable to write batch to store")
            raise
        except ValueError:
            self.__logger.error("Unable to find batch key in store")
            raise
        except Exception:
            self.__logger.error("Unknown error writing batch to store")
            raise

        self._batchidx += 1
        return id_
