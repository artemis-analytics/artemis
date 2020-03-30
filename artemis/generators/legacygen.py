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
Classes for generating legacy (mainframe) like data
"""
import string
import tempfile
import pyarrow as pa

from artemis.decorators import iterable
from artemis.generators.common import GeneratorBase
from artemis.io.protobuf.cronus_pb2 import FileObjectInfo


@iterable
class GenMFOptions:
    """
    Class to hold dictionary of required options
    """

    # seed = 42
    nbatches = 1
    nsamples = 1
    num_rows = 10
    pos_char = {
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
    neg_char = {
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
    header = ""
    header_offset = 0
    footer = ""
    footer_size = 0


class GenMF(GeneratorBase):
    """
    Generator for mainframe style data.

    Generates specific number of records and columns.
    """

    def __init__(self, name, **kwargs):
        """
        Generator parameters. Configured once per instantiation.
        """

        options = dict(GenMFOptions())
        options.update(kwargs)

        super().__init__(name, **options)

        if hasattr(self.properties, "ds_schema"):
            self.ds_schema = self.properties.ds_schema
        else:
            self.ds_schema = []
            for key in options:
                if "column" in key:
                    self.ds_schema.append(options[key])

        self.num_rows = self.properties.num_rows
        self.nsamples = self.properties.nsamples

        # Specific characters used for encoding signed integers.
        self.pos_char = self.properties.pos_char
        self.neg_char = self.properties.neg_char

        # Meta data
        self.header = self.properties.header
        self.header_offset = self.properties.header_offset
        self.footer = self.properties.footer
        self.footer_size = self.properties.footer_size

    def gen_column(self, dataset, size):
        """
        Creates a column of data. The number of records is size.
        """
        rand_col = []

        #  Create data of specific unit types.
        if dataset["utype"] == "int":
            # Creates a column of "size" records of integers.
            for i in range(size):
                dpoint = self.random_state.randint(
                    dataset["min_val"], dataset["max_val"]
                )
                if dpoint < 0:
                    # Convert negative integers.
                    dpoint = str(dpoint)
                    dpoint = dpoint.replace("-", "")
                    dpoint = dpoint.replace(dpoint[-1], self.neg_char[dpoint[-1:]])
                else:
                    # Convert positive integers.
                    dpoint = str(dpoint)
                    dpoint = dpoint.replace(dpoint[-1], self.pos_char[dpoint[-1:]])
                # Print to be converted to logger if appropriate.
                self.__logger.debug("Data pointi: " + dpoint)
                dpoint = ("0" * (dataset["length"] - len(dpoint))) + dpoint
                self.__logger.debug("Data pointiw: " + dpoint)
                rand_col.append(dpoint)
        elif dataset["utype"] == "uint":
            # Creates a column of "size" records of unsigned ints.
            for i in range(size):
                dpoint = self.random_state.randint(
                    dataset["min_val"], dataset["max_val"]
                )
                dpoint = str(dpoint)
                self.__logger.debug("Data pointu: " + dpoint)
                dpoint = ("0" * (dataset["length"] - len(dpoint))) + dpoint
                self.__logger.debug("Data pointuw: " + dpoint)
                rand_col.append(dpoint)
        else:
            # Creates a column of "size" records of strings.
            # Characters allowed in the string.
            source = (
                string.ascii_lowercase
                + string.ascii_uppercase
                + string.digits
                + string.punctuation
            )
            source = list(source)
            for i in range(size):
                dpoint = "".join(self.random_state.choice(source, dataset["length"]))
                self.__logger.debug("Data pointc: " + dpoint)
                dpoint = dpoint + (" " * (dataset["length"] - len(dpoint)))
                self.__logger.debug("Data pointcw: " + dpoint)
                rand_col.append(dpoint)

        self.__logger.debug(rand_col)
        return rand_col

    def pad_header(self):
        len_pad = self.header_offset - len(self.header)
        pad_type = " "
        return self.header + (pad_type * len_pad)

    def pad_footer(self):
        len_pad = self.footer_size - len(self.footer)
        pad_type = " "
        return self.footer + (pad_type * len_pad)

    def gen_chunk(self):
        """
        Generates a chunk of data as per configured instance.
        """

        header = self.pad_header()
        footer = self.pad_footer()

        chunk = header
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

        chunk = chunk + footer

        self.__logger.debug("Chunk: %s", chunk)
        # Encode data chunk in cp500.
        # Might want to make this configurable.
        chunk = chunk.encode(encoding="cp500")
        self.__logger.debug("Chunk ebcdic: %s", chunk)

        return chunk

    def generate(self):
        while self._nbatches > 0:
            self.__logger.info("%s: Generating datum " % (self.__class__.__name__))
            data = self.gen_chunk()
            self.__logger.debug(
                "%s: type data: %s" % (self.__class__.__name__, type(data))
            )
            yield data
            self._nbatches -= 1
            self.__logger.debug("Batch %i", self._nbatches)

    def sampler(self):
        while self.nsamples > 0:
            self.__logger.info("%s: Generating datum " % (self.__class__.__name__))
            data = self.gen_chunk()
            self.__logger.debug(
                "%s: type data: %s" % (self.__class__.__name__, type(data))
            )
            fileinfo = FileObjectInfo()
            fileinfo.type = 2
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

    def __next__(self):
        next(self._batch_iter)
        data = self.gen_chunk()
        fileinfo = FileObjectInfo()
        fileinfo.type = 2
        fileinfo.partition = self.name
        job_id = f"{self.gate.meta.job_id}_batch_{self._batchidx}"
        ds_id = self.gate.meta.parentset_id
        id_ = self.gate.store.register_content(
            data, fileinfo, dataset_id=ds_id, partition_key=self.name, job_id=job_id
        ).uuid
        buf = pa.py_buffer(data)
        self.gate.store.put(id_, buf)
        self._batchidx += 1
        return id_
        # return self.gen_chunk()

    def write(self):
        self.__logger.info("Batch %i", self._nbatches)
        iter_ = self.generate()
        while True:
            try:
                raw = next(iter_)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                break
            except Exception:
                self.__logger.info("Iterator empty")
                raise

            filename = tempfile.mktemp(
                suffix=self.properties.suffix,
                prefix=self.properties.prefix,
                dir=self.properties.path,
            )
            self.__logger.info("Write file %s", filename)
            with open(filename, "wb") as f:
                f.write(raw)
