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
#
# Example for building a complete Artemis job
import uuid
import tempfile

from artemis.artemis import Artemis
from artemis.io.writer import BufferOutputWriter
from artemis.configurables.configurable import MenuBuilder
from artemis.generators.simutablegen import SimuTableGen
from artemis.io.filehandler import FileHandlerTool
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
from artemis.tools.csvtool import CsvTool

from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo
# from artemis.generators.filegen import FileGenerator

from cronus.io.protobuf.configuration_pb2 import Configuration
from cronus.io.protobuf.table_pb2 import Table
from cronus.core.Directed_Graph import Directed_Graph, Node
from cronus.io.protobuf.cronus_pb2 import MenuObjectInfo, ConfigObjectInfo
from cronus.io.protobuf.cronus_pb2 import TableObjectInfo
from cronus.core.cronus import BaseObjectStore

from artemis.core.book import ArtemisBook
from physt.io.protobuf.histogram_pb2 import HistogramCollection


def example_configuration(table_id, seed=42):
    # First define a data generator using SimuTable

    max_malloc = 2147483648  # Maximum memory allowed in Arrow memory pool
    max_buffer_size = 2147483648  # Maximum size serialized ipc message
    write_csv = True  # Output csv files for each arrow output file
    sample_ndatums = 1  # Preprocess job to sample files from dataset
    sample_nchunks = 10  # Preprocess job to sample chunks from a file
    linesep = '\r\n'   # Line delimiter to scan for on csv input
    delimiter = ","    # Field delimiter
    blocksize = 2**16  # Size of chunked data in-memory
    header = ''        # Predefined header
    footer = ''        # Predefined footer
    header_offset = 0  # N bytes to scan past header
    footer_size = 0    # N bytes size of footer
    schema = []        # Predefined list of field names on input
    encoding = 'utf8'  # encoding
    gen_nbatches = 5  # Number of batches to generator
    gen_nrows = 1000  # Number of rows per batch

    config = Configuration()  # Cronus Configuration message
    config.uuid = str(uuid.uuid4())
    config.name = f"{config.uuid}.config.pb"
    config.max_malloc_size_bytes = max_malloc

    generator = SimuTableGen('generator',
                             nbatches=gen_nbatches,
                             num_rows=gen_nrows,
                             file_type=1,  # Output type cronus.proto filetype
                             table_id=table_id,
                             seed=seed)

    # Set the generator configuration
    config.input.generator.config.CopyFrom(generator.to_msg())

    filehandler = FileHandlerTool('filehandler',
                                  filetype='csv',  # TBD use filetype metadata
                                  blocksize=blocksize,
                                  delimiter=delimiter,
                                  linesep=linesep,
                                  header=header,
                                  footer=footer,
                                  header_offset=header_offset,
                                  footer_size=footer_size,
                                  schema=schema,
                                  encoding=encoding,
                                  seed=seed)
    # Add to the tools
    csvtool = CsvTool('csvtool', block_size=(2 * blocksize))
    csvtoolmsg = config.tools.add()
    csvtoolmsg.CopyFrom(csvtool.to_msg())

    fhtoolmsg = config.tools.add()
    fhtoolmsg.CopyFrom(filehandler.to_msg())
    writer = BufferOutputWriter('bufferwriter',
                                BUFFER_MAX_SIZE=max_buffer_size,
                                write_csv=write_csv)
    writertoolmsg = config.tools.add()
    writertoolmsg.CopyFrom(writer.to_msg())

    sampler = config.sampler
    sampler.ndatums = sample_ndatums
    sampler.nchunks = sample_nchunks

    return config


class ExampleMenu(MenuBuilder):
    def __init__(self, name='test'):
        super().__init__(name)

    def _algo_builder(self):
        '''
        define all algorithms required
        '''
        self._algos['testalgo'] = DummyAlgo1('dummy',
                                             myproperty='ptest',
                                             loglevel='INFO')
        self._algos['csvalgo'] = CsvParserAlgo('csvparser', loglevel='INFO')
        self._algos['profileralgo'] = ProfilerAlgo('profiler', loglevel='INFO')

    def _seq_builder(self):
        # Define the sequences and node names
        self._seqs['seqX'] = Node(["initial"],
                                  ('csvparser',),
                                  "seqX")
        self._seqs['seqY'] = Node(["seqX"],
                                  ('profiler',),
                                  "seqY")

    def _chain_builder(self):
        # Add the sequences to a chain
        self._chains['csvchain'] = Directed_Graph("csvchain")
        self._chains['csvchain'].add(self._seqs['seqX'])
        self._chains['csvchain'].add(self._seqs['seqY'])


def example_table():
    # define the schema for the data
    table = Table()
    table.name = 'EvolveModel'
    table.uuid = str(uuid.uuid4())
    # Tables and Files have a partition key
    # This allows relating both to a partition
    # And to relate a table to a file
    schema = table.info.schema.info

    field1 = schema.fields.add()
    field1.name = 'record_id'
    field1.info.type = 'String'
    field1.info.length = 10

    field2 = schema.fields.add()
    field2.name = 'Name'
    field2.info.type = 'String'
    field2.info.length = 10
    field2.info.aux.generator.name = 'name'

    field3 = schema.fields.add()
    field3.name = 'SIN'
    field3.info.type = 'String'
    field3.info.length = 10
    field3.info.aux.generator.name = 'ssn'

    field4 = schema.fields.add()
    field4.name = 'StreetNumber'
    field4.info.type = 'String'
    field4.info.length = 40
    field4.info.aux.generator.name = 'building_number'

    field5 = schema.fields.add()
    field5.name = 'Street'
    field5.info.type = 'String'
    field5.info.length = 40
    field5.info.aux.generator.name = 'street_name'

    field6 = schema.fields.add()
    field6.name = 'City'
    field6.info.type = 'String'
    field6.info.length = 40
    field6.info.aux.generator.name = 'city'

    field7 = schema.fields.add()
    field7.name = 'Province'
    field7.info.type = 'String'
    field7.info.length = 40
    field7.info.aux.generator.name = 'province'

    field8 = schema.fields.add()
    field8.name = 'PostalCode'
    field8.info.type = 'String'
    field8.info.length = 40
    field8.info.aux.generator.name = 'postcode'

    field9 = schema.fields.add()
    field9.name = 'DOB'
    field9.info.type = 'DateTime'
    field9.info.length = 40
    field9.info.aux.generator.name = 'date'

    field10 = schema.fields.add()
    field10.name = 'PhoneNum'
    field10.info.type = 'String'
    field10.info.length = 11
    field10.info.aux.generator.name = 'phone_number'

    return table


def example_job():
    # Artemis Job requirements
    # BaseObjectStore - name, path and id
    # Menu 
    # Configuration
    # Input Dataset
    # Dataset partitions
    # Table schemas for each dataset partition
    
    # Build the Menu
    mb = ExampleMenu()
    msgmenu = mb.build()
    menuinfo = MenuObjectInfo()
    menuinfo.created.GetCurrentTime()
    
    table = example_table()
    # Build the Configuration

    # Build the partition Table schemas
    
    # Register all inputs in the Cronus object store

    # Build the job
    # To use the local directory:
    # dirpath = os.getcwd()
    with tempfile.TemporaryDirectory() as dirpath:
        # All jobs now require an object store
        # All outputs are pesisted in the object store path
        # See github.com/mbr/simplekv
        # Factory class for simplekv provided by
        # blueyonder/storefact
        store = BaseObjectStore(dirpath, 'artemis')
        
        # Requires registering an parent dataset
        # Generator data is written to disk with 
        # The parent dataset uuid
        # Register the 'generator' partition -- required
        
        g_dataset = store.register_dataset()
        store.new_partition(g_dataset.uuid, 'generator')
        job_id = store.new_job(g_dataset.uuid)
        
        # The table schema which defines the model for the generator
        # Persisted first to the object store
        # protobuf file
        tinfo = TableObjectInfo()
        table_id = store.register_content(table,
                                          tinfo,
                                          dataset_id=g_dataset.uuid,
                                          job_id=job_id,
                                          partition_key='generator').uuid

        store.save_store()

        # Now configure all tools and algorithms
        # Includes IO tools
        config = example_configuration(table_id)
        # Algorithms need to added from the menu to the configuration
        for key in mb._algos:
            msg = config.algos.add()
            msg.CopyFrom(mb._algos[key].to_msg())

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()

        # Store the menu and configuration protobufs
        menu_uuid = store.register_content(msgmenu, menuinf ig_uuid)
        store.save_store()

        # Now define the actual Artemis job
        # Again the input is a protobuf
        # All other information read in from the
        # object store
        job = JobInfo_pb()
        job.name = 'arrowproto'
        job.store_id = store.store_uuid
        job.store_name = store.store_name
        job.store_path = dirpath
        job.menu_id = menu_uuid
        job.config_id = config_uuid
        job.dataset_id = dataset.uuid
        job.parentset_id = g_dataset.uuid
        job.job_id = str(job_id)
        bow = Artemis(job, loglevel='INFO')
        bow.control()
        bow._jp.store.save_store()
        
        validate_job(dirpath, store.store_name, store.store_uuid, dataset.uuid)


def validate_job(dirpath, store_name, store_id, dataset_id):
    
    # For reading, may be required to use urllib 
    # to convert a location to a normal file
    # path, then use pa.ipc.open_file(Path_arrow_table)
    # 
    store = BaseObjectStore(dirpath,
                            store_name,
                            store_id)
                            
    # Take a look at the dataset information
    # The dataset metadata is retained in the store
    # Not as seperate persisted data
    # Need some helper functions here
    print(store[dataset_id])
    

    # Get a list of the output arrow files
    # Can be used to read back in the arrow tables  
    files = store.list(prefix=dataset_id, suffix='arrow')
    for f in files:
        print(f)
    
    # get a list of output tables corresponding to the arrow output data
    # type should correspond to the arrow inferred data type
    tables = store.list(prefix=dataset_id, suffix='table.pb')

    print(tables)
    for t in tables:
        tmptbl = Table()
        store.get(t.uuid, tmptbl)
        print(tmptbl.info.schema)

    # get the output histogram
    hists = store.list(prefix=dataset_id, suffix='hist.pb')
    print(hists)
    coll = HistogramCollection()
    store.get(hists[-1].uuid, coll)
    hbook = ArtemisBook()
    hbook._from_message(coll)
    # See utils/hcollections.py
    # Use unpack_collection
    # create_groups
    # create pages
    # Include here as part of an example postprocessing step
    print(hbook.keys()) # Appears this is buggy, since the keys are empty



if __name__ == '__main__':
    example_job()
