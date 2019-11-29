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
#
# Example for building a complete Artemis job

# Tools
from artemis.tools.csvtool import CsvTool
from artemis.tools.filtercoltool import FilterColTool
from artemis.tools.tdigesttool import TDigestTool
from artemis.tools.xlstool import XlsTool

# Algorithms
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.filteralgo import FilterAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo

# Other requirements
import dask.delayed
import tempfile
import uuid
import urllib.parse
import logging
import click

from artemis.configurables.configurable import MenuBuilder
from artemis.distributed.job_builder import runjob
from artemis.generators.simutablegen import SimuTableGen
from artemis.io.protobuf.configuration_pb2 import Configuration
from artemis.io.protobuf.cronus_pb2 import (MenuObjectInfo, ConfigObjectInfo,
                                            TableObjectInfo, DatasetObjectInfo)
from artemis.io.protobuf.table_pb2 import Table
from artemis.io.filehandler import FileHandlerTool
from artemis.io.writer import BufferOutputWriter
from artemis.meta.cronus import BaseObjectStore
from artemis.meta.Directed_Graph import Directed_Graph, Node
from artemis.core.book import TDigestBook

# Validation/graphing code requirements
import numpy as np
import sys
import time
import matplotlib.pyplot as plt

from artemis.externals.tdigest.tdigest import TDigest
from scipy import interpolate
from scipy.stats import norm
# ------------------------------------------

logging.getLogger().setLevel(logging.INFO)
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
    config.tools[filehandler.name].CopyFrom(filehandler.to_msg())

    csvtool = CsvTool('csvtool', block_size=(2 * blocksize))
    config.tools[csvtool.name].CopyFrom(csvtool.to_msg())

    filtercoltool = FilterColTool('filtercoltool',
                                  columns=['record-id', 'SIN', 'DOB'])
    config.tools[filtercoltool.name].CopyFrom(filtercoltool.to_msg())
    
    writer = BufferOutputWriter('bufferwriter',
                                BUFFER_MAX_SIZE=max_buffer_size,
                                write_csv=write_csv)
    config.tools[writer.name].CopyFrom(writer.to_msg())
    
    tdigesttool = TDigestTool('tdigesttool')
    config.tools[tdigesttool.name].CopyFrom(tdigesttool.to_msg())

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
                                             loglevel='WARNING')
        self._algos['csvalgo'] = CsvParserAlgo('csvparser', loglevel='WARNING')
        self._algos['filteralgo'] = FilterAlgo('filter',
                                               loglevel='WARNING')
        self._algos['profileralgo'] = ProfilerAlgo('profiler',
                                               loglevel='WARNING')

    def _seq_builder(self):
        # Define the sequences and node names
        self._seqs['seqX'] = Node(["initial"],
                                  ('csvparser',),
                                  "seqX")
        self._seqs['seqY'] = Node(["seqX"],
                                  ('filter',),
                                  "seqY")
        self._seqs['seqA'] = Node(['seqX'],
                                  ('profiler'),
                                  'seqA')
        self._seqs['seqB'] = Node(['seqY'],
                                  ('dummy'),
                                  'seqB')

    def _chain_builder(self):
        # Add the sequences to a chain
        self._chains['csvchain'] = Directed_Graph("csvchain")
        self._chains['csvchain'].add(self._seqs['seqX'])
        self._chains['csvchain'].add(self._seqs['seqY'])
        self._chains['csvchain'].add(self._seqs['seqA'])
        self._chains['csvchain'].add(self._seqs['seqB'])

@click.command()
@click.option('--location', required = True, prompt = True, help = 'Path to .xlsx')
def example_job(location):
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

    # Read schema and generator names
    xlstool = XlsTool('xlstool', location=location)
    ds_schema = xlstool.execute(location)
    # Example job only have one table
    table = ds_schema.tables[0]
    
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
        menu_uuid = store.register_content(msgmenu, menuinfo).uuid
        config_uuid = store.register_content(config, configinfo).uuid

        # Register an output dataset
        dataset = store.register_dataset(menu_id=menu_uuid,
                                         config_id=config_uuid)
        #Copy metadata from xlstool
        store[dataset.uuid].dataset.aux.CopyFrom(ds_schema.dataset.aux)
        store.save_store()

        # Now define the actual Artemis job
        # Again the input is a protobuf
        # All other information read in from the
        # object store
        inputs = store.list(prefix=g_dataset.uuid)

        ds_results = []
        for _ in range(2):
            job_id = store.new_job(dataset.uuid)
            config = Configuration()
            store.get(config_uuid, config)
            for p in config.input.generator.config.properties.property:
                if p.name == 'glob':
                    p.value = dirpath.split('.')[-2]+'csv'
            store._put_message(config_uuid, config)
            store.get(config_uuid, config)

            ds_results.append(runjob(dirpath,
                                     store.store_name,
                                     store.store_uuid,
                                     menu_uuid,
                                     config_uuid,
                                     dataset.uuid,
                                     g_dataset.uuid,
                                     str(job_id)))

        results = dask.compute(*ds_results, scheduler='single-threaded')
        store.new_partition(dataset.uuid, 'seqA')
        store.new_partition(dataset.uuid, 'seqB')
        store.save_store()
        for buf in results:
            ds = DatasetObjectInfo()
            ds.ParseFromString(buf)
            store.update_dataset(dataset.uuid, buf)

        store.save_store()
        
        print ("============Dataset============")
        print (store[dataset.uuid].dataset)
        print ("=============Table=============")
        temp = Table()
        temp.ParseFromString(store.get(table_id))
        print (temp)

        tds = store.list_tdigests(dataset.uuid)

        evaluated = False

        # Printing the dataset with the tdigests from the book
        for td in tds:
            book = TDigestBook()
            url_data = urllib.parse.urlparse(td.address)
            location = urllib.parse.unquote(url_data.path)
            book = TDigestBook.load(location)

            # We now iterate over each of the TDigest books to create a plot
            
            digest_map = {}

            for key in book:
                print("Dataset name: " + str(key[0]))
                print("Dataset type: " + str(type(book[key[0]])))

                digest = TDigest()
                digest.update_centroids_from_list(book[key[0]].centroids_to_list())
                digest_map[key[0]] = digest
            
            for key, value in digest_map.items():
                
                # This ensures that only the digests that 
                if key == 'Normal' and not evaluated:

                    # Here we create the graphs from the TDigests
                    # key = digest name
                    # value = the TDigest object itself

                    # Timing
                    start_time = time.time()

                    # define the current digest
                    digest = value

                    show = True

                    if not isinstance(digest, TDigest):
                        raise TypeError("ERROR: TDigest expected")
                        
                    # Generate the percentiles used in method
                    # Generate the cdf at each percentile that will be used in this method
                    percentiles = []
                    cdf_percentiles = []
                    for i in range(101):
                        percentiles.append(digest.percentile(i))
                        cdf_percentiles.append(digest.cdf(percentiles[i]))

                    '''
                    For this plot we are using a 3 x 3 plot with subplots
                    '''
                    plt.subplot(3, 3, 1)
                    plt.plot(percentiles, cdf_percentiles)
                    plt.title('CDF at each percentile') 

                    '''
                    We take a more granular CDF from percentile 0 to percentile 100
                    These are the data points that we will make the spline from
                    '''
                    granular_x_values = []
                    granular_cdf = []
                    for i in range(1001):
                        granular_x_values.append(digest.percentile(i/10))
                        granular_cdf.append(digest.cdf(digest.percentile(i/10)))
                    
                    '''
                    For this plot we are using a 3 x 3 plot with subplots
                    '''
                    plt.subplot(3, 3, 2)
                    plt.plot(granular_x_values, granular_cdf)
                    plt.title('CDF') 

                    # we will calculate the cdf using a distribution that is evenly selected from the even space not based on the percentile

                    uniform_x_values = np.linspace(digest.percentile(0), digest.percentile(100), 100)
                    uniform_cdf_values = []

                    for i in range(len(uniform_x_values)):
                        uniform_cdf_values.append(digest.cdf(uniform_x_values[i]))

                    '''
                    For this plot we are using a 3 x 3 plot with subplots
                    '''
                    plt.subplot(3, 3, 3)
                    plt.plot(uniform_x_values, uniform_cdf_values)
                    plt.title('Uniform CDF')

                    # Use cubic interpolation to plot the cdf
                    tck = interpolate.splrep(uniform_x_values, uniform_cdf_values)
                    cdf_spline = interpolate.splev(uniform_x_values, tck, der=0)

                    '''
                    For this plot we are using a 3 x 3 plot with subplots
                    '''
                    plt.subplot(3, 3, 4)
                    plt.plot(uniform_x_values, cdf_spline)
                    plt.title('CDF spline')

                    '''
                    Here we calculate the PDF using a simmilar strategy as above
                    Using cubic interpolation to plot the pdf
                    We can use the same splrep function as defined above just take the first derivative
                    '''

                    pdf_spline = interpolate.splev(uniform_x_values, tck, der=1)

                    '''
                    For this plot we are using a 3 x 3 plot with subplots
                    '''
                    plt.subplot(3, 3, 5)
                    plt.plot(uniform_x_values, pdf_spline)
                    plt.title('PDF spline')

                    '''
                    Here we do validaton of the model/profile of the data created by the Tdigests
                    This is accomplished by fitting a normal disribution to the model and seeing if the pramaters fit 
                    
                    The 'data' in this instance is : pdf_spline

                    In order to do this we are going to fit a fucntion to the CDF percentiles data and use that to fit to the normal distribution
                    '''

                    # for this 6th box, it will be the same as the 5th box however 

                    smooth_tck = interpolate.splrep(uniform_x_values, uniform_cdf_values, s=0.000075)
                    smooth_pdf = interpolate.splev(uniform_x_values, smooth_tck, der=1)

                    plt.subplot(3, 3, 6)
                    plt.plot(uniform_x_values, smooth_pdf)
                    plt.title("Smoothed pdf")

                    mu, std = norm.fit(pdf_spline)
                    plt.subplot(3, 3, 7)
                    plt.hist(pdf_spline, bins=100, density=True, alpha=0.6, color='g')

                    xmin = digest.percentile(0) 
                    xmax = digest.percentile(100)

                    x_vals = np.linspace(xmin, xmax, 100)
                    probability_function = norm.pdf(x_vals, mu, std)
                    plt.plot(x_vals, probability_function, 'k', linewidth=2)
                    title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
                    plt.title(title)

                    print("Finalize time: " + str(time.time() - start_time))
                    evaluated = True
                    if show: plt.show()

            # This is the final value of all the TDigests that are returned at the end
            print(digest_map)

            # print(book['Normal'].centroids_to_list())

if __name__ == '__main__':
    example_job()
