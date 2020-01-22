###############
Getting Started
###############

Artemis project relies on conda as an environment manager and build tool. The project has one
external dependency, the Fixed-width file reader (stcdatascience/fwfr.git) that needs to be built.


Development environment
=======================

.. code:: bash

  mkdir <workspace>
  cd <workspace>
  git clone https://github.com/ryanmwhitephd/artemis.git
  git clone https://github.com/ke-noel/fwfr.git
  conda env create -f artemis/environment.yaml
  conda activate artemis-dev
  cd fwfr
  ./install.sh --source
  cd ../artemis
  python setup.py build_ext --inplace install
  python -m unittest



Artemis Example Job
======================

An example job is available in examples/distributed_example_2.py. The example job does the following:

* Extracts a *table schema* from Excel.
* Generates synthetic data from the *table schema*. 
* Ingests and converts the synthetic data to a standard in-memory format.
* Performs data analytics with *algorithms* and analytical *tools*.
* Profiles columns of data from a tabular data structure.
* Writes the converted data to disk, metadata, profiling and summary statistical information.
* Retains links to the data and metadata in a data repository.

Ensure that Artemis is built, then, run the following command.

.. code:: bash

  python examples/distribucted_example_2.py --location ./examples/data/example_product.xlsx


The example schema is located in examples/data/example_product.xlsx. To create new dataset schemas
please see instructions in artemis/tools/Excel_template/README.md

Let's go through the various steps required to process data with Artemis. Remember Artemis is guided by FAIR principles,
so the products of Artemis must be Findable, Accessible, Interoperable and Reproducible. To this end, there is a lot
of effort to define everything that will happen to the data and persist this information. 

* Define the Business Process(es).
* Define how the data will be ingested, modeled, processed and stored.
* Track all data objects in a common metadata repository.
* Use available computing resources efficiently in the most flexible manner possible.

Create a Business Process Model
-------------------------------
Artemis relies on modeling business processes and retaining this information in metadata. Artemis refers to a business
process model as a *menu*. A *menu* describes all the transformations to be applied to the data, in which order, and the expected outputs.
A helper class is provided to aid in the construction of a *menu*.

.. code:: python
    
    class ExampleMenu(MenuBuilder):
        def __init__(self, name='test'):
            super().__init__(name)

        def _algo_builder(self):
            # Define the algorithms to be used
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
            # One or more chains results in a complete menu
            self._chains['csvchain'] = Directed_Graph("csvchain")
            self._chains['csvchain'].add(self._seqs['seqX'])
            self._chains['csvchain'].add(self._seqs['seqY'])
            self._chains['csvchain'].add(self._seqs['seqA'])
            self._chains['csvchain'].add(self._seqs['seqB'])

Next, the *menu* is built and later will be registered in the object store.

.. code:: python
    
    menu_builder = ExampleMenu()
    mymenu = menu_builder.build()

Create a configuration for data processing
------------------------------------------
Artemis relies on defining the configuration of all tools, algorithms, and properties for managing and processing data and 
storing. This information is retained as metadata and persisted.

.. code:: python
    
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

    myconfig = Configuration()  # Cronus Configuration message
    myconfig.uuid = str(uuid.uuid4())
    myconfig.name = f"{config.uuid}.config.pb"
    myconfig.max_malloc_size_bytes = max_malloc

    generator = SimuTableGen('generator',
                             nbatches=gen_nbatches,
                             num_rows=gen_nrows,
                             file_type=1,  # Output type cronus.proto filetype
                             table_id=table_id,
                             seed=seed)

    # Set the generator configuration
    myconfig.input.generator.config.CopyFrom(generator.to_msg())

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
    myconfig.tools[filehandler.name].CopyFrom(filehandler.to_msg())

    csvtool = CsvTool('csvtool', block_size=(2 * blocksize))
    myconfig.tools[csvtool.name].CopyFrom(csvtool.to_msg())

    filtercoltool = FilterColTool('filtercoltool',
                                  columns=['record-id', 'SIN', 'DOB'])
    myconfig.tools[filtercoltool.name].CopyFrom(filtercoltool.to_msg())

    writer = BufferOutputWriter('bufferwriter',
                                BUFFER_MAX_SIZE=max_buffer_size,
                                write_csv=write_csv)
    myconfig.tools[writer.name].CopyFrom(writer.to_msg())

    tdigesttool = TDigestTool('tdigesttool')
    myconfig.tools[tdigesttool.name].CopyFrom(tdigesttool.to_msg())

    sampler = myconfig.sampler
    sampler.ndatums = sample_ndatums
    sampler.nchunks = sample_nchunks

Create an object store
----------------------
All persisted data objects have a persistent unique identifier that identifies them in a storage system. The storage system is 
a metadata service and a physical storage location.

.. code:: python

    with tempfile.TemporaryDirectory as dirpath:
        store = BaseObjectStore(dirpath, 'artemis')

This will generate a new object store in `dirpath` with a PID. The object store properties are:

* `store_name`
* `store_uuid`
* `store_info`
* `store_aux`

To retrieve the object store at a later time requires both the name and the PID `store = BaseObjectStore(dirpath, 'artemis', store_uuid)`.

Next, let's register a new *dataset* that we will create. The new dataset, in this case, will be synthetic data.

.. code:: python
    
    my_dataset = store.register_dataset()
    store.new_partition(my_dataset.uuid, 'generator')
    job_id = store.new_job(mydataset.uuid)

A *dataset* in Artemis contains one or more *partitions* of data objects. A *partition* is defined a collection of *datums*, e.g. data objects, with a fixed, consistent *table* *schema*. A *job* in Artemis provides lineage to the data produced. One or more *jobs* can be associated
to a given *dataset*. 

In this example, we synthesis data so a *table schema* must be defined and registered in the object store. 
Let's use a predefined *table schema*,

.. code:: python
    
    xlstool = XlsTool('xlstool', location=location)
    ds_schema = xlstool.execute(location)
    # Example job only have one table
    table = ds_schema.tables[0]

Register the metadata
---------------------
At this point, we have defined all aspects of the data, what we will do to the data, where to store the data, and what are 
the expected inputs and outputs. This information must be made avaiable in the object store for use in Artemis.


All data objects, whether *datums* or *metadata* have a contextual metadata, referred to as a *metaobject*, 
which links to the data object in store. The contextual *metaobject* holds similar properties to the store, e.g. name, uuid, info, ...


and register the *table schema* in the object store. This is now available to be consumed by Artemis for synthesizing data.

.. code:: python
    
    menuinfo = MenuObjectInfo()
    menuinfo.created.GetCurrentTime()
    
    # Algorithms need to added from the menu to the configuration
    for key in menu_builder._algos:
        msg = myconfig.algos.add()
        msg.CopyFrom(menu_builder._algos[key].to_msg())

    configinfo = ConfigObjectInfo()
    configinfo.created.GetCurrentTime()

    menu_uuid = store.register_content(mymenu, menuinfo).uuid
    config_uuid = store.register_content(myconfig, configinfo).uuid
    
    tableinfo = TableObjectInfo()
    table_id = store.register_content(table,
                                      tableinfo,
                                      dataset_id=mydataset.uuid,
                                      job_id=job_id,
                                      partition_key='generator').uuid
    # Make sure to persist all this information
    store.save_store()

The *configuration* and *menu* provide all the required metadata to be able to process the data and reproduce that process.
Define the output dataset with the PIDs of the metadata.

.. code:: python
    
    # Register an output dataset
    dataset = store.register_dataset(menu_id=menu_uuid,
                                     config_id=config_uuid)
    
Process the data
----------------
In this example, Artemis processes jobs in parallel, generating and consuming data in each parallel process.

Create a separate job to parallelize the generation and processing of data.

.. code:: python 
    
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

Next, pass the job function to dask to manage the scheduling.

.. code:: python
    
    results = dask.compute(*ds_results, scheduler='single-threaded')
 
Build the final dataset from the output metadata from each job. This will combine all the data into a single *dataset*.

.. code:: python
    
    store.new_partition(dataset.uuid, 'seqA')
    store.new_partition(dataset.uuid, 'seqB')
    store.save_store()
    for buf in results:
        ds = DatasetObjectInfo()
        ds.ParseFromString(buf)
        store.update_dataset(dataset.uuid, buf)

    store.save_store()

Postprocessing
--------------
The final part of the production process is evaluating the outputs and deteriming data quality. 

* Did all the jobs complete without error?
* Is the final dataset complete?
* What are the summary statistics and characterstics of the data?
* How did the job perform, are certain algorithms to slow?
* How much data was processed?
* What does the data look like?

Artemis provides tools to evaluate the quality of the data.

Merge the metadata.
Visualize the metadata.
Process the output data.

