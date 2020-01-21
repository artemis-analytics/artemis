####################
Artemis Framework
####################
.. toctree::
   :maxdepth: 2

   core
   io
   meta
   algorithms
   tools
   generators
   externals

Overview
========
The Artemis primary objective is the production of datasets which utilize memory and cpu resources efficiently 
to accelerate analytical processing of data on a single-node, multi-core machine. The data processing can be 
replicated on independent parts of the dataset in parallel across multiple cores and / or across multiple nodes 
of a computing cluster in a batch-oriented fashion. The resulting dataset consists of a collection of output file(s), 
each file is organized as a collection of record batches. Each record batch consists of the same number of records 
in each batch with a fixed, equivalent schema. The collection of record batches in a file can be considered a table. 
The columnar data structure is highly compressible and retains the schema as well as the entire payload in a given file. 
Arrow supports both streams and random access reads of record batches, resulting in efficient and effective data management.

The control flow from the ingestion of raw data to the production of Arrow datasets proceeds as follows.  
The raw dataset consists of one or more datums, such as files, database tables, or any data partition. 
In order to organize the data into collections of a fixed number of record batches to manage the data in-memory, 
each datum is separated into chunks of fixed size in bytes. The datum is read into native Arrow buffers directly 
and all processing is performed on these buffers. The in-memory native Arrow buffers are collected and organized 
as collections of record batches, through data conversion algorithms, in order to build new on-disk datasets 
given the stream of record batches.

In order to support any arbitrary, user-defined data transformation, the Artemis framework defines a common set of 
base classes for user defined Chains, representing business processes, as an ordered collection of algorithms and 
tools which transform data. User-defined algorithms inherit methods which are invoked by the Artemis application, 
such that the Chains are managed by a Steering algorithm. Artemis manages the data processing Event Loop, 
provides data to the algorithms, and handles data serialization and job finalization. 

Data formats
============

* Apache Arrow tabular data format for the data, both in-memory and on-disk persistent representation.
* Google protocol buffer message layer for the metadata, describing all aspects to configure a job.

Components
==========

* Configuration - Artemis metadata model
    * Menu - directed graph descrbining the relationship between data, their dependencies, and the
      processes to be applied to the data.
* Application
    * Artemis - control flow algorithm managing the data input and outputs
    * Steering - execution engine for algorithms
        * Algorithms - the execution of a distinct business process.
        * Tools - specific action on a record batch. 
    * Services - data sinks for providing framework level access
        * Tool Store 
        * Histogram Store 
        * Job Properties - access to job configuration and to collect metadata during the processing. All
          histograms available in the store are persisted in the job properties object.

Inputs
======

* Protocol buffer message 

Outputs
=======

* Log file
* Protocol buffer message
* Arrow files

In order to run Artemis, a protocol buffer message must be defined and stored, conforming to the
artemis.proto metadata model, defined in `artemis/io/protobuf/artemis.proto`. 

Control Flow
============
Artemis framework defines a common set of base classes for user defined Chains, 
representing business processes, as an ordered collection of algorithms and tools which 
transform data. User-defined algorithms inherit methods which are invoked by the Artemis 
application, such that the Chains are managed by a Steering algorithm (which also inherits 
from a common base algorithm class). Artemis manages the data processing Event Loop, 
provides data to the algorithms, and handles data serialization and job finalization.

The Artemis primary objective is the production of datasets which utilize memory and cpu resources efficiently to accelerate analytical processing of data on a single-node, multi-core machine. The data processing can be replicated on independent parts of the dataset in parallel across multiple cores and / or across multiple nodes of a computing cluster in a batch-oriented fashion. The resulting dataset consists of a collection of output file(s), each file is organized as a collection of record batches (Arrow::RecordBatch). Each record batch consists of the same number of records in each batch with a fixed, equivalent schema. The resulting file can be considered as a table (Arrow::Table). Note, Arrow Tables can support nested, hierarchal tables. In the current prototype, Artemis focuses on demonstrating the use case for flat tables.
The primary assumption for Artemis data production is that chunks of raw data can be read into Arrow buffers and subsequently perform computation on streams of record batches. The computations may perform many kinds of filter-projection-aggregate operations on very large dataset with only a small record batch in-memory. The processing of the record batches can be parallized across many cores or across a cluster of machines, resulting in both vertical and horizontal scaling of computing resources.
The raw dataset consists of one or more datums, such as files, database tables, or any data partition. In order to organize the data into collections of a fixed number of record batches to manage the data in-memory, each datum is separated into chunks of fixed size in bytes. Each chunk is converted from the raw input data to a record batch. The record batch can undergo any number of operations, e.g. parsing, conversion, cleaning, filtering, aggregation, and integration until the the entire transformation is applied to a record batch. The output record batch from a raw input chunk of a datum is written (serialized) to an output buffer (Arrow::BufferOutputStream). Raw input data chunks continue to stream into Artemis while the record batches continue to be serialized into the output stream. Once the buffer output stream consumes enough memory, the output buffer is "spilled" to disk. As more data continues to stream into the application, new buffers are created until all files from the raw dataset are stored in collections of Arrow record batches.
The output dataset does not assume to map directly back to the input dataset, as the data is reorganized into record batches to provide performant, random access data files. The ability to transform the data in-memory can result in one or more final output partitioning schemes which conform to the requirements of the downstream analysis. The columnar data structure is highly compressible and retains the schema as well as the entire payload in a given file. Arrow supports both streams and random access reads, as well as support for writing to variety of on-disk persistent storage formats, e.g. Parquet, ORC, and Feather. Artemis utilizes the ability to "spill" to disk in configurable file sizes into a native Arrow bytestream format.

* Data Ingestion
* Data Preprocessing
* Data Processing
* Data Collection

Data ingestion
--------------

Data ingestion stage of Artemis comprises the opening of a buffer (a file path or byte buffer of in-memory raw data and the chunking of that buffer. 
Artemis parallelization operates at the datum level, further discussion will be covered in the Data Pipeline section. 


1)	Creation of an iterator of datums, referred to as the DataHandler. A datum is defined as a file path (on-disk storage location of raw data) or in-memory synthesized data (equivalent to raw data stored on disk).

a.	Base DataHandler class (GeneratorBase): https://gitlab.k8s.cloud.statcan.ca/stcdatascience/artemis/blob/master/artemis/generators/common.py#L163
b.	

2)	Loop over datums.
3)	Processing of a datum to return an iterator of record batches, referred to as a Reader. The creation of the Reader is managed by the FileHandler. A batch is defined as fixed size chunk of raw data that is managed in-memory one batch at a time. Artemis assumes that the raw data is structured tabular data, with a fixed column schema, organized as a records row wise. A batch must contain a complete record when split from the datum.
4)	FileHandler parses the file or buffer schema or header.
5)	FileHandler scans a batch of raw data to find a complete record to create a block. The block refers to the offset (starting byte of a record batch) and size (length in bytes to last byte in a record batch).
6)	Storing of file or buffer header, schema, and blocks in metadata. Registering and storing file or buffer metadata in the metastore.
7)	Loop over batches, until a datum has been entirely consumed. The Reader returns an Arrow buffer, wrapping the raw data for data processing. The processing of a record batch is described in Data processing.

Data Preprocessing
------------------

Data preprocessing prepares a raw record batch for further processing in the Arrow in-memory storage format.
1)	Preprocessing of a record batch is managed by a Reader. The Reader returns an Arrow buffer.  
2)	The Reader performs the opening and reading a block to return a raw record batch in various formats. 

a.	SAS reader returns a pandas data frame.
b.	Csv reader returns a block of csv date with a complete batch. This requires finding the record ending byte in the FileHandler.
c.	Legacy reader returns a fixed size block of ebcdic encoded data. This assumes all records have a fixed size.
d.	Arrow reader returns an Arrow record batch. This reader assumes the Arrow FileFormat is used.  

3)	Each raw record batch is passed to Steering for data processing.

Data Conversion
---------------
Data processing stage is the core functionality of Artemis. This stage allows for complex data dependencies to be managed through a single execution graph. Artemis allows for creation of user-defined algorithms and tools, however, this document cover standard algorithms for basic data processing. All data processing relies on the Arrow in-memory data format.
Data conversion 
For each data format, Artemis and Arrow aim to support some rudimentary features

* Convert from Arrow record batches to the target format, and back, with minimal loss.
* Automatic decompression of input files.
* Given a supplied schema, read a file in one chunk at time.
* Schema inference (schema on read)
* Conversion profiling for converted data, e.g. frequencies of errors in columns for converting to the desired data type

Artemis works with the schema-on-read paradigm, a general requirement for data science applications. Artemis manages the creation and reading of raw data chunks, in addition to fetching the schema (if available) from the input datum. (A predefined schema can also be provided as part of the initial Artemis job configuration.) Artemis passes the raw data chunk to the Arrow readers for column-wise type inference and data conversion to an Arrow record batch. Robust checking of the supplied and/or extracted schema against the inferred data types occurs on each chunk. Artemis also collects statistics on errors occured during processing, recording this information in frequency tables or histograms as part of the job summary metadata. The data error handling and statistics rely on information gathered from the Arrow readers.
Arrow provides funtionality to chunk data in their readers, returning an Arrow table of one or more record batches. However, delegating this funtionality to Artemis allows the framework to configure the size of the raw data to process in memory, apply all downstream processes to the data one chunk at a time a time, monitor the total Arrow memory consumption and handle flushing the Arrow memory and "spilling" processed data to disk.
The Artemis algorithms leverage the functionality of Arrow by directly interacting with Arrow record batches. Arrow parsers are implemented to support both single-threaded and multi-threaded reads, adapting the Artemis data chunker on the Arrow implementation could be a foreseen development. As well, Artemis is targeting the processing of legacy data (from mainframe, Cobol applications). Developing a native Arrow reader for EBCDIC data format in C++ could be considered a worthwhile contribution to the Arrow project.
Refer to the Appendix for additional details of the CSV reader implemented in Arrow.

1. Raw data is managed in the Arrow memory pool as a pyarrow buffer. Data that is not originally stored in the Arrow format, must be converted.
2. Record Batch conversion. Artemis provides support for converting data stored in flat-width text files with various encodings. 

a. Csv parser. This parses and converts csv data to an Arrow columnar record batch. This tool wraps the Arrow CsvReader.
b. Legacy parser. This parses and converts fixed-width ebcdic-encoded (other encodings supported) data. This tools wraps a tool based on the Arrow CsvReader. Similar functionality to the Arrow CsvReader is available.

3. Arrow in-memory processing. Once data is converted to the standard in-memory Arrow format, any arbitrary data transformation can be applied.

a. Filtering. Efficient column-level filtering can be applied to realize a new Arrow record batch with a subset of columns from a parent record batch.
b. Profiling. Numerical columns are profiled using the T-digest algorithm. TDigests are retained in the metadata, available for post processing in a data pipeline.

Data Processing
---------------

Business process model 
^^^^^^^^^^^^^^^^^^^^^^
Artemis design decouples the definition of the business process model, BPM, from the execution of those business processes on the data. Business process models are defined by the user and retained in the Artemis metadata. The flexibility of defining, retaining and storing the BPM in the metadata enables various configurations to be used on the same data, allows for the job to be reproducible, and facilitates data validation and code testing.
The BPM can be expressed as a directed graph, describing the relationship between data, their dependencies, and the processes to be applied to the data. The user defines the input(s), the output, the process to be applied to the input(s), and the algorithms which consistute a distinct business process. Each business process consumes data and produces new data, where the new data is the output of 
one or several algorithms which transform the data. Once the business processes are expressed in a series of algorithms, the processes must be transformed from a directed graph to a linear ordering of processes (where each process is a list of algorithms using a common input). The ordering of algorithms must ensure that the data dependencies are met before the execution of an algorithm.
TODO
Diagram Business Process Model
The ordering of the algorithmic execution is handled through a sorting algorithm. Users only need to ensure their pipeline defines the input(s), the sequence of algorithms to act on those inputs, and the output. The directed graph of data relationships, Tree, and the execution order is defined in the Artemis metadata.

Definitions:

**Sequence** - Tuple of algorithms (by name) which must be executed in the order defined.

**Node** - Name of an output business process. (COMMENT: We need to standardize/figure out our naming scheme. Node replaces Element in our menu definition. Also, if an Element is the output of a business process, we should state here what is the business process.)
**Chain** - Unordered list of input Elements (by name), a Sequence, and a single output Element.
**Menu** - Collection of user-defined Chains to be processed on a particular dataset.
**Tree** - Resulting directed graph data structure from the Menu

Artemis application requires the topological sorted list of output Nodes and Sequences. The topological sort of a directed graph is a linear ordering of the vertices such that for every directed edge uv from vertex u to vertex v, u comes before v in the ordering. Artemis uses the python library toposort to create the ordered list. (For more information, please refer to the Wikipedia description of topological sorting and the toposort algorithm available on BitBucket.) The resulting Tree and execution order is stored in metadata to be made available to Artemis.

Execution Engine and Inter-algorithm communication 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Artemis provides access to data from different sources. The data is read into native Arrow buffers directly and all processing is performed on these buffers. The in-memory native Arrow buffers are collected and organized as collections of record batches in order to build new on-disk datasets given the stream of record batches. Once raw data is materialized as Arrow record batches, Artemis needs to provide the correct data inputs to the algorithms so that the final record batches have the defined BPM applied to the data.
Artemis has a top-level algorithm, Steering, which serves as the execution engine for the user-defined algorithms. The Steering algorithm manages the data dependencies for the execution of the BPM by seeding the required data inputs to each Node in a Tree via an Element. The Tree data structure is the directed graph generated from the user-defined BPM Menu. Steering holds and manages the entire Tree data structure, consisting of the Elements of each Node, the relationships betweens Nodes, and all Arrow buffers attached to the Elements (a reference to the Arrow buffers). The Elements serve as a medium for inter-algorithm communication. The Arrow buffers (data tables) are attached to Elements and can be accessed by subsequent algorithms.
TODO
Diagram Data Access via Elements
An interesting comparison with the Gandiva contribution to Arrow from Dremio elucidates some parallels to Artemis. Gandiva is a C++ library for efficient evaluation of arbitrary SQL expressions on Arrow buffers using runtime code generation in LLVM. Gandiva is an indepdent kernel, so in principle could be incorporated into any analytics systems. The application submits an expression tree to the compiler, built in a language agnostic protobuf-based expression representation. Once python bindings are developed for Gandiva expressions, Artemis could embed the expressions directly in the algorithm configuration.
Planned developments in Arrow also extend to both algebra operators as well as an embeddable C++ execution engine. The embedded executor engine could be used in-process in many programming languages and therefore easily incorporated into Artemis. Arrow developers are taking inspiration from ideas presented in the Volcano engine and from the Ibis project.

Metadata model 
--------------
The Artemis metadata model has three primary components:
1. The defintion of the data processing job, i.e. all the required metadata to execute a business process model.

   a. Defintion of the data source or source(s)
   b. Definition of business process model for that particular data source(s)
   c. The configuration of algorithms, tools, and services required to execute the business process model

2. Job processing metadata, i.e. metadata required to support the execution of the BPM and to retain data provenance.
    
    a. The current state of the job
    b. Metadata related to the raw data source
    c. Metadata associating raw input data, intermediate data and output data (provenance)

3. Summary metadata
    
    a. Statistical information gathered during the processing of the data
    b. Cost information (timing disrtibutions) for processing stages, algorithm and tool execution

Detailed information on the model can be found in the Appendix.

I/O and Data Collection
-----------------------
Artemis must support for reading and writing common data formats as well as the legacy data formats (e.g. EBCDICs) which can be done efficiently with different file storage systems, including cloud and distributed HPC storage (e.g. GlusterFS).
Arrow is focused on interactions of 5 primary storage formats: CSV, JSON, Parquet, Avro, and ORC. A single file in any of these formats can define a single tabular dataset, or many files together forming a multi-file dataset. Arrow provides I/O capability for local disk, shared memory and in-memory storage. Plans for development for interfacing with cloud-based storage is anticipated. Some development for interacting the hdfs was provided by the Dask developers.
Artemis uses the Arrow native file type handles (e.g. NativeFile, Buffer, OSFile) for reading / writing to local disk for both the raw data as well the converted Arrow tabular data. Artemis also uses the Arrow BufferOutputStream for managing data for writing out random access files.
Artemis will be able to leverage planned developments for dataset abstraction, since datasets will already be organized in a logical set of files consisting of record batches. This will enable dataset functionality for
•	Streaming read (onme record at a time), either on columns or subsets of coumns
•	Streaming read with filtering (predicate filtering)
•	Constructing new on-disk datasets given a stream of record batches
Ideally, once the raw data is organized into Arrow tabular datasets, simple, high-level code for skimming data (selecting) columns and slimming data (predicate pushdown) can be easily implemented to run over datums in parallel (one-job per file). See the Appendix for more discussion.
Modularity of I/O can allow for a seperate process entirely to serve data to the application. Arrow developments for I/O tools, performant database connectors, and messaging are on the roadmap, and Artemis must be able to easily leverage these capabilities as they become available.

Data collection stage organizes collections of record batches into a file for on-disk storage. Record batches collected for a file must conform to a fixed schema, referred to as a partition. The record batches are collected from each leave node of the execution graph, therefore, Artemis supports multiple data streams in a single processing. 

1. Collector manages a dedicated Writer for each leave node. 
2. Collector is executed after Steering for each block.
3. Collector loops over the leave nodes and retrieves the Arrow record batch from each node.
4. Collector writes (serializes) each batch to the dedicated leave node Writer.
5. Collector checks the total size of the serialized data. If over the configured file size, the Writer closes the file and writes to disk. 
6. Collector monitors the total size of the Arrow memory pool. If the memory is over the configured size, Writers dump to disk.
7. Collector flushes all record batches from all nodes in the execution graph and arrow buffers.
8. Collector extracts metadata from the Writers, retaining data stream name, total number of records processed, total number of batches processed, and total number of files created.

Data quality 
------------
The data quality is an inherent part of data analysis, and therefore needs to be part of the core design of Artemis. The importance of data quality transcends domains of science, engineering, commerce, medicine, public health and policy. Traditionally, data quality can be addressed by controlling the measurement and data collection processes and through data ownershp. The increased use of administrative data sources poses a challenge on both ownership and controls of the data. Data quality tools, techniques and methodologies will need to evolve.
In the scientific domain, data quality is generally considered an implicit part of the role of the data user. Statistical infrastructure will need support the data users ability to measure data quality throughout the data lifecycle. Data-as-a-service oriented enterpise data solutions are not generally focused on ensuring the measurement of data quality. As well, the statistical tools required for data quality meaurements need to be developed to address data quality issues in administrative data. Artemis primary statistical analysis tool for data quality is the histogram.
Histograms are the bread-and-butter of statistical data analysis and data visualization, and a key tool for quality control. They serve as an ideal statistical tool for describing data, and can be used to summarize large datasets by retaining both the frequencies as well as the errors. The histogram is an accurate representation of a distribution of numerical data (or dictionary encoded categorical data), and it represents graphically the relationship between a probability density function f(x) and a set of n observations, x, x1, x2, ... xn.
Artemis retains distributions related to the processing and overall cost of the processing for centralized monitoring services:
•	Timing distributions for each processing stage
•	Timing distributions for each algorithm in the BPM
•	Payload sizes, including datums and chunks
•	Memory consumption
•	Total batches processed, number of records per batch
•	Statistics on processing errors
Separate histogram-based monitoring applications can be developed as a postprocessing stage of Artemis and will be able to quickly iterate over large datasets since the input data are Arrow record batches. Automated profiling of the distributions of the data is forseen as well for input to the postprocessing stage data quality algorithms. Further discussion on automated profiling is included in the appendix.

Services, algorithms and tools 
------------------------------

In-memory data store
^^^^^^^^^^^^^^^^^^^^
The data access of Arrow buffers is facilitated by an in-memory data store. The Elements available in the user-defined algorithms provide references to the actual Arrow buffers. The data buffers reside in a backing data store (or sink) that can be changed, under the 
hood, while the interaction of retrieving and attaching data to the Elements remains unchanged. The reference of the data that is retained in the Element is a unique key (UUID) that can be used to retrieve the data from any backing key-value store. The current implementation uses a python dictionary to manage the Arrow buffers. The Apache Ray project contributed the Plasma shared memory object store to the Arrow project. In the case of running Artemis on a multicore machine, multiple Artemis subprocesses could write to a single Plasma object store and faciliate asyncronous collections of Arrow record batches to write to disk. A shared object store may also faciliate shuffling of data across large data sets as well.
The advantage of the abstraction of data access via the dependecy Tree from the underlying data store simplifies data access for user-defined algorithms; allows for the framework to manage the memory; provides control for spilling data to disk when needed and flushing the memory; enables the use of shared memory data stores for leveraging a multicore environment; aleviating the need for users to deal with data management, serialization and persistency. In other words, any algorithm that works with Arrow data types can be easily incorporated into the Artemis framework with general ease.

Metastore
^^^^^^^^^
In order to ensure flexibility, reproducibility and to separate the definition of the BPM from the execution, Artemis requires a metadata model with a persistent representation. The persistent, serialized metadata must be flexible to work with, to store, and to use within Artemis and other applications. The Artemis metadata model must be able to support external application development and use within a scalable data production infrastructure.
Artemis metadata model is defined in a google protobuf messaging format. Protocol buffers are language-neutral, platform-neutral, extensible mechanism for serializing structured data – think XML, but smaller, faster, and simpler. Protocol buffers were developed by Google to support their infrastructure for service and application communication. Protobuf message format enables Artemis to define how to stucture metadata once, then special source code is generated to easily write and read the structured metadata to and from a variety of data streams and using a variety of languages. (Google developor pages). Protobuf messages can be read with reflection without a schema, making the format extremely flexible in terms of application development, use and persistency.
The serialized protobuf is a bytestring, in other words a BLOB (binary large object), which is a flexible, lightweight storage mechanism for metadata. The language-neutral message format implies that any application can be built to interact with the data, while the BLOB simplifies storage. The messages can be persisted simply as files on a local filesystem, or for improved metadata management the messages can be cataloged in a simple key-value store. Applications can persist and retrieve configurations using a key.
The idea of persisting the configuration as well as managing the state of Artemis derived from experimentation with the Pachyderm data science workflow tool and Kubernetes. Moreover, Arrow intends to develop secure, over the wire message transport layer in the Arrow Flight project using gRPC and protobuf. Artemis can leverage Arrow Flight along with gRPC to build scalable, secure, data processing architecture that can be flexible for cloud-native and HPC deployments.

Histogram and Timer Store
^^^^^^^^^^^^^^^^^^^^^^^^^^
Histograms and timers are centralled managed by the framework. The managed store allow Artemis to collect histograms and timing information into the metadata, serialize and persist this information at the job finalize stage. The stores support booking and filling histograms in any user-defined algorithms.
The histogram store also works as proxy to different histogram representations. Artemis currrently support two libraries histbook (from diana-hep) and physt (janpipek). Use in Artemis is primarily with physt since conversion to and from protobuf is supported. Once the physt histogram is converted to a protobuf message, the collection is added to the job metastore.
Algorithms and Tools
Similar to the idea of numpy user-defined functions, Artemis supports user-defined algorithms and tools. Any algorithm or tool which works with Arrow data structures can be easily incorporated into an Artemis BPM and executed on a dataset. Algorithms support user-defined properties in order to easily re-use algorithmic code to perform the same task with different configurations. Developers implement the base class methods, define any defined properties, and access the Arrow buffers through the Element. Steering manages algorithm instantiation, scheduling and execution. For the end-user the most important part of the code is defined in the execute method. Code organization and re-use can be improved by delegating common tasks which return a value to tools. The scheduling of the tools is managed directly in the algorithm, in other words, it is up to the user to apply the tools in the appropiate order.
Histogram and Timer Store
Histograms and timers are centralled managed by the framework. The managed store allow Artemis to collect histograms and timing information into the metadata, serialize and persist this information at the job finalize stage. The stores support booking and filling histograms in any user-defined algorithms.
The histogram store also works as proxy to different histogram representations. Artemis currrently support two libraries histbook (from diana-hep) and physt (janpipek). Use in Artemis is primarily with physt since conversion to and from protobuf is supported. Once the physt histogram is converted to a protobuf message, the collection is added to the job metastore.

Algorithms and Tools
^^^^^^^^^^^^^^^^^^^^
Similar to the idea of numpy user-defined functions, Artemis supports user-defined algorithms and tools. 
Any algorithm or tool which works with Arrow data structures can be easily incorporated into an 
Artemis BPM and executed on a dataset. Algorithms support user-defined properties in order to easily 
re-use algorithmic code to perform the same task with different configurations. 
Developers implement the base class methods, define any defined properties, and access the Arrow buffers 
through the Element. Steering manages algorithm instantiation, scheduling and execution. 
For the end-user the most important part of the code is defined in the execute method. 
Code organization and re-use can be improved by delegating common tasks which return a value to tools. 
The scheduling of the tools is managed directly in the algorithm, in other words, it is up to the user to apply 
the tools in the appropiate order.

An example user-defined algorithm

.. code-block:: python

    class MyAlgo(AlgoBase):
        def __init__(self, name, **kwargs):
            super().__init__(name, **kwargs)
            # kwargs are the user-defined properties
            # defined at configuration 
        def initialize(self):
            pass
        def book(self):
            # define histograms and timers
        def execute(self, element):
            # Algorithmic code
        def finalize(self):
            # gather any user-defined summary information

Logging and exception handling
------------------------------
Artemis uses standard python logging module as well as exception handling. The framework provides a logging functionality with a decorator. All algorithms can access a logger, using self.__logger.info("My message").
Exceptions must be raised in order to propagate upward. As long as all exceptions can be handled appropiately, the framework gracefully moves into an Abort state if the exception prevents further processing.

Data Pipeline
-------------
Data pipeline refers to the distinct stages when data is read, stored, and processed to achieve complete an entire data lifecycle. The Artemis lifecycle consists of:
1. Defining a dataset for processing.
2. Defining the processing strategy of a dataset, which can be serial or parallel.

    a. Managing a serial loop of datums (1 process for all datums)
    b. Managing a parallel loop of datums (e.g. 1 datum per subprocess)
    c. Managing a parallel loop of serial datums (Multiple datums split across subprocesses)

3. Ingestion of raw datums of a dataset.
    
   a.	The processing strategy needs to correctly configure the DataHandler.

4. Conversion and processing of datums.
5. Storage of raw data in a standardized data format.
6. Storage of metadata, including schema information, processing metrics, descriptive statistics.
7. Merging of standard data files (if necessary) to have consistent size files for further processing.
8. Merging of histogram and tdigest (descriptive statistics) metadata for complete dataset summary information. Extended to all metadata from each sub process.
    
    a. Collecting and merging all summary metadata
    b. Validating partition schemas. Are these consistent across all subprocesses.
    c. Checking all input data processed
    d. Checking for processing errors.

9. Visualization and statistical validation of aggregated metadata. 
10. Validation of data processing with reference metadata. 
    a. Release validation. Previous processing of data can be reprocessed with changes to Artemis software, with the same configuration, for validation. Comparison of metadata, checking total number of records and batches processed, comparison of statistical distributions, etc… 
    b. Simulation analysis. Processing of data provides a model (univariate, approximation using TDigest). The model is injected back into Artemis to simulate the original data. Similar to release validation, the processed simulated data produces a new profile of the data. Comparison of metadata, in particular, the statistical metadata.

R&D Task(s)
Pipeline system
Developing a parallelization strategy for managing a dataset.
Separation of user-defined menu, configuration and table generation code from pipeline code. Develop pipeline tools for managing the processing, merging, and postprocessing of metadata. Develop pipeline tools to manage the post-processing and validation of two datasets, reference dataset and reprocessed dataset. 
Dataset API
Arrow is developing a Dataset API for managing heterogenous datasets, with support for conversion into the Arrow in-memory format from common storage warehousing formats, e.g. Parquet, CSV, JSON, ORC, ROOT, etc… If the legacy data converter is donated back to Arrow, Artemis would be able to rely more heavily on Arrow’s IO libraries. R&D to use the Arrow Dataset API either in the data pipeline and/or in the DataHandler and FileHandler is anticipated. See the documentation from Arrow on the Dataset API:
https://docs.google.com/document/d/1bVhzifD38qDypnSjtf8exvpP3sSB5x_Kw9m-n66FB2c/edit?usp=sharing
https://docs.google.com/document/d/1QOuz_6rIUskM0Dcxk5NwP8KhKn_qK6o_rFV3fbHQ_AM/edit?usp=sharing


.. automodule:: artemis

.. automodule:: artemis.artemis
   :members:

