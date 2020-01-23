.. Copyright © Her Majesty the Queen in Right of Canada, as represented
.. by the Minister of Statistics Canada, 2019.
..
.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

########################
IO and Memory management
########################

Artemis must support for reading and writing common data formats as well as the legacy data formats (e.g. EBCDICs) 
which can be done efficiently with different file storage systems, including cloud and distributed HPC storage (e.g. GlusterFS).
Arrow is focused on interactions of 5 primary storage formats: CSV, JSON, Parquet, Avro, and ORC. 
A single file in any of these formats can define a single tabular dataset, or many files together forming a multi-file dataset. 
Arrow provides I/O capability for local disk, shared memory and in-memory storage. 
Plans for development for interfacing with cloud-based storage is anticipated. 
Some development for interacting the hdfs was provided by the Dask developers.
Artemis uses the Arrow native file type handles (e.g. NativeFile, Buffer, OSFile) for reading / writing to 
local disk for both the raw data as well the converted Arrow tabular data. 
Artemis also uses the Arrow BufferOutputStream for managing data for writing out random access files.
Artemis will be able to leverage planned developments for dataset abstraction, since datasets will already 
be organized in a logical set of files consisting of record batches. This will enable dataset functionality for

* Streaming read (onme record at a time), either on columns or subsets of coumns
* Streaming read with filtering (predicate filtering)
* Constructing new on-disk datasets given a stream of record batches

Ideally, once the raw data is organized into Arrow tabular datasets, simple, high-level code for skimming data (selecting) columns and slimming data (predicate pushdown) can be easily implemented to run over datums in parallel (one-job per file). See the Appendix for more discussion.
Modularity of I/O can allow for a seperate process entirely to serve data to the application. 
Arrow developments for I/O tools, performant database connectors, and messaging are on the roadmap, 
and Artemis must be able to easily leverage these capabilities as they become available.


In-memory data store
====================
The data access of Arrow buffers is facilitated by an in-memory data store. The Elements available in the user-defined algorithms provide references to the actual Arrow buffers. The data buffers reside in a backing data store (or sink) that can be changed, under the 
hood, while the interaction of retrieving and attaching data to the Elements remains unchanged. The reference of the data that is retained in the Element is a unique key (UUID) that can be used to retrieve the data from any backing key-value store. The current implementation uses a python dictionary to manage the Arrow buffers. The Apache Ray project contributed the Plasma shared memory object store to the Arrow project. In the case of running Artemis on a multicore machine, multiple Artemis subprocesses could write to a single Plasma object store and faciliate asyncronous collections of Arrow record batches to write to disk. A shared object store may also faciliate shuffling of data across large data sets as well.
The advantage of the abstraction of data access via the dependecy Tree from the underlying data store simplifies data access for user-defined algorithms; allows for the framework to manage the memory; provides control for spilling data to disk when needed and flushing the memory; enables the use of shared memory data stores for leveraging a multicore environment; aleviating the need for users to deal with data management, serialization and persistency. In other words, any algorithm that works with Arrow data types can be easily incorporated into the Artemis framework with general ease.

.. automodule:: artemis.io.collector
   :members:

Data Ingestion
--------------

Data ingestion stage of Artemis comprises the opening of a buffer (a file path or byte buffer of in-memory raw data and the chunking of that buffer. 
Artemis parallelization operates at the datum level, further discussion will be covered in the Data Pipeline section. 


1. Creation of an iterator of datums, referred to as the DataHandler. A datum is defined as a file path (on-disk storage location of raw data) or in-memory synthesized data (equivalent to raw data stored on disk).

    a. Base DataHandler class (GeneratorBase): https://gitlab.k8s.cloud.statcan.ca/stcdatascience/artemis/blob/master/artemis/generators/common.py#L163

2. Loop over datums.
3. Processing of a datum to return an iterator of record batches, referred to as a Reader. The creation of the Reader is managed by the FileHandler. A batch is defined as fixed size chunk of raw data that is managed in-memory one batch at a time. Artemis assumes that the raw data is structured tabular data, with a fixed column schema, organized as a records row wise. A batch must contain a complete record when split from the datum.
4. FileHandler parses the file or buffer schema or header.
5. FileHandler scans a batch of raw data to find a complete record to create a block. The block refers to the offset (starting byte of a record batch) and size (length in bytes to last byte in a record batch).
6. Storing of file or buffer header, schema, and blocks in metadata. Registering and storing file or buffer metadata in the metastore.
7. Loop over batches, until a datum has been entirely consumed. The Reader returns an Arrow buffer, wrapping the raw data for data processing. The processing of a record batch is described in Data processing.

Data Preprocessing
------------------

Data preprocessing prepares a raw record batch for further processing in the Arrow in-memory storage format.

1. Preprocessing of a record batch is managed by a Reader. The Reader returns an Arrow buffer.  
2. The Reader performs the opening and reading a block to return a raw record batch in various formats. 
    
    a. SAS reader returns a pandas data frame.
    b. Csv reader returns a block of csv date with a complete batch. This requires finding the record ending byte in the FileHandler.
    c. Legacy reader returns a fixed size block of ebcdic encoded data. This assumes all records have a fixed size.
    d. Arrow reader returns an Arrow record batch. This reader assumes the Arrow FileFormat is used.  

3. Each raw record batch is passed to Steering for data processing.

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

Data Collection
-----------------------

Data collection stage organizes collections of record batches into a file for on-disk storage. 
Record batches collected for a file must conform to a fixed schema, referred to as a *partition*. The record batches are collected from each leave node of the execution graph, therefore, Artemis supports multiple data streams in a single processing. 

1. Collector manages a dedicated Writer for each leave node. 
2. Collector is executed after Steering for each block.
3. Collector loops over the leave nodes and retrieves the Arrow record batch from each node.
4. Collector writes (serializes) each batch to the dedicated leave node Writer.
5. Collector checks the total size of the serialized data. If over the configured file size, the Writer closes the file and writes to disk. 
6. Collector monitors the total size of the Arrow memory pool. If the memory is over the configured size, Writers dump to disk.
7. Collector flushes all record batches from all nodes in the execution graph and arrow buffers.
8. Collector extracts metadata from the Writers, retaining data stream name, total number of records processed, total number of batches processed, and total number of files created.
