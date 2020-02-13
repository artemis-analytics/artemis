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

####################
Artemis Framework
####################

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

The Artemis primary objective is the production of datasets which utilize memory and cpu resources 
efficiently to accelerate analytical processing of data on a single-node, multi-core machine. 
The data processing can be replicated on independent parts of the dataset in parallel across 
multiple cores and / or across multiple nodes of a computing cluster in a batch-oriented fashion. 
The resulting dataset consists of a collection of output file(s), each file is organized as a 
collection of record batches (Arrow::RecordBatch). Each record batch consists of the same number of records 
in each batch with a fixed, equivalent schema. The resulting file can be considered as a table (Arrow::Table). 
(Note, Arrow Tables can support nested, hierarchal tables, Artemis focuses on demonstrating the use case for flat tables.)
The primary assumption for Artemis data production is that chunks of raw data can be read into 
Arrow buffers and subsequently perform computation on streams of record batches. 

The computations may perform many kinds of filter-projection-aggregate operations on very large dataset 
with only a small record batch in-memory. The processing of the record batches can be parallized across 
many cores or across a cluster of machines, resulting in both vertical and horizontal scaling of computing resources.
The raw dataset consists of one or more datums, such as files, database tables, or any data partition. 
In order to organize the data into collections of a fixed number of record batches to manage the data in-memory, 
each datum is separated into chunks of fixed size in bytes. Each chunk is converted from the raw input data to a record batch. 
The record batch can undergo any number of operations, e.g. parsing, conversion, cleaning, filtering, aggregation, 
and integration until the the entire transformation is applied to a record batch. The output record batch from a raw input 
chunk of a datum is written (serialized) to an output buffer (Arrow::BufferOutputStream). Raw input data chunks continue 
to stream into Artemis while the record batches continue to be serialized into the output stream. Once the buffer output stream consumes enough memory, 
the output buffer is "spilled" to disk. As more data continues to stream into the application, new buffers are created until all files from the raw dataset are stored in collections of Arrow record batches.

The output dataset does not assume to map directly back to the input dataset, as the data is reorganized into record batches to provide performant, random access data files. The ability to transform the data in-memory can result in one or more final output partitioning schemes which conform to the requirements of the downstream analysis. The columnar data structure is highly compressible and retains the schema as well as the entire payload in a given file. Arrow supports both streams and random access reads, as well as support for writing to variety of on-disk persistent storage formats, e.g. Parquet, ORC, and Feather. Artemis utilizes the ability to "spill" to disk in configurable file sizes into a native Arrow bytestream format.

* :ref:`io:Data Ingestion`
* :ref:`io:Data Preprocessing`
* :ref:`core:Data Processing`
* :ref:`io:Data Collection`
* :ref:`dq:Data Postprocessing`

