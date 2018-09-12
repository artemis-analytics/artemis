# Artemis

Artemis is a generic administrative data processing framework powered by Apache Arrow. 
The Artemis prototype presented in this document is a python data processing system 
with the following objectives: 

* Demonstrate the use of the Apache Arrow standard data model for tabular data.
* Demonstrate the ability to represent generic business processes in the form of directed 
acyclic graphs which can be executed in-memory to transform tabular data efficiently.
* Demonstrate the use of a histogram-based data validation and quality assurance framework.

The design decisions for Artemis must uphold data scientists core requirements for performing 
rigourous data analysis and the requirements defined by the Common Statistical Production Achitecture (CSPA). 
Furthermore, design choices align with the Apache Arrow objective to provide a development platform for data science systems
which decouples the vertical integration of data processing components:

* Job configuration and algorithm scheduling (Metadata management)
* I/O and deserialization (Data management)
* In-memory storage (Data format standardization)
* Computing engine (Code re-use)

## Introduction

The use of non-traditional administrative data for Official Statistics has a several notable features:

* Data is closest to the true nature of demographics.
* Preservation of the raw state of the data for analysts is required to extract the information.
* Data access patterns differ from traditional survey data.

Traditional sampling survey defines in advance the data which is collected,
thus facilitating the creation of a data model with a schema that can be enforced on write.

The challenge presented to Official Statistics agencies is how to ingest, store and process adminstrative while 
preserving the raw nature of the data for analysts. 
This challenge is significantly inhibited by a lack
of common data format with which to record the data and to analyze the data in-memory. 

* Variety of file formats are acquired with no common tool to read or convert data to a more suitable format in an organization.
* Enforcing schema on write can impede or prevent the ingestion of data.
* Traditional database modeling focuses on optimizing write operations for transactional, row oriented data.
* Conversion of data to a propriety organizational data format or model can inhibit collaboration.
* Ingestable data formats, such as csv, may be convenient but are not efficient on disk or in-memory.
* Data conversions result in loss of information, significant performance overhead, and sustain fractured data architectures systems.

Analytical workloads for administrative data sources which reside in a data store (filesystem, distributed datastore, object store, etc...) 
will follow a pattern of write once, read many times. Analysts will iterate many times on a master data set to produce subsets of data tailored to the analysis and
business needs. Analytical queries will have common data access patterns such as 

* reading subsets of columns for large number of rows at a time.
* accessing elements in adjacent columns in succession. 

These data access patterns can benefit from column oriented table structures over traditional row-oriented access patterns which are more commonly found
in traditional databases.

Common data format which defines data primitive types that occur in data science, social science and business data will ensure that the
raw state of the data can be preserved when consumed by organizations. 

### Data standardization

Open standards allow for systems to directly communicate with each other.
Direct communication using standard protocols and data formats 
* simplifies system architecture
* reduces ecosystem fragmentation
* improves interoperability across processes. 
* eliminates dependency on proprietary systems.
Most importantly, common data formats faciliate code reuse, sharing, effective collaboration 
and data exchange, resulting in algorithms and libraries which are supported by a large open community.  

Several examples of data standards in computing today:
* Human-readable semi-structured: XML, JSON
* Structure data query lanaguage: SQL has various flavors (MySQL, PostgreSQL, etc...)
* Binary data (with metadata), several from the scientific community
    * NetCDF
    * HDF5
    * Apache Parquet, ORC
    * PAW tuples and ROOT TTree 
* Binary blobs via RPC protocols
    * Apache Avro
    * Protocol buffers (Google)

The scientific community developed many common libraries in-use by data scientists
today in Fortran, such as linear algebra routines. The scientific programming ecosystem
in python effectively united around the ndarray, which is the NumPy multidimensional
fortran-compatible array which allows for re-use of linear algrabra routines, providing 
zero-overhead memory sharing to/from various libraries and processes.

The data science and social science community typically deal with tabular data which
manifests itself in various forms, most commonly refered to as DataFrames. The dataframe
concept and the semantics found in various systems are common to the various DataFrames. 
However, the underlying byte-level memory represention varies across systems. The difference in
the in-memory representation prevents sharing of algorithmic code across various systems and 
programming languages. Effectively, no standard exists for in-memory
tabular data. Common examples of tabular data in-use today by data and social scientists:

* Tabular data is commonly found in SQL.
* Big data community developed Spark and Hive.
* In-memory data frame in data science languages: 
python has pandas, R has data.table, and Julia has jil.table.

Tha Apache Arrow project solves the non-portable dataframe problem by
defining an open, cross-language development platform for in-memory data which specifies a 
standardized language-independent columnar memory format for flat and hierarchical data, 
organized for efficient analytic operations on modern hardware. Arrow provides computational 
libraries and zero-copy streaming messaging and interprocess communication. The key benefits of Arrow:

Fast – enables execution engines to take advantage of the latest SIMD (Single input multiple data) operations in modern processes, for native vectorized optimization of analytical data processing. Columnar layout is optimized for data locality for better performance. The Arrow format supports zero-copy reads for fast data access without serialization overhead.

Flexible – Arrow acts as a high performance interface between various systems and supports a wide variety of industry specific languages, including Python, C++ with Go in progress.

Standard – Apache Arrow is backed by key developers from major open-source projects.

Arrow defines language agnostic column-oriented data structures for array data which include:

* Fixed-length primitive types: numbers, booleans, date and times, fixed size binary, decimals, and other values that fit into a given number
* Variable-length primitive types: binary, string
* Nested types: list, struct, and union
* Dictionary type: An encoded categorical type

Arrow is an ideal in-memory transport layer for data being read or written to Apache Parquet files. Apache Parquet (Parquet, n.d.) project provides open-source columnar data storage format for use in data analysis systems. Parquet contains in-file metadata of column data types and names. Files can be organized as Parquet datasets which facilitate partitioned data. Data columns can be read or written as subsets, facilitating fast reading and skimming of datasets.
Apache Arrow and Parquet will facilitate fast data processing, efficient data storage, and improved governance on data access due to the in-file metadata. The key aspect for integration into a simulation framework and processing framework for non-traditional administrative data is the data validation to correctly infer and define column data types, and convert the raw data to the common format. Arrow supports Pandas which will likely accelerate the adoption of Python and Pandas as the de-factor processing tool.
Apache Arrow is a column-oriented in-memory format with direct persistency to various
column-oriented backend storage systems. The choice for column-oriented format is based
on the benefits achieved for performance reasons.
* Common access patterns that benefit from column-oriented data access
    * Access elements in adjacent columns in succession.
    * Efficient access to specific columns.
* Enables SIMD (Single instruction multiple data) based algorithms
* Vectorized algorithms
* Columnar compression.

The development goal of Apache Arrow is to deconstruct the typical data architecture
stack that is vertically integrated, providing public APIs for each component:

* IO/Deserialize
* In-memory storage
* Compute engine
* Front-end API

where the latter front-end API is really up to the users who are developing Arrow powered
data science systems.

**Apache Arrow and columnar data storage**

Persistent data and in-memory data models require well-defined transient/persistent separation to meet
requirements for in-memory data processing and on disk write and storage capabilities.
Apache Arrow provides in-memory analytic capability which efficiently utilizes container resources,
modern processor technologies and provides a flexible cross-platform in-memory data model.
Columnar, serializable persistent storage provides efficient I/O capability and low on-disk footprint due to compression capability.
Apache Parquet data format (originally developed for Hadoop) meets additional requirements for support of hierarchal data formats and in-file meta-data.



Columnar data storage
both in-memory and on-disk greatly enhances the performance of analytical workloads by organizing data for a given column contiguously. The advantages are:

* Reduced number of seeks for multi-row reads.
* Performant compression on columns due to single data types.

![Arrow Shared][img-arrow-shared]
![Arrow Copy][img-arrow-copy]

## Artemis Prototype

The Arrow portable runtime libraries makes Arrow front-end agnostic, allowing
for developers to create front-end APIs for data access suitable for our use-case. The
Artemis prototype is leveraging this capability to allow for the sharing of tabular data
across sequences of algorithms which describe various (sometimes disparate) business processes.
The prototype not only must demonstrate various capabilities and potential use of data standardization
but also test the validity of assumptions of processing adminstrative data files

* Data can be converted, stored and analyzed in a tabular data format.
* Data can be partitioned and processed independently, in parallel, 
facilitating both vertical and horizontal scaling 
(multi-processing (across cores) and distributed computing (across nodes).
* If the latter assumption fails, aggregated data stored in histograms can be used as input in 
subsequent processing to process the data in parallel. (E.g. imputation based on field mean).

Artemis is an application framework that defines a common set of base classes for user defined
algorithms and tools to interact with data. User-defined 
algorithms inherit methods which are invoked by the Artemis application. Aretmis manages the 
the data processing event loop, provides data to the algorithms, and handles data serialization
and job finalization.

Artemis Diagram

Input -> Data Handler -> Data Event Loop -> Pipelines and Outputs

### State Machine

State machines are common in event-driven data processing systems. Computer users are familiar 
with Graphical User Interfaces which respond to mouse clicks on the computer screen.
Certain clicks of a mouse cause the application to transition to a different
state (e.g., click open to Open a File). From physics, common of example of states and transitions
is water which states gas, liquid, solid. Water transitions from states due
to changes in temputure, volume and pressure, which we refer to as triggers.

Job control flow is implemented as a state machine to provide deterministic, fault tolerant
data processing. The Artemis state machine has several states which transition from one another via
trigger functions. The trigger functions must succesfully execute in order for the application
to proceed. The state machine implementation is currently facilitated by the python library transitions.
Explicit definition of states naturally factorize the application into distinct methods.
These methods may then invoke separate classes, modules, or even processes to handle certain
tasks.

Artemis currently defines the following states

**States** 

* Quiescent or Dormant -- Artemis object instantiation and initial state of the machine.
* Start - Job start
* Configuration - Configure all required services, e.g. connections, handles for data access, etc...
* Initialization - Define all job, algorithm and tool properties for executing and reproducing the job.
* Lock - Freeze the job configuration
* Meta - Persist the job configuration
* Book - Configure additional metadata information to be collected throughout the data processing for
data processing, process profiling and process metrics (input and output rates) 
    * histograms
    * timers
    * counters
* Run - Loop over data requests. Outer part of the Event Loop.
* Execution - Algorithm execution over the datums. Effectively is the inner part of an Event Loop.
* End
* Abort
* Error
* Finalize

**Transitions** 

### Data Handle and Access

I/O and data access are managed by a seperate class, or possibly a seperate process. 
In the case of a seperate process this may allow for asyncronous buffering of data 
from a filesystem, database or network node while processing
of the actual data continues in Artemis. Artemis sends a data request. The data handler
manages the request, fills a buffer and provides a generator which is consumed by Artemis.
The generator provides partitions of data in predetermined chunk sizes.

Assumption -- Each job receives a subset of a complete dataset. Each subset is partioned into
chunks up to a predetermined chunk size. 


### Algorithm scheduling via DAGs and topological sorting

Artemis design decouples the job configuration and the algorithm scheduling from the job
execution in order to completely define the job in metadata. This flexibility allows
for dynamic creation of algorithms to run on the same data with different configurations, 
allows for the job to be reproducible, and facilitates data validation and data process testing.



###

## Data Model and sharing


Additional References:

* [Ursa Labs](https://ursalabs.org/)
* http://wesmckinney.com/blog/announcing-ursalabs/
* https://mapr.com/blog/evolving-parquet-self-describing-data-format-new-paradigms-consumerization-hadoop-data/
* https://www.enigma.com/blog/moving-to-parquet-files-as-a-system-of-record
* https://www.kdnuggets.com/2017/02/apache-arrow-parquet-columnar-data.html
* https://www.slideshare.net/HadoopSummit/efficient-data-formats-for-analytics-with-parquet-and-arrow
* https://tech.blue-yonder.com/efficient-dataframe-storage-with-apache-parquet/
* https://www.mapd.com/blog/mapd-pandas-arrow/
* https://www.dremio.com/webinars/arrow-c
* [Projects Powered by Arrow](https://arrow.apache.org/powered_by/)

Recent talk by Wes McKinney at the SciPy 2018 Conference, Austin, TX

{{< youtube y7zGnKzaKIw >}}
