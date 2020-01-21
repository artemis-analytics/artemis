############
Introduction
############

**Artemis** -- Administrative data science framework powered by Apache Arrow™.

Statistical organizations are shifting to an administrative data first approach for 
producing official statistics. The production of high-quality, fit-for-use administrative data 
must preserve the raw state of the data throughout the data life cycle (ingestion, integration, management, processing and analysis). Data formats and production frameworks must support changes to analytical workloads which have different data access patterns than traditional survey data, efficient iteration on the data at any stage in the data cycle, and statistical tools for continuous data quality and fit-for-use assessment. The framework design must have at the core a well-defined data format that can accelerate analytical processing of the data on modern computing architecture.
At the core of any data enterprise is a consistent, well-defined data model. The data model will underpin the long-term data management strategy and provide the requirements for computing, storage, and data analysis. The choice of the data model must reflect the analysis patterns of the end-user, and support the analytical workloads for data production.
The Artemis data processing framework demonstrates the use of the Apache Arrow in-memory columnar data format and modern computing techniques for data ingestion, management and analysis of very large datasets. The framework supports analytical workloads for administrative data sources that follow a pattern of write once, read many times, and analytical queries with common data access patterns based on a tabular data structure.
The Artemis prototype leverages Apache Arrow from the start, working with Arrow buffers to produce high-quality datasets. Artemis accomplishes this by providing a framework to execute operations on Arrow tables (an execution engine) through user-defined algorithms and tools. Artemis core functionality is configurable control flow for producing datasets consisting of one or more Arrow Tables. The language-neutral Arrow data format allows Artemis to pass data to/from other processes or libraries, in-memory, with zero-copy and no serialization overhead.

Objectives 
==========
1. Production of logical datasets of a single consistent data format that enable efficient interactions with very large datasets in a single-node, multicore environment.
2. Support analysis on streams of record batches that do not neccesarily reside entirely in-memory.
3. Execution of complex business processes on record batches to transform data.
4. Incorporate data quality and fitness-for-use tools as part of the data production process.
5. Provide an experimental learning environment for the development of data science systems with Apache Arrow.

Analysis Requirements 
=====================
In general, analyst require only a few data functions: discover, access, process, analyze, and persist. 
More comprehensively, the general requirements for data processing and analysis consist of:

* Comprehensive, robust support for reading and writing a variety of common storage formats in cloud, on-premise, local or distributed data warehousing (CSV, JSON, Parquet, legacy data).
* Ability to perform schema-on-read either through supplied schema or through type-inference and produce a well-defined schema for the dataset.
* Ability to extract meta-data and catalogue data sets in a language and application agnostic manner.
* Support for a write-once, read-many times iterative analysis with minimal data conversions, IO and serialization overhead.
* Effective and efficient data management.
* Efficient filtering of data, e.g. selection of columns from a master dataset.
* Perform analytical operations, e.g. projections, filters, aggregations, and joins.
* Collection of dataset statistics, e.g. marginal distributions, mean, minimum, maximum.

Production System Requirements
==============================
The design of data production frameworks, which supports the end-user analyst needs, 
must focus on four key features:

1. Performance – The typical performance indicator is the turnaround time required to run the entire processing chain when new requirements or data are introduced. Runtime is limited by transfer of data, loading of data between successive stages (or even succesive pipelines), and conversion between various formats to be used across fragmented systems. The software must minimize the number of read/write operations on data and process as much in memory as possible. Distinct stages should be modular, such that changes in stages can be rerun quickly.
2. Maintainability – Modular software with a clear separation between algorithmic code and configuration facilitate introduction of new code which can be integrated without affecting existing processes through configuration. Modular code also enforces code structure and encourages re-use. Common data format separates the I/O and deserialization from the algorithmic code, and provide computing recipes and boiler-plate code structure to introduce new processes into the system.
3. Reliability – The system is built on well-maintained, open-source libraries with design features to easily introduce existing libraries for common analysis routines.
4. Flexibility – Re-use of common processes is faciliated through configurable algorithmic code. Use of a common in-memory data format simplify introducing new features, quantities and data structures into the datasets.

Platform Components
===================
The Artemis prototype framework leverages the Apache Arrow development platform capability, and focuses on data processing and analysis in a collaborative and reproducible manner. The front-end agnostic Arrow API allows us to define a data model to manage the sharing of tabular data across sequences of algorithms, which describe various (sometimes disparate) business processes in a single, in-memory, data processing job. The algorithms describe various business processes for the same dataset, and the algorithms can be re-used for different datasets with common pre-processing and processing requirements.
Assumptions set forth for Artemis are derived from event-based data processing frameworks from high-energy physics. Therefore, many design choices have been adopted from large-scale data processing systems used in the HEP community, which until recently, have been able to scale to the processing Petabytes of data per year.

Artemis framework design features

* Metadata management - seperation of algorithmic code and configuration.
* Performance - seperation of I/O from data processing managed at the framework level to minimize read/write.
* Reliability - State machine for job control flow and global steering of data pipeline algorithms.
* Reproduciblity - in-memory provenance of data transformations.
* Flexibility - modular code design to faciliate code re-use, simplify testing and development.
* Automation - automatic collection of processing metrics.
* Configuration - user-defined histograms and data tables.


