# Experiment Charter: Artemis Prototype Project
Version: Draft
 
# Executive Summary
* Experiment agreement regarding the investigation. This experiment investigates: 
    * The application of the Apache Arrow in-memory tabular data format for processing Administrative Data.
* The results of experiment will provide: 
    * Prototype data processing system powered by Apache Arrow.
    * Techniques to represent generic business processes as Directed Graphs 
which can be executed in-memory to transform tabular data efficiently.
    * Techniques for performing meta-data driven data processing.
    * Techniques for histogram-based data validation.
    * Preprocessing algorithm applied to selected Administrative Data sets 
for extracting and validating data holding record layout.
* Proof of feasibility may open up further experimentation to integrate with existing or 
experimental data processing systems. 

# Table of Contents

# 1.	Introduction
This document is an agreement between investigators and stakeholders on the timeline, scope and feasibility of the experiment. The agreement is sectioned as follows:
* Problem Background summarizes the problem and the context of this experiment. It further defines potential outcomes.  
* Investigators, Stakeholders and Partners states the parties involved in this experiment 
* Experiment Overview outlines the technology under consideration, specifies the feasibility criteria and the experiment outputs. 
The project is supported by the Administrative Data Division (ADD).

Any additional information on this document can be outlined here…
# 2.	Investigators, Stakeholders and Partners
## 2.1 Investigators 
| Role | Name | Group | Responsibility| 
| ---- | ---- | ----- | ------------- |
| Principal Investigator | Ryan White | Administrative Data Division | Lead in the design of the solution under test <br\> Lead in the design of the test criteria <br\> Lead the execution of experiment and sprints <br\> Coordinates investigation between key investigators <br\> Lead in documentation of experiment output |
| Investigator | Dominic Parent| Administrative Data Division | Aid in the design of test criteria <br\> Aid in the test of the solution <br\> Execution of the experiment <br\> Conduct investigation <br\> Document results |

## 2.2 Partners
| Role | Name | Group | Responsibility| 
| ---- | ---- | ----- | ------------- |
| External Partner | Apache Arrow Project Developers | Apache Arrow Project | Provides support to the experiment|
| Internal Partner | Jos&eacute;e Cellard | Administrative Data Division | Provides requirements on existing business processes, data flow, and data production requirements |


## 2.3 Stakeholders 
| Role | Name | Group | 
| ---- | ---- | ----- | 
| Executive Stakeholder, Sponsor | H&eacute;l&egrave;ne B&eacute;rard| Administrative Data Division|
| Other Stakeholder(s) | Crystal Sewards| Administrative Data Division |
| Stakeholder |	Marc Philippe St-Amour | Digital Innovation |

# 3. Problem Background

The use of administrative data for Official Statistics has a several notable features:

* Preservation of the raw state of the data is required to extract the relevant and accurate statistics.
* Data access patterns differ from traditional survey data.

The challenge facing NSAs is how to ingest, store and process adminstrative data while 
preserving the raw state of the data in a format that is both efficient from a data production 
perspective while also accesible for analysts. 
Analytical workloads for administrative data sources will follow a pattern of write once, read many times. 
Analysts will iterate many times on a master data set to produce subsets of data tailored to the analysis and
business needs. Analytical queries will have common data access patterns, for example: 
* reading subsets of columns for large number of rows at a time 
* accessing elements in adjacent columns in succession. 
The workload and access patterns can benefit from column oriented table structures over traditional 
row-oriented access patterns which are more commonly found in traditional databases.

Issues commonly found with adminstrative data sources: 

* Variety of file formats are acquired with no common tool to read or convert data to a more suitable format in an organization.
* Enforcing schema on write can impede or prevent the ingestion of data.
* Traditional database modeling focuses on optimizing write operations for transactional, row oriented data.
* Conversion of data to a proprietary organizational data format or model inhibits collaboration, results in data copying, and fragments data processing systems.
* Common data formats, such as CSV, are inefficient in terms of storage and processing. 
* Data conversions result in loss of information, significant performance overhead, and sustain fractured data architectures systems.

The development of data processing and analytical systems built on a common, open data format that support data primitive types occuring in data science, social science 
and business data will ensure that the
raw state of the data can be preserved when consumed by organizations. 
Tabular data organized in a column-oriented format is both
computational and I/O performant and simplifies production of derived data sets through simple, high-level filters.


## Administrative Data Common Workflow

The primary role of ADD is two-fold, data acquisition and data stewardship. Data acquisition refers to 
the contractual agreement with external organizations
to obtain datasets which are requested for use within the agency. Data stewardship refers to the 
management and access to those data holdings once received. 
As data stewards, ADD manages the data preparation of all adminstrative data.  
A brief overview of the general processing workflow is described below.

* Data files are parsed and converted to SAS files or are written to a database. 
* Validation of data assets according to an expected record layout.
* Validation of the expected number of records for a given data set.
* Profiling of the data sets and the production of frequency tables.
  
## Requirements for Adminstrative Data Processing

**Performance** The typical performance indicator is the turnaround time required to run
the entire processing chain when new requirements or data are introduced. 
Runtime is limited by transfer of data, loading of data between successive 
stages (or even succesive pipelines), and conversion between various formats to 
be used across fragmented systems. The software must minimize the number of read/write operations on data
and process as much in memory as possible. Distinct stages should be modular, such that changes
in stages can be rerun quickly.

**Maintainability** Modular software with a clear separation between algorithmic code 
and configuration facilitate introduction of new code which can be integrated without
affecting existing processes through configuration. Modular code also enforces 
code structure and encourages re-use. Common data format separates the I/O and deserialization
from the algorithmic code, and provide computing recipes and boiler-plate code structure 
to introduce new processes into the system.

**Reliability** The system is built on well-maintained, open-source libraries with design features
to easily introduce existing libraries for common analysis routines. 

**Flexibility** Re-use of common processes is faciliated through configurable algorithmic code. 
Use of a common in-memory data format simplify introducing new feautures, quantities and data structures
into the datasets.
 
# 4. Experiment Overview  

## 4.1 Technology Summary 

### Open data standards
Open standards allow for systems to directly communicate with each other.
Direct communication using standard protocols and data formats simplifies system architecture,
reduces ecosystem fragmentation, improves interoperability across processes, and eliminates dependency on proprietary systems.
Most importantly, common data formats facilitate code reuse, sharing, effective collaboration 
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
manifests itself in various forms, most commonly refered to as *DataFrames*. The *DataFrame*
concept and the semantics found in various systems are common to the various *DataFrames* implementations. 
However, the underlying byte-level memory representation  varies across systems. The difference in
the in-memory representation prevents sharing of algorithmic code across various systems and 
programming languages. No standard exists for in-memory
tabular data, however, tabular data is ubiquitous.  Tabular data is commonly found in SQL, 
the "Big Data" community developed Spark and Hive, and In-memory *DataFrames* are found across popular data science languages.
R, python and Julia all have a *DataFrame* in-memory tabular data which is commonly used by analysts.

### Apache Arrow

Tha Apache Arrow project solves the non-portable *DataFrame* problem by
providing a cross-language development platform for in-memory data which specifies a 
standardized language-independent columnar memory format for flat and hierarchical data, 
organized for efficient analytic operations on modern hardware. Arrow provides computational 
libraries and zero-copy streaming messaging and interprocess communication. The key benefits of Arrow:

**Fast** – enables execution engines to take advantage of the latest SIMD (Single input multiple data) operations in modern processes, 
for native vectorized optimization of analytical data processing. Columnar layout is optimized for data locality for better performance. 
The Arrow format supports zero-copy reads for fast data access without serialization overhead.

**Flexible** – Arrow acts as a high performance interface between various systems and supports a wide variety of industry specific languages, 
including Python, C++ with Go in progress.

**Standard** – Apache Arrow is backed by key developers from major open-source projects.

Arrow defines language agnostic column-oriented data structures for array data which include 
(see the Columnar Format 1.0 Milestone on Arrow Confluence https://cwiki.apache.org/confluence/display/ARROW/Columnar+Format+1.0+Milestone):

* Fixed-length primitive types: numbers, booleans, date and times, fixed size binary, decimals, and other values that fit into a given number
* Variable-length primitive types: binary, string
* Nested types: list, struct, and union
* Dictionary type: An encoded categorical type

The Arrow column-oriented in-memory format provides serialization/deserialization and supports persistency to various
column-oriented backend storage systems and formats. The choice for column-oriented format is based
on the benefits achieved for performance reasons.
* Common access patterns that benefit from column-oriented data access
    * Access elements in adjacent columns in succession.
    * Efficient access to specific columns.
* Enables SIMD (Single instruction multiple data) based algorithms
* Vectorized algorithms
* Columnar compression.

The development platform goal of Apache Arrow is to deconstruct the typical data architecture
stack that is vertically integrated, providing public APIs for each component:

* IO/Deserialize
* In-memory storage
* Compute engine
* Front-end API

where the latter front-end API is really up to the users who are developing Arrow powered
data science systems.

### Artemis Prototype

**Artemis** is a generic administrative data processing framework powered by Apache Arrow. 
The Artemis prototype is a python data processing system with the following development goals: 

* Demonstrate the use of the Apache Arrow standard data format for tabular data.
* Demonstrate the ability to represent generic business processes in the form of directed 
acyclic graphs which can be executed in-memory to transform tabular data efficiently.
* Demonstrate the ability to process data concurrently on partitions of a dataset.
* Demonstrate the use of a histogram-based data validation and quality assurance framework.


The Artemis design decisions presented here must uphold data scientists core requirements for performing 
rigourous data analysis and align with capabilities defined in the Common Statistical Production Achitecture (CSPA). 
Furthermore, design choices align with the Apache Arrow objective to provide a development platform for data science systems
which decouples the vertical integration of data processing components: configuration, I/O, in-memory storage, computational
engine, and front-end API.

The basic processing components of **Artemis**

* Data parsing and conversion 
    * Use existing I/O tools from Apache Arrow or directly from the Python library to manage
processing of data files in both a batch and stream processing way. The intent is
to organized datasets in an efficient, organized manner that improves downstream processing.
    * Use the Arrow type inference capabilities to determine data types in a dataset. 
    * Publish a dataset record layout or schema, retained seperately in a database.
    * Publish converted datasets in one (or more) persistent data formats. The data formats should support efficient access
to the data payload and schema information in a file. The data format should also support random access reads. Data will be
organized in the form of tabular data. The tabular will consists of batchs of records, where the number of records per batch
will be a configurable property. The ability to modify the number of records per batch will facilitate understanding memory 
managment, memory allocation, and I/O performance.
* Data profiling
    * Develop a histogram-based concurrent processing workflow.
    * Frequency tables will be produced in the form of histograms. Histograms may be published in sub-datasets if the job
is distributed. The histograms will go through a merging step to aggregate across all the sub jobs.
* Data validation
    * Algorithmic code to manage the data validation according to a record layout and / or additional meta data information.
    * Validate the number of records using several methods, such as a processing counter running in the application and seperately 
with metadata that is retained in the file footer.
    * Postprocessing validation algorithms given the data profiling output (histograms).

## 4.2 Duration 
The duration of the experiment is 3 months. After 3 months, testing of the prototype is envisioned on real production data
if the assumptions established above are verified.
Touch points will review ongoing test results. The details of the touch points of the experiment:

* **Artemis** application conceptual design phase.
* Evaluation of dependency requirements.
* Application development phase.
* Algorithmic development phase.
* Conceptual design report.


## 4.3 Feasibility Criteria
The feasibility criteria specifies the areas of investigation which the experiment will demonstrate.  

| Capability | Measure |
| ---------- | ------- | 
| Modular data processing system  | Meta-data driven processing <br\> Input / Output and serialization <br\> Algorithmic development with common data interface <br\> Ability to support various algorithms |
| Open Data format standardization | Data converted and validated in a standard tabular data format <br\> Algorithms to validate the original data and converted data | 
| Distributed processing | Data can be processed independently on partitions (datums) of files |
| Result Retrieval | Converted, tabular, serialized data <br\> Dataset schema (meta-data) <br\> Log files <br\> Job meta-data <br\> Histogram dataset | 



## 4.4 Experiment Output 
1.	Results for each iteration from investigation 
    * Design documents 
    * Test criteria 
    * Evaluation notes 
2.	Access to the prototype solution for demonstration purposes 

## 4.5	Potential Outcomes  
* Proof-of-concept for adoption of an open source, 
cross-language, tabular data format for persistence, access, production and in-memory analysis of administrative data.
* Experimental data processing framework that serves a conceptual design for building modular, 
cross-language, interoperable data processing and analysis systems.
* Data validation and schema management techiques for adminstrative data.
* Histogram-based data validation system.
* **Artemis** version 0.1 application release.

## Works Cited
Apache Arrow, https://apache.arrow.org

## Appendix A - Experiment Charter Guidelines

The following guide helps with understanding the Experiment Charter

The Spirit of Experimentation 
* Experimentation is a de-risk mechanism to explore new techniques and moonshot ideas without impact to the operation of the business. 
* Goal of experimentation is to fail early (e.g. tackle the toughest feasibility issues first)
Investigators, Stakeholders and Partners
* The principal investigator is responsible for the results of this experiment. The other named investigators will aid the principal investigator and perform evaluations of the solution. Roles for each investigator can be further defined if needed. 
* Stakeholders are persons that have an interest in the results in the experiment (e.g. business, technical).
* Partners may be internal or external members that aid in the implementation of the experiment 
* Experiments can have a different investigators, stakeholders and partners

Duration and Experiment Process 
Each iteration (or sprint) should be structured as follows:
1. Design and Create
    * Record the design and procedure to set up
    * Record the identified test cases, the defined metric (feasibility criteria test case), and the means to measure
2. Test 
    * Record the results (measured values), the input, and the output for each test 
3. Evaluate
    * Document other opportunities for experimentation. This can be for the next iteration.

