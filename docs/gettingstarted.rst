===============
Getting Started
===============

#######
Artemis
#######

**************************************************************************
Artemis -- Administrative data science framework powered by Apache Arrow™.
**************************************************************************

The Artemis data science frameowrk is a record batch based data processing framework, powered 
by Apache Arrow open source data format standard, for the production of high-quality
adminstrative data for analytical purposes. Statistical organizations are shifting to
an adminstrative data first approach for producing official statistics. The production
of high-quality, fit-for-use administrative data must preserve the raw state of the data 
throughout the data life cycle (ingestion, integration, management, processing, and analysis).
Data formats and production frameworks must support changes to analytical workloads
which have different data access patterns than traditional survey data, efficient
iteration on the data at any stage in the data life cycle, and statistical tools
for continous data quality and fit-for-use assessement. The Artemis data science
framework at the core relies on the well-defined, cross-lanaguage, Apache Arrow
data format that accelerates analytical processing of the data on modern computing
architecture.

Artemis framework primary objectives:

* Production of logical datasets of a single consistent data format that enable
  efficient interactions with very large datasets in a single-node, multicore environment.
* Support analysis on streams of record batches that do not neccesarily reside entirely 
  in memory
* Execution of complex business processes on record batches to transform data.
* Incorporate data quality and fitness-for-use tools as part of the data production process.
* Provide an experimental learning environment for the development of data science systems 
  with Apache Arrow.

***************
Getting Started
***************

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

Framework components
====================

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

**************
Build & Deploy
**************

To build Artemis, cd to the root of the artemis repository. Follow the instructions below.

.. code:: bash

  conda env create -f environment.yml
  conda activate artemis-dev
  git clone "FWFR GIT REPO"
  conda install conda-build
  conda build conda-recipes
  mv "PATH TO CONDA"/envs/artemis-dev/conda-bld/broken/artemis-"VERSION".tar.bz2 ./
  conda deactivate
  bash release/package.sh -e artemis-dev -n artemis-pack -p artemis-"VERSION" -r "PATH TO ARTEMIS REPO"

This will result in a package called "artemis-pack.tar.gz". You can move this to anywhere you wish to 
deploy.

You can install the created package file with the "deploy.sh" script. 

.. code:: bash

  bash deploy.sh -e "NAME OF CONDA ENV TO CREATE" -n "NAME OF PACKAGE FILE" -p "NAME OF PACKAGE"

**********************************
Artemis Release and Tag Management
**********************************

During a new Artemis release, the commit that will be released needs to be
tagged with the new version tag, of the format X.Y.Z.
- X is a major version, and should only be incremented when major features are added to Artemis.
- Y is a minor version, it should be incremented when minor features are added to Artemis.
When a new X version is released, Y is returned to 0.
- Z is a fix version, it should be incremented when releases for Artemis are only to fix bugs
or correct small errors. When a new X or Y version is released, Z is returned to 0.

It is important to update the setup.py file with the new Artemis version.

*********************
Building the Protobuf
*********************

Artemis metadata is defined in io/protobuf/artemis.proto. An important component
of the metadata are histograms. Histograms are provided by the physt package
which includes io functionality to/from protobuf. However, the proto file is
not distributed with the package. This requires building the protobuf with
a copy of the histogram.proto class.

To build (from the io/protobuf directory)

.. code:: bash

  protoc -I=./ --python_out=./ ./artemis.proto


*******************
Artemis Job Example
*******************

An example job is available in examples/distributed_example_2.py which involves extracting dataset
schema from Excel, generating synthetic data, performing data analytics algorithms,and outputs distributions for data profiling.
Ensure that Artemis is built, then, run the following command.

.. code:: bash

  python examples/distribucted_example_2.py --location ./examples/data/example_product.xlsx


The example schema is located in examples/data/example_product.xlsx. To create new dataset schemas
please see instructions in artemis/tools/Excel_template/README.md
