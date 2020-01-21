.. pyartemis documentation master file, created by
   sphinx-quickstart on Fri Jan 10 14:54:05 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyartemis's documentation!
=====================================

.. toctree::
   :maxdepth: 2
   :caption: Table of Contents
   
   gettingstarted

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


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
