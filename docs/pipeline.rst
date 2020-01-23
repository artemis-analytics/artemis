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

##############
Data Pipeline
##############

Data pipeline refers to the distinct stages when data is read, stored, and processed to achieve complete an entire data lifecycle. 
The Artemis data lifecycle consists of:
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
