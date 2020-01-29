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

############
Data Quality 
############
The data quality is an inherent part of data analysis, and therefore needs to be part of the core design of Artemis. The importance of data quality transcends domains of science, engineering, commerce, medicine, public health and policy. Traditionally, data quality can be addressed by controlling the measurement and data collection processes and through data ownershp. The increased use of administrative data sources poses a challenge on both ownership and controls of the data. Data quality tools, techniques and methodologies will need to evolve.

In the scientific domain, data quality is generally considered an implicit part of the role of the data user. Statistical infrastructure will need support the data users ability to measure data quality throughout the data lifecycle. Data-as-a-service oriented enterpise data solutions are not generally focused on ensuring the measurement of data quality. As well, the statistical tools required for data quality meaurements need to be developed to address data quality issues in administrative data. Artemis primary statistical analysis tool for data quality is the histogram.

Histograms are the bread-and-butter of statistical data analysis and data visualization, and a key tool for quality control. They serve as an ideal statistical tool for describing data, and can be used to summarize large datasets by retaining both the frequencies as well as the errors. The histogram is an accurate representation of a distribution of numerical data (or dictionary encoded categorical data), and it represents graphically the relationship between a probability density function f(x) and a set of n observations, x, x1, x2, ... xn.

Artemis retains distributions related to the processing and overall cost of the processing for centralized monitoring services:

* Timing distributions for each processing stage
* Timing distributions for each algorithm in the BPM
* Payload sizes, including datums and chunks
* Memory consumption
* Total batches processed, number of records per batch
* Statistics on processing errors

Separate histogram-based monitoring applications can be developed as a postprocessing stage of Artemis and will be able to quickly iterate over large datasets since the input data are Arrow record batches. Automated profiling of the distributions of the data is forseen as well for input to the postprocessing stage data quality algorithms. Further discussion on automated profiling is included in the appendix.

Histogram and Timer Store
-------------------------
Histograms and timers are centralled managed by the framework. The managed store allow Artemis to collect histograms and timing information into the metadata, serialize and persist this information at the job finalize stage. The stores support booking and filling histograms in any user-defined algorithms.
The histogram store also works as proxy to different histogram representations. Artemis currrently support two libraries histbook (from diana-hep) and physt (janpipek). Use in Artemis is primarily with physt since conversion to and from protobuf is supported. Once the physt histogram is converted to a protobuf message, the collection is added to the job metastore.
Algorithms and Tools
Similar to the idea of numpy user-defined functions, Artemis supports user-defined algorithms and tools. Any algorithm or tool which works with Arrow data structures can be easily incorporated into an Artemis BPM and executed on a dataset. Algorithms support user-defined properties in order to easily re-use algorithmic code to perform the same task with different configurations. Developers implement the base class methods, define any defined properties, and access the Arrow buffers through the Element. Steering manages algorithm instantiation, scheduling and execution. For the end-user the most important part of the code is defined in the execute method. Code organization and re-use can be improved by delegating common tasks which return a value to tools. The scheduling of the tools is managed directly in the algorithm, in other words, it is up to the user to apply the tools in the appropiate order.
Histogram and Timer Store
Histograms and timers are centralled managed by the framework. The managed store allow Artemis to collect histograms and timing information into the metadata, serialize and persist this information at the job finalize stage. The stores support booking and filling histograms in any user-defined algorithms.
The histogram store also works as proxy to different histogram representations. Artemis currrently support two libraries histbook (from diana-hep) and physt (janpipek). Use in Artemis is primarily with physt since conversion to and from protobuf is supported. Once the physt histogram is converted to a protobuf message, the collection is added to the job metastore.

Logging and exception handling
------------------------------
Artemis uses standard python logging module as well as exception handling. The framework provides a logging functionality with a decorator. All algorithms can access a logger, using self.__logger.info("My message").
Exceptions must be raised in order to propagate upward. As long as all exceptions can be handled appropiately, the framework gracefully moves into an Abort state if the exception prevents further processing.

Data Postprocessing
-------------------
