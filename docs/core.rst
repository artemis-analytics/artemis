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
Core modules 
############

This documentation describes the core modules required for processing data with Artemis.

Data Processing
===============
3. Arrow in-memory processing. Once data is converted to the standard in-memory Arrow format, any arbitrary data transformation can be applied.

a. Filtering. Efficient column-level filtering can be applied to realize a new Arrow record batch with a subset of columns from a parent record batch.
b. Profiling. Numerical columns are profiled using the T-digest algorithm. TDigests are retained in the metadata, available for post processing in a data pipeline.


Execution Engine and Inter-algorithm communication 
--------------------------------------------------
Artemis provides access to data from different sources. The data is read into native Arrow buffers directly and all processing is performed on these buffers. The in-memory native Arrow buffers are collected and organized as collections of record batches in order to build new on-disk datasets given the stream of record batches. Once raw data is materialized as Arrow record batches, Artemis needs to provide the correct data inputs to the algorithms so that the final record batches have the defined BPM applied to the data.

Artemis has a top-level algorithm, *Steering*, which serves as the execution engine for the user-defined algorithms. The Steering algorithm manages the data dependencies for the execution of the BPM by seeding the required data inputs to each Node in a Tree via an Element. The Tree data structure is the directed graph generated from the user-defined BPM Menu. Steering holds and manages the entire Tree data structure, consisting of the Elements of each Node, the relationships betweens Nodes, and all Arrow buffers attached to the Elements (a reference to the Arrow buffers). The Elements serve as a medium for inter-algorithm communication. The Arrow buffers (data tables) are attached to Elements and can be accessed by subsequent algorithms.
TODO
Diagram Data Access via Elements

An interesting comparison with the Gandiva contribution to Arrow from Dremio elucidates some parallels to Artemis. Gandiva is a C++ library for efficient evaluation of arbitrary SQL expressions on Arrow buffers using runtime code generation in LLVM. Gandiva is an indepdent kernel, so in principle could be incorporated into any analytics systems. The application submits an expression tree to the compiler, built in a language agnostic protobuf-based expression representation. Once python bindings are developed for Gandiva expressions, Artemis could embed the expressions directly in the algorithm configuration.
Planned developments in Arrow also extend to both algebra operators as well as an embeddable C++ execution engine. The embedded executor engine could be used in-process in many programming languages and therefore easily incorporated into Artemis. Arrow developers are taking inspiration from ideas presented in the Volcano engine and from the Ibis project.

Metadata Access
---------------

:class:`artemis.core.gate.ArtemisGateSvc`

Access to framework-level resources in algorithms, such as metadata, histograms and timers is managed via a single class that is 
available in any algorithm inheriting from the base class.
