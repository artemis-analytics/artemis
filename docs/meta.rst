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

########
Metadata
########
This is the documentation which describes the Artemis metadata model(s) and the Cronus metadata service.
Artemis metadata schemas are implemented in protobufs.

In order to ensure flexibility, reproducibility and to separate the definition of the BPM from the execution, 
Artemis requires a metadata model with a persistent representation. The persistent, serialized metadata must be flexible to work with, 
to store, and to use within Artemis and other applications. The Artemis metadata model must be able to support external application development 
and use within a scalable data production infrastructure.

Artemis metadata model is defined in the google protobuf messaging format. Protocol buffers are language-neutral, platform-neutral, 
extensible mechanism for serializing structured data – think XML, but smaller, faster, and simpler. 
Protocol buffers were developed by Google to support their infrastructure for service and application communication. 
Protobuf message format enables Artemis to define how to stucture metadata once, then special source code is generated 
to easily write and read the structured metadata to and from a variety of data streams and using a variety of languages. (Google developor pages). 
Protobuf messages can be read with reflection without a schema, making the format extremely flexible in terms of application development, use and persistency.
The serialized protobuf is a bytestring, in other words a BLOB (binary large object), which is a flexible, lightweight storage mechanism for metadata. 
The language-neutral message format implies that any application can be built to interact with the data, while the BLOB simplifies storage. 
The messages can be persisted simply as files on a local filesystem, or for improved metadata management the messages can be cataloged in a 
simple key-value store. Applications can persist and retrieve configurations using a key.

The idea of persisting the configuration as well as managing the state of Artemis derived from experimentation with the 
Pachyderm data science workflow tool and Kubernetes. Moreover, Arrow intends to develop secure, over the wire message transport layer in the 
Arrow Flight project using gRPC and protobuf. Artemis can leverage Arrow Flight along with gRPC to build scalable, secure, 
data processing architecture that can be flexible for cloud-native and HPC deployments.

Artemis Information Management Model
====================================

Artemis Metadata Model
----------------------

The Artemis metadata model has three primary components:
1. The defintion of the data processing job, i.e. all the required metadata to execute a business process model.

   a. Defintion of the data source or source(s)
   b. Definition of business process model for that particular data source(s)
   c. The configuration of algorithms, tools, and services required to execute the business process model

2. Job processing metadata, i.e. metadata required to support the execution of the BPM and to retain data provenance.
    
    a. The current state of the job
    b. Metadata related to the raw data source
    c. Metadata associating raw input data, intermediate data and output data (provenance)

3. Summary metadata
    
    a. Statistical information gathered during the processing of the data
    b. Cost information (timing disrtibutions) for processing stages, algorithm and tool execution

Detailed information on the model can be found in the Appendix.

Menu
----
This section describes the Business Process Model and creation of an execution graph.

Artemis design decouples the definition of the business process model, BPM, from the execution of those business processes on the data. Business process models are defined by the user and retained in the Artemis metadata. The flexibility of defining, retaining and storing the BPM in the metadata enables various configurations to be used on the same data, allows for the job to be reproducible, and facilitates data validation and code testing.
The BPM can be expressed as a directed graph, describing the relationship between data, their dependencies, and the processes to be applied to the data. The user defines the input(s), the output, the process to be applied to the input(s), and the algorithms which consistute a distinct business process. Each business process consumes data and produces new data, where the new data is the output of 
one or several algorithms which transform the data. Once the business processes are expressed in a series of algorithms, the processes must be transformed from a directed graph to a linear ordering of processes (where each process is a list of algorithms using a common input). The ordering of algorithms must ensure that the data dependencies are met before the execution of an algorithm.
TODO
Diagram Business Process Model
The ordering of the algorithmic execution is handled through a sorting algorithm. Users only need to ensure their pipeline defines the input(s), the sequence of algorithms to act on those inputs, and the output. The directed graph of data relationships, Tree, and the execution order is defined in the Artemis metadata.

Definitions:

**Sequence** - Tuple of algorithms (by name) which must be executed in the order defined.

**Node** - Name of an output business process. (COMMENT: We need to standardize/figure out our naming scheme. Node replaces Element in our menu definition. Also, if an Element is the output of a business process, we should state here what is the business process.)

**Chain** - Unordered list of input Elements (by name), a Sequence, and a single output Element.

**Menu** - Collection of user-defined Chains to be processed on a particular dataset.

**Tree** - Resulting directed graph data structure from the Menu

Artemis application requires the topological sorted list of output Nodes and Sequences. The topological sort of a directed graph is a linear ordering of the vertices such that for every directed edge uv from vertex u to vertex v, u comes before v in the ordering. Artemis uses the python library toposort to create the ordered list. (For more information, please refer to the Wikipedia description of topological sorting and the toposort algorithm available on BitBucket.) The resulting Tree and execution order is stored in metadata to be made available to Artemis.

Configuration
-------------

Properties
----------

Datasets
--------

Cronus Data Management System
=============================


