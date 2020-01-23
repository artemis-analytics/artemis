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

###############
Data Synthesis
###############

Artemis provides an in-situ data synthesizer, SimuTable, to enable development, testing, and simple modeling of data. The DataHandler can be configured as an instance of SimuTable to provide synthetic, but realistic datums which conform to format and schema of common data sources. Simutable extends the functionality to provide realistic record linkage data, through the generation of common record linkage data along with common errors embedded into the data. 

Development Task(s)

* Extend the set of generator functions in SimuTable to support a wide-range of common statistical distributions to sample. This enables generating synthetic data with known parameters per column. Currently, this is intended for univariate distributions.
* Extend SimuTable to simulate from a set of TDigest(s), each describing a column of profiled data.
* Develop a set of complex models which can serve as a data intensive source of simulated data.
* Combine the TDigest model implementation with the data pipeline development for a complete, end-end processing of real data, modeling, generation, and model validation.
