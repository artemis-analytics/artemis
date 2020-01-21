###############
Data Synthesis
###############

Artemis provides an in-situ data synthesizer, SimuTable, to enable development, testing, and simple modeling of data. The DataHandler can be configured as an instance of SimuTable to provide synthetic, but realistic datums which conform to format and schema of common data sources. Simutable extends the functionality to provide realistic record linkage data, through the generation of common record linkage data along with common errors embedded into the data. 

Development Task(s)

* Extend the set of generator functions in SimuTable to support a wide-range of common statistical distributions to sample. This enables generating synthetic data with known parameters per column. Currently, this is intended for univariate distributions.
* Extend SimuTable to simulate from a set of TDigest(s), each describing a column of profiled data.
* Develop a set of complex models which can serve as a data intensive source of simulated data.
* Combine the TDigest model implementation with the data pipeline development for a complete, end-end processing of real data, modeling, generation, and model validation.
