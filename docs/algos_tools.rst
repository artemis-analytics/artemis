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

####################
Algorithms and Tools
####################
Similar to the idea of numpy user-defined functions, Artemis supports user-defined algorithms and tools. 
Any algorithm or tool which works with Arrow data structures can be easily incorporated into an 
Artemis BPM and executed on a dataset. Algorithms support user-defined properties in order to easily 
re-use algorithmic code to perform the same task with different configurations. 
Developers implement the base class methods, define any defined properties, and access the Arrow buffers 
through the Element. Steering manages algorithm instantiation, scheduling and execution. 
For the end-user the most important part of the code is defined in the execute method. 
Code organization and re-use can be improved by delegating common tasks which return a value to tools. 
The scheduling of the tools is managed directly in the algorithm, in other words, it is up to the user to apply 
the tools in the appropiate order.

Metaclasses and Base Classes
----------------------------

Algorithms
----------
Algorithms provide the following functionality:

* Access to data
* Executing tools to transform data
* Histogram and timer creation and filling

An example user-defined algorithm

.. code-block:: python

    class MyAlgo(AlgoBase):
        def __init__(self, name, **kwargs):
            super().__init__(name, **kwargs)
            # kwargs are the user-defined properties
            # defined at configuration 
        def initialize(self):
            pass
        def book(self):
            # define histograms and timers
        def execute(self, element):
            # Algorithmic code
        def finalize(self):
            # gather any user-defined summary information

Registering Histograms and Timers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Retrieving and storing data
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Executing Tools
^^^^^^^^^^^^^^^

Tools
-----

Developing Analytical Tools with Apache Arrow
---------------------------------------------
