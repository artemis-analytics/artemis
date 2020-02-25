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
Similar to the idea of numpy user-defined functions, Artemis supports user-defined *algorithms* and *tools*. 
The role of an *algorithm* is to retrieve data from an input data source, execute analytical tools to transform the data, 
store the resulting output data, and perform monitoring, analytic tool profiling, and logging. The role of a *tool* 
is to execute an analytical operation on a data buffer and return a result back to the *algorithm*. Users create a :ref:`meta:Menu`
which defines the input data and the sequence of *algorithms* to execute on the data. 
Inter-algorithm communication is faciliated through a managed in-memory data store. *Steering* prepares data, executes 
the algorithms as defined in a given :ref:`meta:Menu`, and performs algorithm profiling. 
The execution of the *tools* is managed within the *algorithm*. 
*Tools* can be shared across *algorithms*, thus performing the same action on different data.  However, *algorithms* are associated with a specific 
node and expect a pre-defined data input.

Metaclasses, Base Classes and Mixins
------------------------------------
:ref:`api:Base Classes`

Artemis class structure is designed to provide users with the following capabilities within algorithms:

* Automated logging that allows for debugging
* Access to managed histograms, timers, shared tools and metadata
* Helper methods via mixin classes

**AbcAlgoBase** is the abstract base class for *tools* and *algorithms*. The Abc ensures that loggers are available to all algorithms, and that logging
can be identified according to a *algorithm* or *tool* name.

**AlgoBase** is the base class for all user-defined algorithms. User-defined algorithms have access to `ArtemisGateSvc` through defined Mixin methods. 
Mixin class methods will continue to be added so that users do not directly use the `ArtemisGateSvc` object.

Properties
----------
Artemis supports dynamic configuration and reproducibility with python class attributes added at runtime from the metadata. 
Tools and algorithms are instantiated with the properties in a configuration module that defines everything that will
be performed on a dataset. All relevant information is then persisted before the data is processed. 

The protobuf model for an algorithm or tool:

.. code-block:: protobuf
    
    message Module {
        string name = 1;
        string module = 2;
        string klass = 3;
        Properties properties = 4;
    }

Properties are the required configurable parameters.

.. code-block:: protobuf
    
    message Property {
        string name = 1;
        string type = 2;
        string value = 3;
    }

Internally, the strings are converted when required and supports floats, integers, bools, strings and dictionaries. 
Properties are created from key-word arguments at instantiation:

.. code-block:: python
    
    csvtool = CsvTool('csvtool', block_size=2**16)

The property can then be accessed at run time with a property class attribute in an algorithm or tool:

.. code-block:: python
    
    def initialize(self):
        block_size = self.properties.block_size

A decorator is available for defining default properties that can be updated by the user. This allows for
documenting all required configurable properties, defines suitable default parameters, and ensures that
all configuration properties is always stored in the metadata.

.. code-block:: python
    
    @iterable
    class MyProperties:
        myproperty = True
    
    class MyAlgo(AlgoBase):
        def __init__(name, **kwargs):
            options = **dict(MyProperties())
            options.update(kwargs)

            super.__init__(name, **options)

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
Logging
^^^^^^^
Standard python logging is available, either info or debug logging can be used. 
Logging examples

.. code-block:: python
    
    self.__logger.info("Info")
    self.__logger.debug("Debug")
    self.__logger.info('algorithm name: %s' % self.name)

Registering Histograms and Timers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Histograms and timers are managed in collections, and access is made available in `self.gate`.

Book a histogram

.. code-block:: python
    
    self.gate.hbook.book(self.name, 'histogram name', bins)

where `self.name` is an attribute of the algorithm, `histogram name` is defined by the user, and bins is an array.

Book a timer

.. code-block:: python
    
    self.gate.hbook.book(self.name, 'time.name', bins, 'label', timer=True)

where `self.name` is an attribute of the algorithm, `time.name` is defined by the user, and bins is an array. Setting timer to true allows for the contents 
to be reset in the case of sampling data to define the bins and range of the histogram according to the profiling of data processing.

To fill the histogram,

.. code-block:: python
    
    self.gate.hbook.fill(self.name, 'histogram name', value)

Retrieving and storing data
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Data access occurs in the *execute* method via the *element*. 

Retrieve data

.. code-block:: python
    
    raw_data = element.get_data()

Store data

.. code-block:: python
    
    element.add_data(curated_data)


Executing Tools
^^^^^^^^^^^^^^^
Tools are accessible via their configured name. Mixin method is available to faciliate tool access.

.. code-block:: python
    
    curated_data = self.get_tool('mytool').execute(raw_data)

Here is an example. Note that algorithms must be properly initialized with the *name* and *kwargs*. The key-word args
converts stored properties of algorithms to configurables for the class. This allows for algorithms to be re-used with different 
configurations.

.. code-block:: python

    class CsvParserAlgo(AlgoBase):

        def __init__(self, name, **kwargs):
            super().__init__(name, **kwargs)
            self.__logger.info('%s: __init__ CsvParserAlgo' % self.name)
        def initialize(self):
            self.__logger.info('%s: Initialized CsvParserAlgo' % self.name)

        def book(self):
            self.__logger.info("Book")
            bins = [x for x in range_positive(0., 100., 2.)]
            self.gate.hbook.book(self.name, 'time.pyarrowparse',
                                    bins, 'ms', timer=True)

        def execute(self, element):
            raw_data = element.get_data()
            try:
                curated_data = self.get_tool('csvtool'.execute(raw_data)
            except Exception:
                self.__logger.error("PyArrow parsing fails")
                raise
            element.add_data(curated_data)

        def finalize(self):
            self.__logger.info("Parsing complete")
                                                                                                                    
Exceptions
^^^^^^^^^^
Exceptions can be handled in the framework in a way that allows Artemis to abort a job without a fatal crash. 
Exceptions in a tool must be caught and evaluated within an algorithm. If the exception prevents data to
be processed, then the it must be raised in the algorithm. In addition, data access should be validated such that 
if data is not retrieved, the job should abort. Exceptions raised in an *algorithm* will be handled by *Steering* 
first then up to the *Artemis* application such the job can be aborted in a safe manner. 

Tools
-----
Tools are intended to perform operations on data and do not access any components of the framework. Tools
rely on data to be passed to them from an algorithm. The aim of a *tool* is to encapsulate a complex analytical
function that can be used standalone. In most cases, a *tool* in Artemis is simply a wrapper where the actual
tool may be in a seperate libary, module, etc.. For example, a machine learning algorithm such as a classifier
can be easily added to Artemis without any changes, simply importing the module.

From the example above, the *csvtool* is a wrapper to Apache Arrow csv reader which is low-level C++ code with python bindings.
The wrapper ensures that the configuration of the underlying tool is stored in metadata and the data is passed from the data store, 
to the algorithm then on to the tool.

.. code-block:: python
    
    @iterable
    class CsvToolOptions:

        # Add user-defined options for Artemis.CsvTool
        pass


    class CsvTool(ToolBase):

        def __init__(self, name, **kwargs):

            # Retrieves the default options from arrow
            # Updates with any user-defined options
            # Create a final dictionary to store all properties
            ropts = self._get_opts(ReadOptions(), **kwargs)
            popts = self._get_opts(ParseOptions(), **kwargs)
            copts = self._get_opts(ConvertOptions(), **kwargs)
            options = {**ropts, **popts, **copts, **dict(CsvToolOptions())}
            options.update(kwargs)

            super().__init__(name, **options)
            self.__logger.info(options)
            self._readopts = ReadOptions(**ropts)
            self._parseopts = ParseOptions(**popts)
            self._convertopts = ConvertOptions(**copts)
            self.__logger.info('%s: __init__ CsvTool', self.name)
            self.__logger.info("Options %s", options)

        def _get_opts(self, cls, **kwargs):
            options = {}
            for attr in dir(cls):
                if attr[:2] != '__' and attr != "escape_char":
                    options[attr] = getattr(cls, attr)
                    if attr in kwargs:
                        options[attr] = kwargs[attr]
            return options

        def initialize(self):
            pass
             def execute(self, block):
        '''
        Calls the read_csv module from pyarrow

        Parameters
        ----------
        block: pa.py_buffer

        Returns
        ---------
        pyarrow RecordBatch
        '''
        try:
            table = read_csv(block,
                             read_options=self._readopts,
                             parse_options=self._parseopts,
                             convert_options=self._convertopts)
        except Exception:
            self.__logger.error("Problem converting csv to table")
            raise
        # We actually want a batch
        # batch can be converted to table
        # but not vice-verse, we get batches
        # Should always be length 1 though (chunksize can be set however)
        batches = table.to_batches()
        self.__logger.debug("Batches %i", len(batches))
        for batch in batches:
            self.__logger.debug("Batch records %i", batch.num_rows)
        if len(batches) != 1:
            self.__logger.error("Table has more than 1 RecordBatches")
            raise Exception

        return batches[-1]


Developing Analytical Tools with Apache Arrow
---------------------------------------------

Introduction
^^^^^^^^^^^^

Artemis is primarily a Python project. However, there are some use cases, like file reading and chunking, 
where Artemis would benefit from a more performant, lower-level language, like C++. 
To properly integrate C++ code into Artemis, we would need to be able to pass our data (Apache Arrow datatypes) 
and IPC (Google Protocol Buffers) between Python and C++ scripts. 
Google's Protocol Buffers are already language-neutral, but getting the same behaviour from Apache Arrow, 
we will need to explore a little bit. In this section we discuss:

* Methods to implement lower-level/slower parts of Artemis in C++.
* How to Interface C++ projects with Python.
* How to Pass Arrow datatypes between Python and C++.
* How to develop a configurable analytical tool for Artemis.  

Planning
^^^^^^^^
* Wrap a simple C++ class with Cython and access the resulting module from Python.
* Write a simple C++ class that uses Apache Arrow datatypes.
* Modify the C++ class to wrap the C++ Arrow datatypes for Python using Apache Arrow's arrow_python library.
* Cythonize a C++ class that uses Apache Arrow's arrow_python library to convert to and from Arrow datatypes.

.. _Basic Cython:

Basic Cython
^^^^^^^^^^^^
Cython is a Python module that takes C++ code and template files(s) (.pyx, .pxd) and automatically creates a 
Python wrapper for the C++ code using the Python/C++ API. This process generates a new C++ file, 
which is then compiled into a shared object library. From that point, you can import the module as usual. 
Note that shared object files take precedence over Python files for Python imports: "import thing" 
will import thing.so over thing.py if both exist in the search path. When you call this new module, 
while the interface is Python, it is actually running compiled C++ code behind the scenes.

We are using it because it is simple and it is standard. Many Python/C++ projects use Cython in some way, 
including Apache Arrow.

**Example Cython Template**

Important note: only functions, classes and attributes defined in the template are made accessible to Python. 
These templates are also written in Cython, a slightly extended version of Python which allows for static typing and the use of certain C libraries.

For the following examples, assume there is a Thing.h and a Thing.cpp file.

.pxd

This file is similar to a C/C++ header and defines which functions, classes and attributes will be made available to Python. For example:


.. code-block:: cython
    
    # thing.pxd
    cdef extern from "Thing.h":
        cdef cppclass Thing:
            Thing(c_type init_arg) except +
            c_type attribute
            c_type function(c_type arg)

Where c_type would be replaced by an actual type, either by importing from the C++ standard library (from libcpp import bool as c_bool) 
or using a type with automatic conversion (float). Notice that the constructor is followed by "except +". 
This attempts to translate any C++ errors to their appropriate Python counterpart. 
For more information on error-handling, look here.

.. code-block:: cython
    
    # py_thing.pyx
    
    # distutils: sources = CPP_SOURCE_FILE
    # distutils: language = c++

    from Thing cimport Thing as _Thing
    cdef class Thing:
        cdef _Thing *c_self
        
        def __cinit__(self, c_type init_arg):
            self.c_self = new _Thing(init_arg)
        
        def function(self, c_type arg):
            return self.c_self.function(arg)

        @property
        def attribute(self):
            return self.c_self.attribute

        @attribute.setter
        def attribute(self, value):
            self.c_self.attribute = value
        
        def __dealloc__(self):
            del self.c_self

A few key things:
* `__cinit__` runs once at startup and is used to create the internal C++ object.
* `__dealloc__` runs once at teardown and is used to free any memory currently in use by the program.
* To get/set attributes, you need to use the property and setter decorators. These define interactions with the attribute.


**Type Conversion**

The original C++ class still expects and returns C++ types. In some cases, these are not compatible with Python types. 
Generally, standard types like int, float, double and bool convert automatically. 
The main exceptions are strings, whether they're character pointers or from the standard library's string class. 
For these, convert as follows: C/C++ string ↔ Python bytes ↔ Python str. To make interfacing with the program easy, 
this should be handled in the .pyx file under the property and setter decorators; expect a str from the user, 
convert to bytes internally and pass the bytes object as an argument to C++ object.

To include the proper C/C++ type in the Cython files, you can pull from the C (libc) and C++ (libcpp) standard libraries in Cython. 
Among others, libcpp supports bools, strings, vectors, maps and shared pointers, and libc's stdint (standard integer) library supports C integers (uint8_t, etc.).

.. code-block:: cython
    
    # thing.pxd
    cdef extern from "Thing.h":
        cdef cppclass CThing:
            c_string string_arg

.. code-block:: cython
    
    # py_thing.pxd
    
    from Thing cimport *  # pull in objects from .pxd
    cdef class Thing:
        cdef CThing *c_thing
        
        def __init__(self, string_arg):
            self.string_arg = string_arg
        
        @property
        def string_arg(self):
            return (self.c_thing.string_arg).decode('utf8')
        
        @string_arg.setter
        def string_arg(self, value):
            if isinstance(value, str):
                self.c_thing.string_arg = value.encode('utf8')
            else:
                self.c_thing.string_arg = value

Now, when anyone accesses the attribute string_arg, it's converted to the appropriate type.

**Building with Cython: The Setup File**

Setup.py controls the Cython build process, including the compilation for the C++ files. The easiest way seems to be to define the module as an extension,
add Cython arguments to it and then cythonize.

.. code-block:: python
    
    # setup.py
    from distutils.core import setup, Extension
    from Cython.Build import cythonize
    ext_modules = [Extension(name='MODULE_NAME', sources=['YOUR_PYX_HERE'])]
    for ext in ext_modules:
        ext.include_dirs.append('PATH/TO/HEADERS/TO/INCLUDE')
        ext.library_dirs.extend(['DIRS/TO/ADD/TO/LIBRARY/SEARCH/PATH'])
        ext.libraries.extend(['LIBRARY_TO_LINK_TO'])
        ext.extra_compile_args.append('-std=c++11')
        ext.extra_link_args.append('-Wl,-rpath,$ORIGIN')
    setup(ext_modules=cythonize(ext.modules),)

Running `python setup.py build_ext --inplace` will cythonize the extension.
Filling in more options in setup (version, packages, name, etc.) will let you install the final package 
with the Cython files compiled at install time.

For a complete example, include notebooks/wrapping-with-cython.md

Apache Arrow in C++
^^^^^^^^^^^^^^^^^^^

Apache Arrow develops first in C++ and then creates bindings for a number of other languages, 
including Python. So the C++ implementation includes some functionality that has not yet been 
implemented in other languages.

**Including Modules**

Every major Apache Arrow module (csv, io, etc.) has an API. 
To interact with that module, you only need to include the corresponding API. 
For general Arrow objects: #include <arrow/api.h>. 
For specialized modules: #include <arrow/lowercase_module_name/api.h>, like <arrow/csv/api.h>.

**Apache Arrow's Encapsulation Style**

Constructors for all classes are protected. To create an instance of a class, you need to create a shared pointer of that object type 
(e.g. for arrow::Table, I would make std::shared_ptr<arrow::Table> table) and pass this shared_pointer to a static method from that class. 
This static method accesses the constructor and creates the new instance at the shared pointer's location in memory. 
The user never handles the actual object, only the shared pointer to the object.

.. code-block:: cpp

    #include <arrow/api.h>
    #include <arrow/io/api.h>
    int main() {
        // Create a shared pointer for a future arrow::io::ReadableFile
        std::shared_ptr<arrow::io::ReadableFile> file;
        // Put an arrow::io::ReadableFile object at file's location
        arrow::Status st = arrow::io::ReadableFile::Open(file_name, memory_pool, &file);
        // Ensure the read was successful
        if (!st.ok()) {
            std::cerr << st.ToString() << std::endl;
            exit(EXIT_FAILURE);
        }
        return EXIT_SUCCESS;
    }

This doesn't have any real effect on Python development–Arrow's pyarrow/Cython API handles this–but is crucial for C++ development with the library.

Next, do the linking to Apache Arrow's C++ Libraires
`g++ -std=c++11 YOUR_FILE.cpp -I/ARROW/INCLUDE/PATH -L/ARROW/LIBRARY/PATH -larrow -o EXECUTABLE_NAME`

Arrow's pyarrow wheels comes with the prebuilt C++ dependencies bundled inside. These can be linked against, but they're not intended for that purpose and they're missing some newer components. They also stand a good chance of losing support in the next release (1.0) as the build system grows more complicated.
Arrow's arrow-cpp Conda package is built with the C++11 ABI (application binary interface). 
This means that systems using GCC <5.1 cannot link against these libraries. 
Even if you rebuild arrow-cpp from source with your compiler, 
the libraries it depends on are also built with this higher ABI. 
To get it to work, you would need to rebuild Arrow and all of its many, many dependencies from source. 

Alternatively, you can install gcc and g++ >5.1 inside your Conda environment. In conda, their library is managed by pkg-config, which can be used to locate
the library and headers. See the CMakeLists file below as an example.

Here is an example using the Arrow C++ csv reader in a C++ program.

.. code-block:: cpp
    
    // simple_csv_reader.cpp

    #include <iostream>

    #include <arrow/api.h>
    #include <arrow/io/api.h>
    #include <arrow/csv/api.h>

    int main() {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::shared_ptr<arrow::Table> table;
        
        // Get input stream
        std::shared_ptr<arrow::io::ReadableFile> file;  // this is a subclass of InputStream
        arrow::Status st = arrow::io::ReadableFile::Open("sample.csv", pool, &file);
        if (!st.ok()) {
            std::cerr << st.ToString() << std::endl;
            return EXIT_FAILURE;
        }
                                            
        // Generate the table from CSV
        std::shared_ptr<arrow::csv::TableReader> reader;
        st = arrow::csv::TableReader::Make(pool, file,
                                           arrow::csv::ReadOptions::Defaults(),
                                           arrow::csv::ParseOptions::Defaults(),
                                           arrow::csv::ConvertOptions::Defaults(),
                                           &reader);
        if (!st.ok()) {
            std::cerr << st.ToString() << std::endl;
            return EXIT_FAILURE;
        }
        st = reader->Read(&table);
        if (!st.ok()) {
            std::cerr << st.ToString() << std::endl;
            return EXIT_FAILURE;
        }
        
        // Read out the table
        for (int i=0; i < table->num_columns(); i++) {
                std::cout << "column " << i << "--";
                std::cout << "num_records:" << table->column(i)->length() << ", ";
                std::cout << "datatype:" << *(table->column(i)->type()) << ", ";
                std::cout << "data:" << *(table->column(i)->data()->chunk(0)) << std::endl;
        }

        return 0;
    }

Next, build with g++ (install gcc_linux-64 and gxx_linux-64 with Conda if your GCC < 5):
`$CXX -std=c++11 simple_csv_reader.cpp -I/PATH/TO/ARROW/HEADERS -L/PATH/TO/ARROW/LIBRARY -larrow -o test`

The first -I is an uppercase i, the second is a lowercase L.

To build with CMake:

.. code-block:: bash

    mkdir build && cd build
    cmake ..
    make
    cd .. && rm -r build

.. code-block:: bash
    
    # CMakeLists.txt

    cmake_minimum_required (VERSION 2.8)

    # Project settings
    project (simple-arrow)

    if ($ENV{CONDA_PREFIX} STREQUAL "")
        message (FATAL_ERROR "No active Conda environment found.")
    endif()

    set (VENV $ENV{CONDA_PREFIX})

    # Must use C++11
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

    # Include Arrow
    find_package(PkgConfig)

    if (PkgConfig_FOUND)
        pkg_check_modules (ARROW REQUIRED arrow)
        link_directories (${ARROW_LIBRARY_DIRS})
        set (LIBS arrow)
        set (INCLUDE_DIRS ${ARROW_INCLUDE_DIRS})
    else()
        find_library (ARROW_LIB NAME arrow PATHS ${VENV}/lib)
        set (LIBS ${ARROW_LIB})
        set (INCLUDE_DIRS ${VENV}/include)  # default install location for Arrow headers
    endif()

    include_directories (${INCLUDE_DIRS})
    add_executable (run ${PROJECT_SOURCE_DIR}/simple_csv_reader.cpp)
    target_link_libraries (run ${LIBS})

For additional documentation, refer to Apache Arrow

* `Apache Arrow C++ Implementation Docs <https://arrow.apache.org/docs/cpp/index.html>`_
* `Apache Arrow C++ Reference <https://arrow.apache.org/docs/cpp/namespacearrow.html>`_
* `Apache Arrow C++ source <https://github.com/apache/arrow/tree/master/cpp>`_

Wrapping C++ Arrow Objects as Python Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Under the hood, Apache Arrow objects in Python are C++ objects with a Python wrapper over top. To pass these objects between languages, we just need to wrap and unwrap them. Fortunately, Arrow provides a C++/Python API for this. It's part of the arrow_python library, which comes standard with Conda installations of arrow-cpp and (while they're still supported) PyPi installations of pyarrow.
Using the arrow_python Library
At this stage, we're using the library through a C++ script. This complicates the dependencies a little. The arrow_python library assumes there is an active Python environment, so we will need to include the appropriate Python headers and shared library to activate it properly. Note that all elements from this library have the namespace arrow::py.
In addition to the usual Apache Arrow C++ includes, add the following:

.. code-block:: cpp
    
    #include <Python.h>; #if you're in Conda, this is in $CONDA_PREFIX/include/Python3.7m/Python.h, replacing "Python3.7m" for whatever version of Python you're running.
    #include <arrow/python/pyarrow.h>
    // Initialize the Python environment
    Py_Initialize();
    // Import pyarrow and associated wrapping/unwrapping functions
    if (arrow::py::import_pyarrow() != 0) {
        std::cerr << "Fatal error - pyarrow import failure" << std::endl;
        exit(EXIT_FAILURE);
    }

Once you've imported the pyarrow module, you have access to functions to wrap/unwrap the following types:

* `arrow::Array`
* `arrow::Buffer`
* `arrow::Column` (may be discontinued; Arrow considering removing this type)
* `arrow::DataType`
* `arrow::Field`
* `arrow::RecordBatch`
* `arrow::Schema`
* `arrow::Table`
* `arrow::Tensor`

It also includes functions to check if a PyObject is the Python equivalent of one of those types.

* Wrapping: `PyObject *wrap_array(const std::shared_ptr<arrow::Array> &array)`
* Unwrapping: `arrow::Status unwrap_array(PyObject *obj, std::shared_ptr<arrow::Array> *out)`
* Validating type: `bool is_array(PyObject *obj)`

The function naming scheme is the same for other Arrow objects.

Example code:

.. code-block:: cpp
    
    // Holder.h

    #ifndef HOLDER_H
    #define HOLDER_H
    #include <iostream>
    #include <Python.h>
    #include <arrow/api.h>
    #include <arrow/python/pyarrow.h>
    class Holder {
        public:
            Holder();
            PyObject* create_array();
            std::shared_ptr<arrow::Array> unwrap_array(PyObject* py_array);
       };
    #endif

.. code-block:: cpp
    
    // Holder.cpp

    #include "Holder.h"

    Holder::Holder() {
    /* Initialize class and activate Arrow's C++ pyarrow API */
    Py_Initialize();  // only needed when running in C++
    if (arrow::py::import_pyarrow() != 0) {
        std::cerr << "FATAL ERROR - pyarrow import failure" << std::endl;
        exit(EXIT_FAILURE);
        }
    }
    PyObject* Holder::create_array() {
        // Build a C++ arrow::Array
        std::shared_ptr<arrow::Array> array;
        arrow::Int64Builder builder;
        for (int i=0; i < 5; ++i) {
            builder.Append(i);
        }
        arrow::Status st = builder.Finish(&array);
        if (!st.ok()) {
            std::cerr << "ERROR - building the array failed" << std::endl;
            exit(EXIT_FAILURE);
        }
                                                                                        
        // Wrap the array as a Python object
        return arrow::py::wrap_array(array);
    }
    std::shared_ptr<arrow::Array> Holder::unwrap_array(PyObject* py_array) {
        std::shared_ptr<arrow::Array> array;
        arrow::Status st = arrow::py::unwrap_array(py_array, &array);
        if (!st.ok()) {
            std::cerr << "ERROR - unwrapping the PyObject failed" << std::endl;
            exit(EXIT_FAILURE);
        }
        return array;
    }

    int main() {
        Holder holder;
        auto py_array = holder.create_array();
        auto array = holder.unwrap_array(py_array);    
                                                                                                                                                
        // Print resulting array
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
        for (int i=0; i < array.get()->length(); i++) {
            std::cout << int64_array->Value(i) << " ";
        }
        std::cout << std::endl;
        return EXIT_SUCCESS;
    }

To build: include Python.h and arrow headers, and link against the arrow, arrow_python and python libraries. 
Install gcc_linux-64 and gxx_linux-64 from conda if your GCC < 5.
`$CXX -std=c++11 Holder.cpp -I$CONDA_PREFIX/include -I$CONDA_PREFIX/python37m -L$CONDA_PREFIX/lib -larrow -larrow_python -lpython3.7m -o test`

`Apache Arrow documentation for the C++ API <https://arrow.apache.org/docs/python/extending.html#c-api>`_

Putting it all together
^^^^^^^^^^^^^^^^^^^^^^^

From the previous steps, we have:

* Cython template files
* Python setup.py file
* C++ class (header and source) using Apache Arrow

We just need to bring them together. There are two options here, depending on how we want to Connect C++ Apache Arrow and Python. 
If the C++ code already handles the conversion to Python types, the exposed functions (ones in the Cython templates) 
are already pretty much Python-native and no complicated conversion is necessary. You can ignore that it's an Arrow datatype altogether. The conversion happens automatically. See:

.. code-block:: cpp
    
    // array_funcs.h

    #include <iostream>

    #include <Python.h>
    #include <arrow/api.h>
    #include <arrow/python/pyarrow.h>

    PyObject* create_array(int n);

array_funcs.cpp

.. code-block:: cpp
    
    // array_funcs.cpp

    #include "array_funcs.h"

    int active = 0;

    PyObject* create_array(int n) {
        if (!active) {
            if (arrow::py::import_pyarrow() != 0) {
                std::cerr << "FATAL_ERROR - pyarrow import failure" << std::endl;
                exit(EXIT_FAILURE);
            }
            active = 1;
            }
        }
        std::shared_ptr<arrow::Array> array;
        arrow::Int64Builder builder;
        for (int i=0; i < n; i++) {
            builder.Append(i);
        }
        arrow::Status st = builder.Finish(&array);
        if (!st.ok()) {
            std::cerr << "ERROR - building the array failed" << std::endl;
            exit(EXIT_FAILURE);
        }
        return arrow::py::wrap_array(array);
    }

.. code-block:: cython
    
    # py_array_funcs.pyx

    # distutils: sources = array_funcs.cpp
    # distutils: language = c++
    # cython: language_level = 3

    cdef extern from "array_funcs.h":
        cdef create_array(int n);
        
    def py_create_array(int n):
        return create_array(n)

Next, create the setup.py file to build the extension.

.. code-block:: python
    
    # setup.py
    from distutils.core import setup, Extension
    from Cython.Build import cythonize

    import os
    import numpy as np
    import pyarrow as pa

    ext_modules = [Extension('holder', ['holder.pyx']), Extension('py_array_funcs', ['py_array_funcs.pyx'])]

    for ext in ext_modules:
        ext.include_dirs.append(np.get_include())
        ext.include_dirs.append(pa.get_include())
        ext.libraries.extend(pa.get_libraries())
        ext.library_dirs.extend(pa.get_library_dirs())
                    
        # force C++11 usage
        if os.name == 'posix':
            ext.extra_compile_args.append('-std=c++11')
        
        ext.extra_compile_args.append('-w')  # disable warnings
    
    setup(ext_modules=cythonize(ext_modules),)

Then, run the build `setup.py build_ext --inplace`

This fits some use cases but others. It only allows for the C++ scripts to send Arrow object to Python. 
You can't send Python Arrow objects and pass them to the C++ portion of the project. If you need two-way communication, 
it's a little more complicated and you have to use Apache Arrow's Cython support. Below is an example using pyarrow with Cython (no C++), that involves

* Create a module in pyx file(s) in slightly modified python.
* Create the setup file to set the build parameters for the module
* Run the setup

.. code-block:: cython
    
    # attributes.pyx

    # distutils: language = c++
    # cython: language_level=3

    from pyarrow.lib cimport *

    def get_array_length(obj):
        cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(obj)
        if arr == NULL:
            raise TypeError('not an array')
        return arr.get().length()

    def get_array_type(obj):
        cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(obj)
        if arr == NULL:
            raise TypeError('not an array')
        return pyarrow_wrap_data_type(arr.get().type())

Notice that this looks a lot more like C++ than previous Cython examples. When this is compiled by Cython, it'll become C++ code. We need to be able to pass pyarrow objects between our Python program and this soon-to-be-C++ module. These are not inherently compatible so we need to take certain steps.
We define a C++ shared pointer of our Arrow datatype (array, in this case).
Arrow types in Cython are C[ARROW_OR_C++_NAME]. Array --> CArray


Unwrap the Python object to expose the C++ shared pointer underneath. This is possible because pyarrow is a collection of Python bindings to the original C++ code.
We now have a pointer, so we need to append .get() to access the array itself.

Similarly, to return a C++ type, we need to wrap it with the Python bindings.For reference, the equivalent Python-only script is below.

.. code-block:: python
 
    import pyarrow as pa

    def py_get_array_length(arr):
        return len(arr)

    def py_get_array_type(arr):
        return arr.type
        

setup.py 

.. code-block:: python
    
    # setup.py

    from distutils.core import setup, Extension
    from Cython.Build import cythonize

    import os
    import numpy as np  # arrow to remove dependency with new release
    import pyarrow as pa

    ext_modules = [Extension('attributes', ['attributes.pyx'])]

    for ext in ext_modules:
        ext.include_dirs.append(np.get_include())
        ext.include_dirs.append(pa.get_include())
        ext.libraries.extend(pa.get_libraries())
        ext.library_dirs.extend(pa.get_library_dirs())
                    
        # force c++11 usage or bad stuff happens
        if os.name == 'posix':
            ext.extra_compile_args.append('-std=c++11')
                                        
    setup(ext_modules=cythonize(ext_modules),)  # note the comma

Build with `setup.py build_ext --inplace`

In your script, the attributes module can be used

.. code-block:: python
    
    import pyarrow as pa
    import numpy as np
    import attributes

    arr = pa.array(np.arange(100000))
    attributes.get_array_length(arr)
    attributes.get_array_type(arr)

The best approach is when this is combined with C++ such that the pyarrow/Cython interaction acts as a go-between for Python and C++. 
When Python tries to pass a Python Arrow datatype to C++, it has to pass through the pyarrow/Cython layer where it's converted to the corresponding 
C++ type before reaching the actual C++ code. It's exactly like the **Type Conversion** example with str to bytes to C/C++ strings in the :ref:`Basic Cython`, 
except using Arrow objects. Apache Arrow supplies Cython versions of their objects through their pyarrow.includes.common and pyarrow.includes.libarrow libraries. 
They also supply common tools through pyarrow.compat. These are generally useful.

* .pxd: pull the native-C++ objects into Cython, including their c_types. By convention, these are denoted by their original name prefixed with a "C" (Array → CArray). 
* .pyx: define the Python functions and bundle an instance of the corresponding C++ function inside. If necessary, make any modifications to arguments/return values for them to be correctly interpreted by the target language before passing to the C object/passing the return value to Python.
* .py: import only the necessary functions/classes from the .pyx. This is the file the end-user will import.

For a more complete example of extending the Arrow libraries and building bindings, see our `fixed-width file reader <https://github.com/ke-noel/fwfr>`_ project on GitHub.

