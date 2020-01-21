###############
Getting Started
###############

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



Running an Artemis Job
======================

An example job is available in examples/distributed_example_2.py which involves extracting dataset
schema from Excel, generating synthetic data, performing data analytics algorithms,and outputs distributions for data profiling.
Ensure that Artemis is built, then, run the following command.

.. code:: bash

  python examples/distribucted_example_2.py --location ./examples/data/example_product.xlsx


The example schema is located in examples/data/example_product.xlsx. To create new dataset schemas
please see instructions in artemis/tools/Excel_template/README.md

