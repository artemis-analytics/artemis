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

################
Developers Guide
################

Development environment
=======================

.. code:: bash

  mkdir <workspace>
  cd <workspace>
  git clone https://github.com/artemis-analytics/artemis.git
  git clone https://github.com/artemis-analytics/fwfr.git
  conda env create -f artemis/environment.yaml
  conda activate artemis-dev
  cd fwfr
  ./install.sh --source
  cd ../artemis
  python setup.py build_ext --inplace install
  python -m unittest


Build & Deploy
==============

To build Artemis, cd to the root of the artemis repository. Follow the instructions below.

.. code:: bash

  conda env create -f environment.yml
  conda activate artemis-dev
  git clone "FWFR GIT REPO"
  conda install conda-build
  conda build conda-recipes -c conda-forge
  mv "PATH TO CONDA"/envs/artemis-dev/conda-bld/broken/artemis-"VERSION".tar.bz2 ./
  conda deactivate
  bash release/package.sh -e artemis-dev -n artemis-pack -p artemis-"VERSION" -r "PATH TO ARTEMIS REPO"

This will result in a package called "artemis-pack.tar.gz". You can move this to anywhere you wish to 
deploy.

You can install the created package file with the "deploy.sh" script. 

.. code:: bash

  bash deploy.sh -e "NAME OF CONDA ENV TO CREATE" -n "NAME OF PACKAGE FILE" -p "NAME OF PACKAGE"


Contributing
============
Before committing new branches to GitHub, start a new issue. Several labels are available 
to identify the area of work in the project. 

Use the following naming for branches
<feature_or_bug>-<label>-<issue>


Artemis Release and Tag Management
==================================

During a new Artemis release, the commit that will be released needs to be
tagged with the new version tag, of the format X.Y.Z.
- X is a major version, and should only be incremented when major features are added to Artemis.
- Y is a minor version, it should be incremented when minor features are added to Artemis.
When a new X version is released, Y is returned to 0.
- Z is a fix version, it should be incremented when releases for Artemis are only to fix bugs
or correct small errors. When a new X or Y version is released, Z is returned to 0.

It is important to update the setup.py file with the new Artemis version.


Building the Protobuf
=====================

Requires the protoc compiler from google.

Artemis metadata is defined in io/protobuf/artemis.proto. An important component
of the metadata are histograms. Histograms are provided by the physt package
which includes io functionality to/from protobuf. However, the proto file is
not distributed with the package. This requires building the protobuf with
a copy of the histogram.proto class.

To build (from the io/protobuf directory)

.. code:: bash

  protoc -I=./ --python_out=./ ./artemis.proto
