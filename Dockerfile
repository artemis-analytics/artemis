# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM registry.k8s.cloud.statcan.ca/stcdatascience/dockers/miniconda-python3.7:latest

MAINTAINER Dominic Parent <dominic.parent@canada.ca>

ARG CI_JOB_TOKEN=0
RUN git clone https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.k8s.cloud.statcan.ca/stcdatascience/cronus.git
RUN pip install --upgrade pip
RUN pip install setuptools cython packaging protobuf matplotlib
ADD cronus ./cronus
RUN cd cronus; python setup.py install

ADD artemis ./artemis
ADD tests ./tests
ADD setup.py ./setup.py

RUN python setup.py install
