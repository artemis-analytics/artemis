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
