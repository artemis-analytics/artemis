FROM centos/python-36-centos7:latest

MAINTAINER Dominic Parent <dominic.parent@canada.ca>

RUN pip install --upgrade pip
RUN pip install setuptools numpy pandas scipy cython pyarrow packaging protobuf matplotlib

ADD artemis ./artemis
ADD tests ./tests
ADD setup.py ./setup.py

RUN python setup.py install
RUN python -m unittest
