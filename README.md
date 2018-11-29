# Artemis

Artemis -- stateful processing framework for administrative data powered by Apache Arrow™.

# To Build

Build application with ''python setup.py sdist bdist_wheel''.

# Building the protobuf
Artemis metadata is defined in io/protobuf/artemis.proto. An important component
of the metadata are histograms. Histograms are provided by the physt package
which includes io functionality to/from protobuf. However, the proto file is
not distributed with the package. This requires building the protobuf with
a copy of the histogram.proto class. 

To build (from the io/protobuf directory)

''
protoc -I=./ --python_out=./ ./artemis.proto
''

After, modify the python code:

''
import histogram_pb2 as histogram_pb2
''

to

''
import physt.io.protobuf.histogram_pb2 as histogram_pb2
''
