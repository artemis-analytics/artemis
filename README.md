# Artemis

Artemis -- stateful processing framework for administrative data powered by Apache Arrow™.

# Build & Deploy

To build Artemis, simply call the shell script package.sh with the argument -d to create a directory to dump the resulting archive file to.

Example: '''bash package.sh -d test_build'''

Artemis deployment is handled via setup.sh. You must supply the version (with the format artemis-x.y.z) and the type of installation, cli for source code access or package for interpreter/import only access.

Example: '''bash setup.sh -t cli -v artemis-0.0.1'''

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
