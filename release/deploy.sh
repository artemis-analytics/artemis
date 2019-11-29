#! /bin/bash
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache LIcense, Version 2.0 (the "License");
# you may not use this file except in compliance with the Licesnse.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Liccense is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitation under the License.

# Argument passing.
POSITIONAL=()
while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -e|--environment) # Name of conda environment to create.
            CONENV="${2}"
            shift
            shift
            ;;
        -n|--name) # Name of compressed file.
            NAME="${2}"
            shift
            shift
            ;;
        -p|--package) # Name of package.
            CONDAPACK="${2}"
            shift
            shift
            ;;
esac
done
set -- "${POSITIONAL[@]}"

# Help option to explain program.

if [[ $1 == 'help' || $1 == '-h' || $1 == '--help' ]]
then
    echo "Usage:"
    echo "deploy.sh -e name_of_conda_environment_to_create -n name_of_compressed_file -p packagename -l vm_server"
    exit 0
fi

CWD=$(pwd)
tar -xzvf $NAME.tar.gz
conda create --offline -y -n $CONENV python=3.7
eval "$(conda shell.bash hook)" # Allows the use of all of Anaconda
conda activate $CONENV
conda deactivate
pushd ./$NAME/conda/pkgs/linux-64/ # Conda install doesn't work from other directories, not sure why.
conda install -y -c ./ --override-channels -n $CONENV $CONDAPACK
popd
