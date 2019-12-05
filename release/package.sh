#! /bin/bash
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
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
# limitation under the License.

# Argument passing.
POSITIONAL=()
while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -e|--environment) # Conda environment that needs to be bundled.
            CONENV="${2}"
            shift
            shift
            ;;
        -n|--name) # Name of compressed file.
            NAME="${2}"
            shift
            shift
            ;;
        -p|--package) # Conda built package.
            CONDAPACK="${2}"
            shift
            shift
            ;;
        -r|--repo|--repository) # Path to local copy of repo.
            REPO="${2}"
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
    echo "package.sh -e conda-virtual-environment-to-be-bundled -n compressed_file_name"
    exit 0
fi

DIR=../$NAME # Create work environment, to be tar'ed later.
mkdir $DIR
mkdir $DIR/pypi
mkdir $DIR/pypi/list
mkdir $DIR/pypi/pkgs
mkdir $DIR/conda
mkdir $DIR/conda/list
mkdir $DIR/conda/pkgs
mkdir $DIR/conda/pkgs/linux-64 # Only platform currently supported is Linux-64.

conda list -n $CONENV > $DIR/env_list.tmp # Create list of packages.
tail -n+4 $DIR/env_list.tmp > $DIR/env_list # Remove extra lines.

sed -i 's/\s\s*/ /g' $DIR/env_list # sed removes extra spaces.
# AWK splits packages according to their channels/repos.
awk -v outdir="$DIR" '{
if($4 == "pypi")
    PYPI[$1"=="$2];
else
    PACKAGE[$1 "-" $2 "-" $3".tar.bz2"]=$4;
}
END{
for (var in PYPI)
    print var >> outdir"/pypi/list/requirements.txt"

for (var in PACKAGE)
    if (PACKAGE[var]=="")
        print var >> outdir"/conda/list/anaconda.txt"
    else
        print var >> outdir"/conda/list/"PACKAGE[var]".txt"
}
' $DIR/env_list
# Copy needed packages from conda pkg directory to CWD.
FILES=$DIR/conda/list/*
for f in $FILES
do
    if [[ $f == "." || $f == ".." ]]; then
        cd .
    else
        echo "Processing $f file..."
        rsync -a ~/miniconda3/pkgs/ --files-from=$f $DIR/conda/pkgs/linux-64
    fi
done
source ~/miniconda3/etc/profile.d/conda.sh
eval "$(conda shell.bash hook)" # Allows the use of Anaconda.
conda activate $CONENV
#pip download -vvv -d $DIR/pypi/pkgs -r $DIR/pypi/list/requirements.txt
conda deactivate
TEMPCONENV="tmp_con_env_for_index"
conda create -y -n $TEMPCONENV python=3.7 conda-build # Create a temp environment to index packages.
conda activate $TEMPCONENV
cp $CONDAPACK*.tar.bz2 $DIR/conda/pkgs/linux-64/
conda index $DIR/conda/pkgs/
conda deactivate
conda remove -y --name $TEMPCONENV --all # Clean up working environment.
cp -r $REPO $DIR
#rsync -avr --exclude=./$DIR "$REPO/" $DIR/artemis
tar -cvf $NAME.tar $DIR
gzip -c $NAME.tar > $NAME.tar.gz

rm -rf $DIR $NAME.tar # Clean up working environment.
