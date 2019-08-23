#! /bin/bash
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.
set -e
set -x
echo $(pwd)
echo $(ls)
cd fwfr;
conda install -y -c conda-forge --file requirements.txt
mkdir -p build && cd build
cmake -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX ..
make install
cd ..
rm -r build
cd bindings
python setup.py build_ext --inplace install
cd ../../
$PYTHON setup.py build_ext --inplace install
