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
from setuptools import Extension, setup, find_packages
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize

import numpy as np
import os
import pyarrow as pa

def main():
    # Define all Cython extensions here - by default, link to arrow and disable warnings
    make_ext('artemis.tools._filtercoltool', sources='./artemis/tools/_filtercoltool.pyx')
    make_ext('artemis.externals.accumulation_tree.accumulation_tree', sources='./artemis/externals/accumulation_tree/accumulation_tree.pyx')
    # Any non-Cython extensions should be manually added to 'ext_modules'.

    setup(name='artemis',
        version='0.6.0',
        author='Ryan White',
        author_email='ryan.white4@canada.ca',
        packages=find_packages(),
        ext_modules=cythonize(ext_modules),
        cmdclass={'build_ext': build_ext_},
        package_data={
            '':['*.so', '*.pyx', '*.pxd', '*.h']},
        install_requires=[],
        description="Stateful processing framework for administrative data powered by Apache Arrow."
    )

def link_arrow(ext):
    """
    Link the extension to the Arrow C++ library.
    """
    ext.include_dirs.append(np.get_include())  # Arrow is working on removing this dependency
    ext.include_dirs.append(pa.get_include())
    ext.library_dirs.extend(pa.get_library_dirs())
    ext.libraries.extend(pa.get_libraries())

def make_ext(name, sources, include_arrow=True, quiet=True):
    """
    Create extension from name and sources, and add extension to ext_modules list for
    build/installation time.
    
    Parameters
    ----------
    name: str, required
        Where to build extension shared object file, and eventual Python import name.
        ex. 'artemis.tools._mytool'
    sources: str or list of str, required
        Relative path to source .pyx file(s).
        ex. 'artemis/tools/_mytool.pyx'
    include_arrow: bool, optional (default True)
        Whether to include and link to Arrow's C++ libraries.
    quiet: bool, optional (default True)
        Whether to disable Cython warnings at build time.

    Returns
    -------
    setuptools.Extension
        The extension has already been added to the build list, but you can still add
        more options (ex. ext.include_dirs.append('NEW_INCLUDE_DIRS')).
    """
    if not isinstance(sources, list):
        sources = [sources]
    ext = Extension(name=name, sources=sources)
    
    if quiet:
        ext.extra_compile_args.append('-w')
    if include_arrow:
        link_arrow(ext)
    if os.name == 'posix':
        ext.extra_compile_args.append('-std=c++11')
    
    ext_modules.append(ext)
    return ext


class build_ext_(build_ext):
    """ 
    Silence -Wstrict-prototypes warning

    build_ext bug where if $CC is not defined as g++, -Wstrict-prototypes is passed to the 
    compiler. This compiler flag is not supported in C++. The compiler will ignore it, but
    we can remove the warning by manually removing the flag.
    """
    def build_extensions(self):
        if '-Wstrict-prototypes' in self.compiler.compiler_so:
            self.compiler.compiler_so.remove('-Wstrict-prototypes')
        super().build_extensions()

if __name__ == '__main__':
    ext_modules = []  # extensions are passed here by 'make_ext'
    main()
