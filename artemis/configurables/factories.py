#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Factory classes for generating instances of common tools
"""
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.io.filehandler import FileHandlerTool
from artemis.generators.filegen import FileGenerator
from artemis.generators.legacygen import GenMF


class GeneratorFactory():

    def __new__(cls, ctype, **kwargs):
        if ctype == 'csv':
            return GenCsvLikeArrow('generator',
                                   nbatches=kwargs['nbatches'],
                                   num_cols=kwargs['num_cols'],
                                   num_rows=kwargs['num_rows'])
        elif ctype == 'legacy':
            columns = {}
            for key in kwargs:
                if 'column' in key:
                    columns[key] = kwargs[key]
            return GenMF('generator',
                         nbatches=kwargs['nbatches'],
                         # num_cols=kwargs['num_cols'],
                         num_rows=kwargs['num_rows'],
                         **columns)
        elif ctype == 'file':
            return FileGenerator('generator',
                                 path=kwargs['path'],
                                 glob=kwargs['glob'],
                                 nbatches=kwargs['nbatches'])
        else:
            print("Unknown type")


class FileHandlerFactory():

    def __new__(cls,
                ctype,
                blocksize=2**16,
                delimiter='\r\n',
                offset_header=None):

        if ctype == 'csv':
            return FileHandlerTool('filehandler',
                                   blocksize=blocksize,
                                   skip_header=True,
                                   delimiter=delimiter)
        elif ctype == 'legacy':
            return FileHandlerTool('filehandler',
                                   blocksize=blocksize,
                                   skip_header=True,
                                   legacy_data=True,
                                   offset_header=offset_header)
        else:
            print('Unknown type')


class MenuFactory():

    def __new__(cls, menu, name='test'):

        if menu == 'csvgen':
            from artemis.configurables.menus.csvgenmenu import CsvGenMenu
            return CsvGenMenu(name)
        elif menu == 'legacygen':
            from artemis.configurables.menus.legacygenmenu import LegacyGenMenu
            return LegacyGenMenu(name)
        else:
            print("Menu not found")
            return None


class JobConfigFactory():

    def __new__(cls, config, menu=None):

        if config == 'csvgen':
            from artemis.configurables.configs.csvgenconfig import CsvGenConfig
            return CsvGenConfig(menu)
        elif config == 'csvio':
            from artemis.configurables.configs.csvgenconfig import CsvIOConfig
            return CsvIOConfig(menu)
        elif config == 'legacygen':
            from artemis.configurables.configs.legacygenconfig \
                import LegacyGenConfig
            return LegacyGenConfig(menu)
        elif config == 'legacyio':
            from artemis.configurables.configs.legacygenconfig \
                import LegacyIOConfig
            return LegacyIOConfig(menu)
        else:
            print("Config not found")
            return None
