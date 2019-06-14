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
from artemis.generators.simutablegen import SimuTableGen


class GeneratorFactory():

    def __new__(cls, ctype, **kwargs):
        print(ctype, kwargs)
        if ctype == 'csv':
            return GenCsvLikeArrow('generator',
                                   nbatches=kwargs['nbatches'],
                                   # num_cols=kwargs['num_cols'],
                                   table_id=kwargs['table_id'],
                                   num_rows=kwargs['num_rows'],
                                   seed=kwargs['seed'])
        elif ctype == 'legacy':
            columns = {}
            for key in kwargs:
                if 'column' in key:
                    columns[key] = kwargs[key]
            return GenMF('generator',
                         nbatches=kwargs['nbatches'],
                         # num_cols=kwargs['num_cols'],
                         num_rows=kwargs['num_rows'],
                         seed=kwargs['seed'],
                         encoding=kwargs['encoding'],
                         header=kwargs['header'],
                         footer=kwargs['footer'],
                         header_offset=kwargs['header_offset'],
                         footer_size=kwargs['footer_size'],
                         **columns)
        elif ctype == 'simutable':
            print("Config simutable")
            return SimuTableGen('generator',
                                nbatches=kwargs['nbatches'],
                                num_rows=kwargs['num_rows'],
                                file_type=1,
                                table_id=kwargs['table_id'],
                                seed=kwargs['seed'])

        elif ctype == 'file':
            return FileGenerator('generator',
                                 path=kwargs['path'],
                                 glob=kwargs['glob'],
                                 nbatches=kwargs['nbatches'],
                                 seed=kwargs['seed'])
        else:
            raise ValueError


class FileHandlerFactory():

    def __new__(cls,
                ctype,
                blocksize=2**16,
                delimiter=',',
                linesep='\r\n',
                header='',
                footer='',
                header_offset=0,
                footer_size=0,
                schema=[],
                encoding='utf8',
                seed=None):

        return FileHandlerTool('filehandler',
                               filetype=ctype,
                               blocksize=blocksize,
                               delimiter=delimiter,
                               linesep=linesep,
                               header=header,
                               footer=footer,
                               header_offset=header_offset,
                               footer_size=footer_size,
                               schema=schema,
                               encoding=encoding,
                               seed=seed)


class MenuFactory():

    def __new__(cls, menu, name='test'):

        if menu == 'csvgen':
            from artemis.configurables.menus.csvgenmenu import CsvGenMenu
            return CsvGenMenu(name)
        elif menu == 'legacygen':
            from artemis.configurables.menus.legacygenmenu import LegacyGenMenu
            return LegacyGenMenu(name)
        else:
            raise ValueError


class JobConfigFactory():

    def __new__(cls, config, menu=None, **kwargs):

        if config == 'csvgen':
            from artemis.configurables.configs.csvgenconfig import CsvGenConfig
            return CsvGenConfig(menu, **kwargs)
        elif config == 'csvio':
            from artemis.configurables.configs.csvgenconfig import CsvIOConfig
            return CsvIOConfig(menu, **kwargs)
        elif config == 'legacygen':
            from artemis.configurables.configs.legacygenconfig \
                import LegacyGenConfig
            return LegacyGenConfig(menu, **kwargs)
        elif config == 'legacyio':
            from artemis.configurables.configs.legacygenconfig \
                import LegacyIOConfig
            return LegacyIOConfig(menu, **kwargs)
        else:
            raise ValueError
