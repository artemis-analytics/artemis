#! /usr/bin/env python
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
# limitations under the License.

"""
Interface to the Artemis Metadata Store
"""
from pathlib import Path
import uuid
import hashlib
import urllib.parse
from dataclasses import dataclass

import pyarrow as pa
import numpy as np
from simplekv.fs import FilesystemStore

from artemis.io.protobuf.cronus_pb2 import CronusObjectStore
from artemis.io.protobuf.cronus_pb2 import CronusObject, FileType
from artemis.io.protobuf.menu_pb2 import Menu as Menu_pb
from artemis.io.protobuf.configuration_pb2 import Configuration
from artemis.logger import Logger
from artemis.core.book import BaseBook

# Import all the info objects to set the oneof of a CronusObject
# Annoying boiler plate
from artemis.io.protobuf.cronus_pb2 import MenuObjectInfo, \
        ConfigObjectInfo, \
        DatasetObjectInfo, \
        HistsObjectInfo, \
        JobObjectInfo, \
        LogObjectInfo, \
        FileObjectInfo, \
        TableObjectInfo, \
        TDigestObjectInfo


@dataclass
class MetaObject:
    '''
    Helper data class for accessing a content object metadata
    The returned class does not give access to the original protobuf
    that is only accesible via uuid (content's hash)
    '''
    name: str
    uuid: str
    parent_uuid: str
    address: str


@Logger.logged
class BaseObjectStore(BaseBook):

    def __init__(self,
                 root,
                 name,
                 store_uuid=None,
                 storetype='hfs',
                 algorithm='sha1',
                 alt_root=None):
        '''
        Loads a base store type
        Requires a root path where the store resides
        Create a store from persisted data
        Or create a new one
        '''
        self._mstore = CronusObjectStore()
        self._dstore = FilesystemStore(f"{root}")
        self._alt_dstore = None
        if alt_root is not None:
            self.__logger.info("Create alternative data store location")
            self._alt_dstore = FilesystemStore(f"{alt_root}")
        self._algorithm = algorithm
        if store_uuid is None:
            # Generate a new store
            self.__logger.info("Generating new metastore")
            self._mstore.uuid = str(uuid.uuid4())
            self._mstore.name = f"{self._mstore.uuid}.{name}.cronus.pb"
            self._mstore.address = self._dstore.url_for(self._mstore.name)
            self._mstore.info.created.GetCurrentTime()
            self.__logger.info("Metastore ID %s", self._mstore.uuid)
            self.__logger.info("Storage location %s", self._mstore.address)
            self.__logger.info("Created on %s",
                               self._mstore.info.created.ToDatetime())
        elif store_uuid is not None:
            self.__logger.info("Load metastore from path")
            self._load_from_path(name, store_uuid)
        else:
            self.__logger.error("Cannot retrieve store: %s from datastore %s",
                                store_uuid, root)
            raise KeyError

        self._name = self._mstore.name
        self._uuid = self._mstore.uuid
        self._parent_uuid = self._mstore.parent_uuid
        self._info = self._mstore.info
        self._aux = self._info.aux

        self._dups = dict()
        self._child_stores = dict()

        objects = dict()

        for item in self._info.objects:
            self.__logger.debug("Loading object %s", item.uuid)
            objects[item.uuid] = item
            if item.WhichOneof('info') == 'dataset':
                for child in item.dataset.files:
                    objects[child.uuid] = child
                for child in item.dataset.hists:
                    objects[child.uuid] = child
                for child in item.dataset.tdigests:
                    objects[child.uuid] = child
                for child in item.dataset.logs:
                    objects[child.uuid] = child
                for child in item.dataset.jobs:
                    objects[child.uuid] = child
                for child in item.dataset.tables:
                    objects[child.uuid] = child

        super().__init__(objects)

    @property
    def store_name(self):
        return self._name

    @property
    def store_uuid(self):
        return self._uuid

    @property
    def store_info(self):
        return self._info

    @property
    def store_aux(self):
        return self._aux

    def _load_from_path(self, name, id_):
        self.__logger.info("Loading from path")
        try:
            buf = self._dstore.get(name)
        except FileNotFoundError:
            self.__logger.error("Metastore data not found")
            raise
        except Exception:
            self.__logger.error("Unknown error")
            raise

        self._mstore.ParseFromString(buf)
        if name != self._mstore.name:
            self.__logger.error("Store name expected: %s received: %s",
                                self._name, name)
            raise ValueError

    def save_store(self):
        buf = self._mstore.SerializeToString()
        self._dstore.put(self._mstore.name, buf)

    def register_content(self,
                         content,
                         info,
                         **kwargs):
        '''
        Returns a dataclass representing the content object
        content is the raw data, e.g. serialized bytestream to be persisted
        hash the bytestream, see for example github.com/dgilland/hashfs

        info object can be used to call the correct
        register method and validate all the required inputs are received

        Metadata model
        --------------

        Menu metadata
            Menu protobuf
        Configuration metadata
            config protobuf
        Dataset metadata
            Partition keys
            Job Ids
            Dataset protobuf
            Log file
            Hists protobuf
            Job protobuf
            Data files
            Table (Schema) protobuf

        Parameters
        ----------
        buf : bytestream, object ready to be persisted
        info : associated metadata object describing the content of buf


        kwargs
        --------
        dataset_id : required for logs, files, tables, hists
        partition_key : required for files and tables
        job_id : job index
        menu_id : uuid of a stored menu
        config_id : uuid of a stored configuration
        glob : pattern for selecting files in an existing directory
        content : pass a serialized blob to compute hash for uuid

        Returns
        -------
        MetaObject dataclass

        '''
        metaobj = None
        dataset_id = kwargs.get('dataset_id', None)
        partition_key = kwargs.get('partition_key', None)
        job_id = kwargs.get('job_id', None)
        #  menu_id = kwargs.get('menu_id', None)
        #  config_id = kwargs.get('config_id', None)
        glob = kwargs.get('glob', None)

        content_type = type(content)
        if kwargs is not None:
            self.__logger.debug("Registering content %s", kwargs)
        if dataset_id is not None:
            self.__logger.debug("%s %s %s", dataset_id, partition_key, job_id)
        if isinstance(info, FileObjectInfo):
            if dataset_id is None:
                self.__logger.error("Registering file requires dataset id")
                raise ValueError
            if partition_key is None:
                self.__logger.error("Registering file requires partition key")
                raise ValueError

            if content_type is str:
                if glob is None:
                    try:
                        metaobj = self._register_file(content,
                                                      info,
                                                      dataset_id,
                                                      partition_key)
                    except Exception:
                        self.__logger.error("Cannot register on-disk file")
                        raise
                else:
                    try:
                        metaobj = self._register_dir(content,
                                                     glob,
                                                     info,
                                                     dataset_id,
                                                     partition_key)
                    except Exception:
                        self.__logger.error("Cannot register files")
                        raise

            else:
                if job_id is None:
                    self.__logger.error("Partition file requires job id")
                    raise ValueError
                try:
                    metaobj = self._register_partition_file(content,
                                                            info,
                                                            dataset_id,
                                                            job_id,
                                                            partition_key)
                except Exception:
                    self.__logger.error("Cannot register partiion file")
                    raise

        elif isinstance(info, MenuObjectInfo):
            try:
                metaobj = self._register_menu(content, info)
            except Exception:
                self.__logger.error("Error registering menu")
                raise

        elif isinstance(info, ConfigObjectInfo):
            try:
                metaobj = self._register_config(content, info)
            except Exception:
                self.__logger.error("Error registering config")
                raise

        elif isinstance(info, DatasetObjectInfo):
            self.__logger.error("Use register_dataset")
            raise TypeError

        elif isinstance(info, HistsObjectInfo):
            if dataset_id is None:
                self.__logger.error("Registering hists requires dataset id")
                raise ValueError
            if job_id is None:
                self.__logger.error("Registering hists requires job id")
                raise ValueError
            try:
                metaobj = self._register_hists(content,
                                               info,
                                               dataset_id,
                                               job_id)
            except Exception:
                self.__logger.error("Error registering hists")

        elif isinstance(info, TDigestObjectInfo):
            if dataset_id is None:
                self.__logger.error("Registering tdigest requires dataset id")
                raise ValueError
            if job_id is None:
                self.__logger.error("Registering tdigest requires job id")
                raise ValueError
            try:
                metaobj = self._register_tdigests(content,
                                                  info,
                                                  dataset_id,
                                                  job_id)
            except Exception:
                self.__logger.error("Error registering tdigest")

        elif isinstance(info, LogObjectInfo):
            self.__logger.error("To register a new log, use register_log")
            raise TypeError

        elif isinstance(info, JobObjectInfo):
            if dataset_id is None:
                self.__logger.error("Registering hists requires dataset id")
                raise ValueError
            if job_id is None:
                self.__logger.error("Registering hists requires job id")
                raise ValueError
            try:
                metaobj = self._register_job(content,
                                             info,
                                             dataset_id,
                                             job_id)
            except Exception:
                self.__logger.error("Error registering hists")

        elif isinstance(info, TableObjectInfo):
            if dataset_id is None:
                self.__logger.error("Registering file requires dataset id")
                raise ValueError
            if job_id is None:
                self.__logger.error("Registering file requires job id")
                raise ValueError
            if partition_key is None:
                self.__logger.error("Registering file requires partition key")
                raise ValueError
            metaobj = self._register_partition_table(content,
                                                     info,
                                                     dataset_id,
                                                     job_id,
                                                     partition_key)
        else:
            self.__logger.error("Unknown info object")
            raise ValueError
        return metaobj

    def register_dataset(self, menu_id=None, config_id=None):
        '''
        dataset creation
        occurs before persisting storing information
        works as a datasink
        Datasets are not a persisted object in the datastore

        Parameters
        ----------
        menu_id : uuid of a stored menu
        config_id : uuid of a stored configuration

        Returns
        -------
        MetaObject dataclass describing the dataset content object
        '''
        self.__logger.debug("Register new dataset")
        obj = self._mstore.info.objects.add()
        obj.uuid = str(uuid.uuid4())  # Register new datsets with UUID4
        obj.parent_uuid = self._uuid
        obj.name = f"{obj.uuid}.dataset"

        # Set the transform objects, assumes menu, config and datasets
        # all reside in one store
        if menu_id is not None:
            obj.dataset.transform.menu.CopyFrom(self[menu_id])
        if config_id is not None:
            obj.dataset.transform.config.CopyFrom(self[config_id])
        obj.address = self._dstore.url_for(obj.name)
        self[obj.uuid] = obj
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def register_log(self, dataset_id, job_id):
        '''
        log file content


        Parameters
        ----------
        dataset_id : uuid of a dataset
        job_id : index of job for this log

        Returns
        -------
        MetaObject dataclass describing the log content object
        '''
        self.__logger.debug("Register new log")
        obj = self[dataset_id].dataset.logs.add()
        obj.uuid = str(uuid.uuid4())
        obj.name = f"{dataset_id}.job_{job_id}.{obj.uuid}.log"
        obj.parent_uuid = dataset_id
        obj.address = self._dstore.url_for(obj.name)
        self[obj.uuid] = obj
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def update_dataset(self, dataset_id, buf):
        '''
        '''
        _update = DatasetObjectInfo()
        _update.ParseFromString(buf)

        parts = self[dataset_id].dataset.partitions

        if parts != _update.partitions:
            self.__logger.error("Paritions not equal")
            self.__logger.error("Dataset %s", dataset_id)
            self.__logger.error("Expected: %s", parts)
            self.__logger.error(_update.partitions)
        objs = []
        for obj in _update.jobs:
            _new = self[dataset_id].dataset.jobs.add()
            _new.CopyFrom(obj)
            self[_new.uuid] = _new
            objs.append(MetaObject(_new.name, _new.uuid,
                                   _new.parent_uuid, _new.address))
        for obj in _update.hists:
            _new = self[dataset_id].dataset.hists.add()
            _new.CopyFrom(obj)
            self[_new.uuid] = _new
            objs.append(MetaObject(_new.name, _new.uuid,
                                   _new.parent_uuid, _new.address))

        for obj in _update.tdigests:
            _new = self[dataset_id].dataset.tdigests.add()
            _new.CopyFrom(obj)
            self[_new.uuid] = _new
            objs.append(MetaObject(_new.name, _new.uuid,
                                   _new.parent_uuid, _new.address))
        for obj in _update.files:
            _new = self[dataset_id].dataset.files.add()
            _new.CopyFrom(obj)
            self[_new.uuid] = _new
            objs.append(MetaObject(_new.name, _new.uuid,
                                   _new.parent_uuid, _new.address))
        for obj in _update.logs:
            _new = self[dataset_id].dataset.logs.add()
            _new.CopyFrom(obj)
            self[_new.uuid] = _new
            objs.append(MetaObject(_new.name, _new.uuid,
                                   _new.parent_uuid, _new.address))
        for obj in _update.tables:
            _new = self[dataset_id].dataset.logs.add()
            _new.CopyFrom(obj)
            self[_new.uuid] = _new
            objs.append(MetaObject(_new.name, _new.uuid,
                                   _new.parent_uuid, _new.address))

    def new_job(self, dataset_id):
        '''
        Increment job counter of a dataset

        Parameters
        ----------
        dataset_id : uuid of a registered dataset
        '''
        job_idx = self[dataset_id].dataset.job_idx
        self[dataset_id].dataset.job_idx += 1
        return job_idx

    def new_partition(self, dataset_id, partition_key):
        '''
        Add a partition key to a dataset
        Artemis datastreams are associated to partitions via the graph leaf

        Parameters
        ----------
        dataset_id : uuid of dataset
        partition_key : Leaf node name of menu

        Returns
        -------

        '''
        self[dataset_id].dataset.partitions.append(partition_key)

    def put(self, id_, content):
        '''
        Writes data to kv store
        Support for:
            - data wrapped as a pyarrow Buffer
            - protocol buffer message

        Parameters
        ----------
        id_ : uuid of object
        content : pyarrow Buffer or protobuf msg

        Returns
        ----------
        '''
        if type(content) is pa.lib.Buffer:
            try:
                self._put_object(id_, content)
            except Exception:
                raise
        else:
            try:
                self._put_message(id_, content)
            except Exception:
                raise

    def get(self, id_, msg=None):
        '''
        Retrieves data from kv store
        Support for:
            - pyarrow ipc file or stream
            - pyarrow input_stream, e.g. csv, fwf, ...
            - bytestream protobuf message

        Parameters
        ----------
        id_ : uuid of content
        msg : protobuf message to be parsed into

        Returns
        ---------
        In-memory buffer of data
        Deserialized protobuf message in python class instance

        Note:
            User must know protobuf message class to deserialize
        '''

        if msg is None:
            return self._get_object(id_)
        else:
            self._get_message(id_, msg)

    def open(self, id_):
        '''
        Open a stream for reading
        Enables chunking of data
        Relies on the metaobject to determine how to read the file

        Parameters
        ----------
        id_ : uuid of object to open in kv store

        Returns
        ----------
        pyarrow IO handler
        '''
        # Returns pyarrow io handle
        if self[id_].WhichOneof('info') == 'file':
            # Arrow RecordBatchFile
            if self[id_].file.type == 5:
                # Convert the url to path
                return self._open_ipc_file(id_)
            # Arrow RecordBatchStream
            elif self[id_].file.type == 6:
                return self._open_ipc_stream(id_)
            else:
                return self._open_stream(id_)
        else:
            # Anything else in the store is either a protobuf bytestream
            # or just text, e.g. a log file
            # Need to handle compressed files
            return self._open_stream(id_)

    def list(self, prefix=u"", suffix=u""):
        objs = []
        for id_ in self.keys():
            if self[id_].name.startswith(prefix) \
                    and self[id_].name.endswith(suffix):
                self.__logger.debug(self[id_].name)
                objs.append(MetaObject(self[id_].name,
                                       self[id_].uuid,
                                       self[id_].parent_uuid,
                                       self[id_].address))
        return objs

    def list_partitions(self, dataset_id):
        return self[dataset_id].dataset.partitions

    def list_jobs(self, dataset_id):
        return self[dataset_id].dataset.jobs

    def list_tdigests(self, dataset_id):
        return self[dataset_id].dataset.tdigests
    
    def list_histograms(self, dataset_id):
        return self[dataset_id].dataset.hists

    def _compute_hash(self, stream):
        hashobj = hashlib.new(self._algorithm)
        hashobj.update(stream.read())
        return hashobj.hexdigest()

    def _register_menu(self, menu, menuinfo):
        self.__logger.info("Registering menu object")

        obj = self._mstore.info.objects.add()

        # obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.uuid = menu.uuid
        obj.parent_uuid = self._uuid
        obj.name = menu.name
        # New data, get a url from the datastore
        obj.address = self._dstore.url_for(obj.name)
        self.__logger.info("Retrieving url %s", obj.address)
        self.__logger.info("obj name %s", obj.name)
        # Copy the info object
        obj.menu.CopyFrom(menuinfo)
        self[obj.uuid] = obj
        self._put_message(obj.uuid, menu)
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_config(self, config, configinfo):
        '''
        Takes a config protbuf bytestream
        '''
        self.__logger.info("Registering config object")

        obj = self._mstore.info.objects.add()

        obj.uuid = config.uuid
        obj.parent_uuid = self._uuid
        obj.name = config.name
        # New data, get a url from the datastore
        obj.address = self._dstore.url_for(obj.name)
        self.__logger.info("Retrieving url %s", obj.address)
        self.__logger.info("obj name %s", obj.name)

        # Copy the info object
        obj.config.CopyFrom(configinfo)
        self[obj.uuid] = obj
        self._put_message(obj.uuid, config)
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_partition_table(self,
                                  table,
                                  tableinfo,
                                  dataset_id,
                                  job_id,
                                  partition_key,
                                  file_id=None):
        '''
        dataset uuid
        job key
        partition key
        file uuid -- optional for tables
            extracted from an input file
            or an output RecordBatchFile
        '''
        self.__logger.debug("Registering table Dataset %s, Partition %s",
                            dataset_id, partition_key)
        if partition_key not in self[dataset_id].dataset.partitions:
            self.__logger.error("Partition %s not registered for dataset %s",
                                dataset_id,
                                partition_key)
            raise ValueError

        obj = self[dataset_id].dataset.tables.add()
        # obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.uuid = table.uuid
        obj.name = table.name
        obj.parent_uuid = dataset_id
        obj.address = self._dstore.url_for(obj.name)
        self.__logger.debug("Retrieving url %s", obj.address)
        obj.table.CopyFrom(tableinfo)
        self[obj.uuid] = obj
        self._put_message(obj.uuid, table)
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_partition_file(self,
                                 buf,
                                 fileinfo,
                                 dataset_id,
                                 job_id,
                                 partition_key):
        '''
        Requires
        dataset uuid
        partition key
        job key
        file uuid
        '''
        self.__logger.debug("Registering file")
        self.__logger.debug("Dataset: %s, Partition: %s",
                            dataset_id,
                            partition_key)
        if partition_key not in self[dataset_id].dataset.partitions:
            self.__logger.error("Partition %s not registered for dataset %s",
                                dataset_id,
                                partition_key)
            raise ValueError

        key = str(FileType.Name(fileinfo.type)).lower()
        obj = self[dataset_id].dataset.files.add()
        # obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.uuid = str(uuid.uuid4())

        if obj.uuid in self:
            if obj.uuid in self._dups:
                self._dups[obj.uuid] += 1
            else:
                self._dups[obj.uuid] = 0
            obj.uuid = obj.uuid + '_' + str(self._dups[obj.uuid])

        obj.name = \
            f"{dataset_id}.job_{job_id}.part_{partition_key}.{obj.uuid}.{key}"
        obj.parent_uuid = dataset_id
        obj.address = self._dstore.url_for(obj.name)
        self.__logger.debug("Retrieving url %s", obj.address)
        obj.file.CopyFrom(fileinfo)

        self[obj.uuid] = obj

        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_hists(self,
                        hists,
                        histsinfo,
                        dataset_id,
                        job_id):
        '''
        Requires
        uuid of dataset
        generate a hists uuid from buffer
        job key common to all jobs in a dataset
        keep an running index of hists?
        extension hists.data
        dataset_id.job_name.hists_id.dat
        '''
        self.__logger.debug("Register histogram")
        obj = self[dataset_id].dataset.hists.add()
        # obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.uuid = str(uuid.uuid4())
        obj.parent_uuid = dataset_id
        obj.name = f"{dataset_id}.job_{job_id}.{obj.uuid}.hist.pb"
        obj.address = self._dstore.url_for(obj.name)
        obj.hists.CopyFrom(histsinfo)
        self[obj.uuid] = obj
        self._put_message(obj.uuid, hists)
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_tdigests(self,
                           tdigests,
                           tdigestinfo,
                           dataset_id,
                           job_id):
        '''
        Requires
        uuid of dataset
        generate a hists uuid from buffer
        job key common to all jobs in a dataset
        keep an running index of hists?
        extension hists.data
        dataset_id.job_name.hists_id.dat
        '''
        self.__logger.debug("Register histogram")
        obj = self[dataset_id].dataset.tdigests.add()
        # obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.uuid = str(uuid.uuid4())
        obj.parent_uuid = dataset_id
        obj.name = f"{dataset_id}.job_{job_id}.{obj.uuid}.tdigest.pb"
        obj.address = self._dstore.url_for(obj.name)
        obj.tdigests.CopyFrom(tdigestinfo)
        self[obj.uuid] = obj
        self._put_message(obj.uuid, tdigests)

        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_job(self,
                      meta,
                      jobinfo,
                      dataset_id,
                      job_id):
        '''
        Requires
        uuid of dataset
        generate a hists uuid from buffer
        job key common to all jobs in a dataset
        keep an running index of hists?
        extension hists.data
        dataset_id.job_name.hists_id.dat
        '''
        self.__logger.debug("Register job")
        obj = self[dataset_id].dataset.jobs.add()
        # obj.uuid = self._compute_hash(pa.input_stream(buf))
        obj.uuid = str(uuid.uuid4())
        obj.parent_uuid = dataset_id
        obj.name = f"{dataset_id}.job_{job_id}.{obj.uuid}.job.pb"
        obj.address = self._dstore.url_for(obj.name)
        obj.job.CopyFrom(jobinfo)
        self[obj.uuid] = obj
        self._put_message(obj.uuid, meta)

        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_file(self,
                       location,
                       fileinfo,
                       dataset_id,
                       partition_key):
        '''
        Returns the content identifier
        for a file that is already in a store
        Requires a stream as bytes
        '''
        self.__logger.debug("Registering on disk file %s", location)
        path = Path(location)
        if path.is_absolute() is False:
            path = path.resolve()
        obj = self[dataset_id].dataset.files.add()
        obj.uuid = self._compute_hash(pa.input_stream(str(path)))
        obj.name = f"{dataset_id}.part_{partition_key}.{obj.uuid}.{path.name}"
        obj.parent_uuid = dataset_id
        # Create a Path object, ensure that location points to a file
        # Since we are using simplekv, new objects always registers as url
        # So make a file path as url
        obj.address = path.as_uri()
        obj.file.CopyFrom(fileinfo)

        if obj.uuid in self:
            if obj.uuid in self._dups:
                self._dups[obj.uuid] += 1
            else:
                self._dups[obj.uuid] = 0
            obj.uuid = obj.uuid + '_' + str(self._dups[obj.uuid])

        self[obj.uuid] = obj
        return MetaObject(obj.name, obj.uuid, obj.parent_uuid, obj.address)

    def _register_dir(self,
                      location,
                      glob,
                      fileinfo,
                      dataset_id,
                      partition_key):
        '''
        Registers a directory of files in a store
        '''
        objs = []
        for file_ in Path(location).glob(glob):
            objs.append(self._register_file(file_,
                                            fileinfo,
                                            dataset_id,
                                            partition_key))
        return objs

    def __setitem__(self, id_, msg):
        '''
        book[key] = value
        enfore immutible store
        '''
        if id_ in self:
            self.__logger.error("Key exists %s", id_)
            raise ValueError
        if not isinstance(id_, str):
            raise TypeError
        if not isinstance(msg, CronusObject):
            raise TypeError

        self._set(id_, msg)

    def _put_message(self, id_, msg):
        # proto message to persist
        self.__logger.debug("Putting message to datastore %s",
                            self[id_].address)
        try:
            self._dstore.put(self[id_].name, msg.SerializeToString())
        except IOError:
            self.__logger.error("IO error %s", self[id_].address)
            raise
        except Exception:
            self.__logger.error("Unknown error put %s", self[id_].address)
            raise

    def _get_message(self, id_, msg):
        # get object will read object into memory buffer
        try:
            buf = self._dstore.get(self[id_].name)
            msg.ParseFromString(buf)
        except KeyError:
            self.__logger.error("Message not found in store %s",
                                self[id_].address)
            raise

    def _put_object(self, id_, buf):
        # bytestream to persist
        self.__logger.debug("Putting buf to datastore %s", self[id_].address)
        try:
            self._dstore.put(self[id_].name, buf.to_pybytes())
        except IOError:
            self.__logger.error("IO error %s", self[id_].address)
            raise
        except Exception:
            self.__logger.error("Unknown error put %s", self[id_].address)
            raise

    def _get_object(self, id_):
        # get object will read object into memory buffer
        self.__logger.debug(self[id_])
        try:
            buf = self._dstore.get(self[id_].name)
        except KeyError:
            self.__logger.warning("Key not in store, try local %s", self[id_])
            # File resides outside of kv store
            # Used for registering files already existing in persistent storage
            buf = pa.input_stream(self._parse_url(id_)).read()
        except Exception:
            self.__logger.error("Key not in store, try local %s", self[id_])
        return buf

    def _parse_url(self, id_):
        url_data = urllib.parse.urlparse(self[id_].address)
        return urllib.parse.unquote(url_data.path)

    def _open_ipc_file(self, id_):
        path = self._parse_url(id_)
        try:
            stream = pa.ipc.open_file(path)
        except IOError:
            self.__logger.error("Unable to open ipc message %s", path)
            raise
        except Exception:
            self.__logger.error("Unknown error opening ipc message %s", path)
            raise
        return stream

    def _open_ipc_stream(self, id_):
        path = self._parse_url(id_)
        try:
            stream = pa.ipc.open_stream(path)
        except IOError:
            self.__logger.error("Unable to open ipc message %s", path)
            raise
        except Exception:
            self.__logger.error("Unknown error opening ipc message %s", path)
            raise
        return stream

    def _open_stream(self, id_):
        path = self._parse_url(id_)
        try:
            stream = pa.input_stream(path)
        except IOError:
            self.__logger.error("Unable to open stream %s", path)
            raise
        except Exception:
            self.__logger.error("Unknown error opening stream %s", path)
            raise
        return stream


@Logger.logged
class JobBuilder():
    '''
    Class the simulate functionality of Artemis
    '''
    def __init__(self,
                 root,
                 store_name,
                 store_id,
                 menu_id,
                 config_id,
                 dataset_id,
                 job_id):

        self.dataset_id = dataset_id
        self.job_id = job_id

        # Connect to the metastore
        # Setup a datastore
        self.store = BaseObjectStore(str(root),
                                     store_name,
                                     store_uuid=store_id)

        self.parts = self.store.list_partitions(dataset_id)
        self.menu = Menu_pb()
        self.store.get(menu_id, self.menu)
        self.config = Configuration()
        # Get the menu and config to run the job
        self.store.get(config_id, self.config)

        self.buf = None

    def execute(self):
        '''
        Execute simulates creating data
        creating associating metaobject
        storing data and metadata

        returns a serialized dataset object for updating
        a final store
        '''
        self.__logger.info("Running job %s", self.job_id)
        data = [
                pa.array(np.random.rand(100000,)),
                pa.array(np.random.rand(100000,)),
                pa.array(np.random.rand(100000,)),
                pa.array(np.random.rand(100000,)),
                pa.array(np.random.rand(100000,)),
                pa.array(np.random.rand(100000,)),
                ]
        batch = pa.RecordBatch.from_arrays(data,
                                           ['f0', 'f1', 'f2',
                                            'f3', 'f4', 'f5'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()

        fileinfo = FileObjectInfo()
        fileinfo.type = 5
        fileinfo.aux.description = 'Some dummy data'

        ids_ = []
        for key in self.parts:
            ids_.append(self.store.register_content(buf,
                                                    fileinfo,
                                                    dataset_id=self.dataset_id,
                                                    job_id=self.job_id,
                                                    partition_key=key).uuid)
            self.store.put(ids_[-1], buf)
        buf_ds = self.store[self.dataset_id].dataset.SerializeToString()
        self.buf = buf_ds
