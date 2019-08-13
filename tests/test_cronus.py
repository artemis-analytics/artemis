#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Test Class for Artemis MetaStore
"""

import unittest
import logging
import tempfile
import os, shutil
import hashlib
import datetime
import dask.delayed
from pathlib import Path
import pyarrow as pa
import numpy as np

from artemis.core.timerstore import TimerSvc
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.meta.cronus import BaseObjectStore, JobBuilder
from artemis.io.protobuf.cronus_pb2 import CronusStore, CronusObjectStore, CronusObject
from artemis.io.protobuf.cronus_pb2 import DummyMessage, FileObjectInfo, MenuObjectInfo, ConfigObjectInfo, DatasetObjectInfo
from artemis.io.protobuf.menu_pb2 import Menu as Menu_pb
from artemis.io.protobuf.configuration_pb2 import Configuration
import uuid

logging.getLogger().setLevel(logging.INFO)



class CronusTestCase(unittest.TestCase):

    def setUp(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        Singleton.reset(Physt_Wrapper)
        Singleton.reset(TimerSvc)
        pass

    def tearDown(self):
        pass

    def test_menu(self):
        testmenu = Menu_pb() 
        print(type(testmenu))
        print(testmenu)
        testmenu.uuid = str(uuid.uuid4())
        testmenu.name = f"{testmenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(testmenu.SerializeToString())
        

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            menu_uuid = store.register_content(testmenu, menuinfo).uuid
            store.put(menu_uuid, testmenu)
            amenu = Menu_pb()
            store.get(menu_uuid, amenu)
            self.assertEqual(testmenu.name, amenu.name)
            self.assertEqual(testmenu.uuid, amenu.uuid)
    
    def test_config(self):
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())
        

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            config_uuid = store.register_content(myconfig, configinfo).uuid
            store.put(config_uuid, myconfig)
            aconfig = Configuration()
            store.get(config_uuid, aconfig)
            self.assertEqual(myconfig.name, aconfig.name)
            self.assertEqual(myconfig.uuid, aconfig.uuid)

    def test_arrow(self):

        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        mymsg = DummyMessage()
        mymsg.name = "dummy"
        mymsg.description = "really dumb"
        
        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'
            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            print(menu_uuid)
            print(config_uuid)
            dataset = store.register_dataset(menu_uuid, config_uuid)
            store.new_partition(dataset.uuid, 'key')
            job_id = store.new_job(dataset.uuid)
            id_ = store.register_content(buf, fileinfo, dataset_id=dataset.uuid, job_id=0, partition_key='key' ).uuid
            print(store[id_].address)
            store.put(id_, buf) 
            for item in Path(_path).iterdir():
                print(item)

            
            buf = pa.py_buffer(store.get(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)

            reader = store.open(id_)
            self.assertEqual(reader.num_record_batches, 10)
    
    def test_register_object(self):
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        mymsg = DummyMessage()
        mymsg.name = "dummy"
        mymsg.description = "really dumb"

        mymenu = CronusObject()
        mymenu.name = "menu"
        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'

            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            dataset = store.register_dataset(menu_uuid, config_uuid)
            store.new_partition(dataset.uuid, 'key')
            path = dirpath+'/test/dummy.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            id_ = store.register_content(path, fileinfo, dataset_id=dataset.uuid, partition_key='key' ).uuid
            print(store[id_].address)
            buf = pa.py_buffer(store.get(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
    
    def test_identical_files(self):
        print("Testing add file from path")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        mymsg = DummyMessage()
        mymsg.name = "dummy"
        mymsg.description = "really dumb"

        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())
        
        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'

            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            dataset = store.register_dataset(menu_uuid, config_uuid)
            store.new_partition(dataset.uuid, 'key')
            path = dirpath+'/test/dummy.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            id_ = store.register_content(path, fileinfo, dataset_id=dataset.uuid, partition_key='key' ).uuid
            print(id_,store[id_].address)
            buf = pa.py_buffer(store._get_object(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
            
            path = dirpath+'/test/dummy2.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            id_ = store.register_content(path, fileinfo, dataset_id=dataset.uuid, partition_key='key' ).uuid
            print(id_,store[id_].address)
            buf = pa.py_buffer(store.get(id_))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
        print("Test Done ===========================")


    def test_dir_glob(self):
        print("Testing directory globbing")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())
        
        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        mymsg = DummyMessage()
        mymsg.name = "dummy"
        mymsg.description = "really dumb"

        store_id = str(uuid.uuid4())
        mystore = CronusObjectStore()
        mystore.name = 'test'
        mystore.uuid = str(store_id)
        mystore.parent_uuid = '' # top level store

        with tempfile.TemporaryDirectory() as dirpath:
            mystore.address = dirpath+'/test'
            _path = Path(mystore.address)
            _path.mkdir()
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'

            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            dataset = store.register_dataset(menu_uuid, config_uuid)
            store.new_partition(dataset.uuid, 'key')
            path = dirpath+'/test/dummy.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            path = dirpath+'/test/dummy2.arrow'
            with pa.OSFile(str(path),'wb') as f:
                f.write(sink.getvalue())
            
            objs_ = store.register_content(mystore.address, fileinfo, glob='*arrow', dataset_id=dataset.uuid, partition_key='key' )
            for obj_ in objs_:
                print(obj_.uuid,store[obj_.uuid].address)
                buf = pa.py_buffer(store.get(obj_.uuid))
                reader = pa.ipc.open_file(buf)
                self.assertEqual(reader.num_record_batches, 10)

            ds = store.list(suffix='dataset')
            for d in ds:
                p = d.uuid + '.part_key'
                f = store.list(prefix=p, suffix='arrow')
                print(f)
        print("Test Done ===========================")

    def test_register_dataset(self):
        
        #Create a fake dataset
        #from a menu_id and menu msg
        #from a config_id and config msg
        #add files
        #add tables
        
        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())
       
        store_id = str(uuid.uuid4())
        mystore = CronusObjectStore()
        mystore.name = 'test'
        mystore.uuid = str(store_id)
        mystore.parent_uuid = '' # top level store
        
        print("Testing directory globbing")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        #schema = batch.schema.to_pybytes()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        fileinfo = FileObjectInfo()
        fileinfo.type = 5
        fileinfo.aux.num_columns = 3

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test')  
            store_id = store.store_uuid
            print(store.store_info.created.ToDatetime())
            
            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            print(menu_uuid)
            print(config_uuid)
            dataset = store.register_dataset(menu_uuid, config_uuid)
            store.new_partition(dataset.uuid, 'key')
            job_id = store.new_job(dataset.uuid)
            store.register_content(buf, 
                                    fileinfo, 
                                    dataset_id=dataset.uuid, 
                                    partition_key='key',
                                    job_id=job_id)

            ds = store.list(suffix='dataset')
            print(ds)
            
    def test_validation(self):
        print("Simulate production")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        
        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            dataset = store.register_dataset(menu_uuid, config_uuid)
            
            # Multiple streams
            store.new_partition(dataset.uuid, 'key1')
            store.new_partition(dataset.uuid, 'key2')
            store.new_partition(dataset.uuid, 'key3')
            
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'
            
            ids_ = []
            parts = store.list_partitions(dataset.uuid)
            # reload menu and config
            newmenu = Menu_pb()
            store.get(menu_uuid, newmenu)
            newconfig = Configuration()
            store.get(config_uuid, newconfig)
            print(parts)
            for _ in range(10):
                job_id = store.new_job(dataset.uuid)
                
                for key in parts:
                    ids_.append(store.register_content(buf, 
                                                 fileinfo, 
                                                 dataset_id=dataset.uuid, 
                                                 job_id=job_id, 
                                                 partition_key=key ).uuid)
                    store.put(ids_[-1], buf)
            
            for id_ in ids_:
                buf = pa.py_buffer(store.get(id_))
                reader = pa.ipc.open_file(buf)
                self.assertEqual(reader.num_record_batches, 10)
            
            # Save the store, reload
            store.save_store()
            newstore = BaseObjectStore(str(_path), 'test', store_uuid=store.store_uuid) 
            for id_ in ids_:
                print("Get object %s", id_)
                print(type(id_))
                buf = pa.py_buffer(newstore.get(id_))
                reader = pa.ipc.open_file(buf)
                self.assertEqual(reader.num_record_batches, 10)
            print(newmenu)
            print(newconfig)
            print("Simulation Test Done ===========================")
            
    def test_validation(self):
        print("Simulate production")
        data = [
                pa.array([1, 2, 3, 4]),
                pa.array(['foo', 'bar', 'baz', None]),
                pa.array([True, None, False, True])
                ]
        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1', 'f2'])
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)

        for i in range(10):
            writer.write_batch(batch)

        writer.close()
        buf = sink.getvalue()
        
        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())

        with tempfile.TemporaryDirectory() as dirpath:
            _path = dirpath+'/test'
            store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
            # Following puts the menu and config to the datastore
            menu_uuid = store.register_content(mymenu, menuinfo).uuid
            config_uuid = store.register_content(myconfig, configinfo).uuid
            dataset = store.register_dataset(menu_uuid, config_uuid)
            
            # Multiple streams
            store.new_partition(dataset.uuid, 'key1')
            store.new_partition(dataset.uuid, 'key2')
            store.new_partition(dataset.uuid, 'key3')
            
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.aux.description = 'Some dummy data'
            
            ids_ = []
            parts = store.list_partitions(dataset.uuid)
            # reload menu and config
            newmenu = Menu_pb()
            store.get(menu_uuid, newmenu)
            newconfig = Configuration()
            store.get(config_uuid, newconfig)
            print(parts)
            for _ in range(10):
                job_id = store.new_job(dataset.uuid)
                
                for key in parts:
                    ids_.append(store.register_content(buf, 
                                                 fileinfo, 
                                                 dataset_id=dataset.uuid, 
                                                 job_id=job_id, 
                                                 partition_key=key ).uuid)
                    store.put(ids_[-1], buf)
            
            for id_ in ids_:
                buf = pa.py_buffer(store.get(id_))
                reader = pa.ipc.open_file(buf)
                self.assertEqual(reader.num_record_batches, 10)
            
            # Save the store, reload
            store.save_store()
            newstore = BaseObjectStore(str(_path), store._name, store_uuid=store.store_uuid) 
            for id_ in ids_:
                print("Get object %s", id_)
                print(type(id_))
                buf = pa.py_buffer(newstore.get(id_))
                reader = pa.ipc.open_file(buf)
                self.assertEqual(reader.num_record_batches, 10)
            print(newmenu)
            print(newconfig)
            print("Simulation Test Done ===========================")

    def test_distributed(self):


        print("Simulate production")
        
        mymenu = Menu_pb() 
        mymenu.uuid = str(uuid.uuid4())
        mymenu.name = f"{mymenu.uuid}.menu.dat"

        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        bufmenu = pa.py_buffer(mymenu.SerializeToString())
        
        myconfig = Configuration() 
        myconfig.uuid = str(uuid.uuid4())
        myconfig.name = f"{myconfig.uuid}.config.dat"

        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        bufconfig = pa.py_buffer(myconfig.SerializeToString())

        dirpath = tempfile.mkdtemp()
        _path = dirpath+'/test'
        store = BaseObjectStore(str(_path), 'test') # wrapper to the CronusStore message
        # Following puts the menu and config to the datastore
        menu_uuid = store.register_content(mymenu, menuinfo).uuid
        config_uuid = store.register_content(myconfig, configinfo).uuid
        dataset = store.register_dataset(menu_uuid, config_uuid)
        
        # Multiple streams
        store.new_partition(dataset.uuid, 'key1')
        store.new_partition(dataset.uuid, 'key2')
        store.new_partition(dataset.uuid, 'key3')
        
        # Persist the store for access in distributed jobs
        store.save_store()
        
        ds_bufs = []
        for _ in range(10):
            job_id = store.new_job(dataset.uuid)
            
            ds_bufs.append(run_job(_path, 
                    store._name, 
                    store.store_uuid,
                    menu_uuid,
                    config_uuid,
                    dataset.uuid, 
                    job_id))
        results = dask.compute(*ds_bufs,scheduler='single-threaded')
        
        # Update the dataset 
        for buf in results:
            store.update_dataset(dataset.uuid, buf)
        # Save the store, reload
        store.save_store()
        
        # Let's check everything was saved in metastore and datastore
        newstore = BaseObjectStore(str(_path), store._name, store_uuid=store.store_uuid) 
        ids_ = newstore.list(prefix=dataset.uuid, suffix='arrow')
        
        for id_ in ids_:
            print("Get object %s", id_)
            buf = pa.py_buffer(newstore.get(id_.uuid))
            reader = pa.ipc.open_file(buf)
            self.assertEqual(reader.num_record_batches, 10)
        
        # cleanup
        if os.path.exists(dirpath):
            shutil.rmtree(dirpath)


@dask.delayed
def run_job(root, 
            store_name, 
            store_id,
            menu_id,
            config_id,
            dataset_id, 
            job_id):
    
    runner = JobBuilder(root, 
            store_name, 
            store_id,
            menu_id,
            config_id,
            dataset_id, 
            job_id)
    runner.execute()
    msg = runner.buf
    return msg

        
if __name__ == '__main__':
    unittest.main()
