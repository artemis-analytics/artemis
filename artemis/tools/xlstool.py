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

from pathlib import Path

# import click
import pandas as pd
import uuid

# from pandas import ExcelFile

from artemis_format.pymodels.cronus_pb2 import DatasetObjectInfo as Dataset
from artemis_format.pymodels.table_pb2 import Table

from artemis.decorators import iterable
from artemis.core.tool import ToolBase
from dolos.simutable.loader import local_providers, faker_providers


class DataSet:
    """
    Helper Class
    """

    def __init__(self, dataset, tables):
        self.dataset = dataset  # Dataset
        self.tables = tables  # List of Tables


@iterable
class XlsToolOptions:
    """
    Class to hold user-defined options
    """

    pass


class XlsTool(ToolBase):
    """
    Read_excel returns a DataSet class which contains a protobuf
    DatasetObjectInfo and a list of protobuf Table model
    """

    def __init__(self, name, **kwargs):

        self.options = dict(XlsToolOptions())
        self.options.update(kwargs)

        super().__init__(name, **self.options)

        if "location" not in self.options:
            self.__logger.error("Missing required path to Excel file")
            raise Exception
        if not (str(self.options["location"]).endswith(".xlsx")):
            self.__logger.error("Specified file is not .xlsx file.")
            raise Exception
        if not (Path(self.options["location"]).is_file()):
            self.__logger.error("Specified file does not exist in given path")
            raise Exception

    def initialize(self):
        pass

    def execute(self, location):
        """
        User input location of excel, calls read_excel

        Parameters
        ----------
        location: path to excel dataset file

        Returns
        ----------
        Dataset instance, contains a DatasetObjectInfo and a Table
        """
        ds = self.read_excel(location)
        return ds

    def read_excel(self, location):
        """
        Read excel from location, calls store_dataset and store_table

        Parameters
        ----------
        location: path to excel dataset file

        Returns
        ---------
        class DataSet which contains a Dataset and a list of Table
        """
        self.__logger.info("Reading " + str(location) + "...")
        try:
            xls = pd.ExcelFile(location)
        except Exception:
            self.__logger.error("Problem converting xlsx to dataframe")
            raise

        sheet_list = xls.sheet_names
        dataset = Dataset()
        tables = list()
        # One workbook should only contain one dataset schema
        is_populated = False
        for sheets in sheet_list:
            if sheets == "Dataset Info":
                if is_populated:
                    self.__logger.error("Workbook contains > 1 dataset")
                    raise Exception
                dataset = self.store_dataset(pd.read_excel(xls, sheets, header=None))
                is_populated = True
            else:
                tables.append(self.store_table(pd.read_excel(xls, sheets, header=None)))
        if not is_populated:
            self.__logger.error("No dataset schema")
            raise Exception

        return DataSet(dataset, tables)

    def store_dataset(self, ds):
        """
        Convert the dataset metadata from template into protobuf object

        Parameters
        ----------
        ds: Excel sheet that is named Dataset Info, usually the first sheet

        Returns
        ---------
        a DatasetObjectInfo instance
        """
        # Turn off warning of chained assignment
        pd.set_option("mode.chained_assignment", None)

        dataset = Dataset()
        data_holding = dataset.aux.data_holding
        data_asset = dataset.aux.data_asset
        dh_detail = data_holding.data_holding_detail
        pa = data_holding.provision_agreement
        dr = data_asset.data_retention

        ds = ds.iloc[
            2:,
        ]  # Drop header
        ds[0].fillna(method="ffill", inplace=True)  # Fill combined cells
        ds[2].fillna("", inplace=True)  # Fill with empty string

        for row in ds.iterrows():
            if row[1][1] in [
                "Dataset Name",
                "Description",
                "Program Element",
                "Has Sensitive Statistical Info? [y/n]",
                "Has Personal Identifiers? [y/n]",
                "Has Other Supporting Documentation? [y/n]",
                "List of Expected Medium",
                "List of Usage",
                "Permission",
                "Provider",
                "Provider Type [External/Internal/Custodian]",
                "Reference Period",
                "Granularity Type",
                "State",
                "Data Asset Category",
                "Retention Description",
                "Retention Period",
                "Retention Trigger Date [dd-mm-yyyy]",
                "Retention Trigger",
                "Retention Type",
                "Reception Frequency",
                "Acquisition Stage",
                "Acquisition Cost [decimal value]",
                "Quality Evaluation Done on Input? [y/n]",
                "Channel Type",
                "List of Statcan Act Sections",
                "Channel Detail",
                "Data Usage Type",
                "Data Acquisition Type",
            ] and row[1][0] in [
                "Data Holding",
                "Data Asset",
                "Data Retention",
                "Data Holding Detail",
                "Provision Agreement",
            ]:
                pass
            else:
                self.__logger.error(
                    str(row[1][1]) + " is not part of Dataset schema, "
                    "please follow the template"
                )
                raise Exception

        try:
            data_holding.name = str(self.mmap(ds, "Dataset Name"))
            data_holding.description = str(
                ds.loc[(ds[1] == "Description") & (ds[0] == "Data Holding"), 2].item()
            )
            data_holding.program_element = str(self.mmap(ds, "Program Element"))
            data_holding.sensitive_statistical_info = self.to_bool(
                self.mmap(ds, "Has Sensitive Statistical " "Info? [y/n]")
            )
            data_holding.has_personal_identifiers = self.to_bool(
                self.mmap(ds, "Has Personal Identifiers? [y/n]")
            )
            data_holding.has_other_supporting_documentation = self.to_bool(
                self.mmap(ds, "Has Other Supporting " "Documentation? [y/n]")
            )
            data_holding.expected_medium.extend(
                list(
                    ds.loc[ds[1] == "List of Expected Medium", 2:]
                    .dropna(how="all", axis=1)
                    .values.flatten()
                )
            )
            data_holding.usage.extend(
                list(
                    ds.loc[ds[1] == "List of Usage", 2:]
                    .dropna(how="all", axis=1)
                    .values.flatten()
                )
            )
            data_holding.permission = str(self.mmap(ds, "Permission"))
            data_holding.provider = str(self.mmap(ds, "Provider"))
            temp = str(
                self.mmap(ds, "Provider Type " "[External/Internal/Custodian]")
            ).lower()

            if temp == "external":
                data_holding.provider_type = 0
            elif temp == "internal":
                data_holding.provider_type = 1
            elif temp == "custodian":
                data_holding.provider_type = 2
        except Exception:
            self.__logger.error("Error storing Data Holding")
            raise

        try:
            data_asset.description = str(
                ds.loc[(ds[1] == "Description") & (ds[0] == "Data Asset"), 2].item()
            )
            data_asset.reference_period = str(self.mmap(ds, "Reference Period"))
            data_asset.granularity_type = str(self.mmap(ds, "Granularity Type"))
            data_asset.state = str(self.mmap(ds, "State"))
            data_asset.creation_time.GetCurrentTime()
            data_asset.data_asset_category = str(self.mmap(ds, "Data Asset Category"))
        except Exception:
            self.__logger.error("Error storing Data Asset")

        try:
            dr.description = str(self.mmap(ds, "Retention Description"))
            dr.period = str(self.mmap(ds, "Retention Period"))
            dr.retention_trigger_date = str(
                self.mmap(ds, "Retention Trigger Date [dd-mm-yyyy]")
            )
            dr.retention_trigger = str(self.mmap(ds, "Retention Trigger"))
            dr.type = str(self.mmap(ds, "Retention Type"))
        except Exception:
            self.__logger.error("Error storing Data Retention")

        try:
            dh_detail.receptionFrequency = str(self.mmap(ds, "Reception Frequency"))
            dh_detail.acquisition_stage = str(self.mmap(ds, "Acquisition Stage"))
            dh_detail.acquisition_cost = float(
                self.mmap(ds, "Acquisition Cost [decimal value]")
            )
            dh_detail.quality_evaluation_done_on_input = self.to_bool(
                self.mmap(ds, "Quality Evaluation " "Done on Input? [y/n]")
            )
        except Exception:
            self.__logger.error("Error storing Data Holding Detail")

        try:
            pa.channel = str(self.mmap(ds, "Channel Type"))
            pa.statcan_act_section.extend(
                list(
                    ds.loc[ds[1] == "List of Statcan Act Sections", 2:]
                    .dropna(how="all", axis=1)
                    .values.flatten()
                )
            )
            pa.channel_detail = str(self.mmap(ds, "Channel Detail"))
            pa.data_usage_type = str(self.mmap(ds, "Data Usage Type"))
            pa.data_acquisition_type = str(self.mmap(ds, "Data Acquisition Type"))
        except Exception:
            self.__logger.error("Error storing Provision Agreement")
        return dataset

    def store_table(self, tb):
        """
        Convert the table metadata from template into protobuf object

        Parameters
        ----------
        tb: Excel sheet that is not named Dataset Info

        Returns
        ---------
        a Table instance
        """
        table = Table()

        if table.uuid == "":
            table.uuid = str(uuid.uuid4())
        else:
            self.__logger_error("Error registering table uuid")
            raise Exception

        schema = table.info.schema

        if schema.uuid == "":
            schema.uuid = str(uuid.uuid4())
        else:
            self.__logger_error("Error registering schema uuid")
            raise Exception

        field = schema.info.fields

        schm = tb.iloc[2:5, 0:2]  # Isolate table level info
        adin = tb.iloc[1:7, 10:]  # Isolate additional metadata
        tb.fillna("", inplace=True)  # Fill with empty string
        meta_name = tb.loc[6]  # Row for metadata names
        tb = tb.iloc[
            7:,
        ]  # Isolate info based on each field

        # generator function list
        genfx = local_providers() + faker_providers()

        if meta_name[0] == "Variable Name":
            var_names = tb.iloc[0:, 0].drop_duplicates()
        else:
            self.__logger.error(
                "First column of the Table Schema " "should be Variable Name."
            )
            raise Exception

        for row in schm.iterrows():
            if row[1][0] in ["Table Name", "Schema Name", "Description"]:
                try:
                    table.name = str(schm.loc[schm[0] == "Table Name", 1].item())
                    schema.name = str(schm.loc[schm[0] == "Schema Name", 1].item())
                    schema.info.aux.description = str(
                        schm.loc[schm[0] == "Description", 1].item()
                    )
                except Exception:
                    self.__logger.error("Error storing Table level metadata")
                    raise
            else:
                self.__logger.error(
                    "%s is not part of Table schema, " "please follow the template",
                    str(row[1][1]),
                )
                raise Exception

        for i in var_names.index:  # All unique variable names in a series
            new = field.add()  # Add repeated message

            if new.uuid == "":
                new.uuid = str(uuid.uuid4())
            else:
                self.__logger_error(
                    "Error registering field uuid, field: " + field.name
                )
                raise Exception

            # Chunk rows for each field
            temp = pd.DataFrame(tb.loc[tb[0] == tb.loc[i, 0]].drop_duplicates())
            # Requires that users fill the first row for each new variable

            try:
                new.name = str(
                    temp.loc[i, meta_name[meta_name == "Variable Name"].index].item()
                )
                new.info.type = str(
                    temp.loc[i, meta_name[meta_name == "Type"].index].item()
                )
                new.info.length = int(
                    temp.loc[i, meta_name[meta_name == "Length"].index].item()
                )
                new.info.nullable = self.to_bool(
                    temp.loc[i, meta_name[meta_name == "Nullable [y/n]"].index].item()
                )
                new.info.aux.description = str(
                    temp.loc[i, meta_name[meta_name == "Description"].index].item()
                )
            except Exception:
                self.__logger.error("Error storing variable on row " + str(i))
                raise

            # Store generator name
            try:
                new.info.aux.generator.name = str(
                    temp.loc[
                        i, meta_name[meta_name == "Synthetic Data Generator Name"].index
                    ].item()
                )
            except Exception:
                self.__logger.error(
                    "Error storing Synthetic Data Generator "
                    + "Name for variable "
                    + new.name
                )
                raise

            if new.name != "record_id":
                if new.info.aux.generator.name == "":
                    self.__logger.error(
                        "Variable "
                        + new.name
                        + " requires Synthetic Data"
                        + " Generator Name"
                    )
                    raise Exception
                if new.info.aux.generator.name.lower() not in genfx:
                    self.__logger.error(
                        "Cannot find generator function "
                        + new.info.aux.generator.name
                        + " for "
                        + new.name
                    )
                    raise Exception

            # Codeset store:
            cs = new.info.aux.codeset
            cs.name = str(
                temp.loc[i, meta_name[meta_name == "Codeset Name"].index].item()
            )
            cs.version = str(
                temp.loc[i, meta_name[meta_name == "Codeset Version"].index].item()
            )

            # Code value store:
            for j in temp.index:  # Each row for variable
                newc = cs.codevalues.add()
                try:
                    newc.code = str(
                        temp.loc[j, meta_name[meta_name == "Code Value"].index].item()
                    )
                    newc.description = str(
                        temp.loc[
                            j, meta_name[meta_name == "Code Description"].index
                        ].item()
                    )
                    newc.lable = str(
                        temp.loc[j, meta_name[meta_name == "Lable"].index].item()
                    )
                except Exception:
                    self.__logger.error("Error storing codeset info")
                    raise Exception
                # Add additional info for codeset values
                for col in adin.iteritems():
                    if (
                        str(col[1][1]) == "Additional Codeset Info"
                        and str(col[1][6]) != ""
                    ):
                        key = str(col[1][6])
                        if key == "":
                            self.__logger.error(
                                "Additional Codeset Info " "missing metadata name"
                            )
                            raise Exception
                        if col[1][2] == "Description":
                            try:
                                newc.others[key].description = str(col[1][3])
                            except Exception:
                                self.__logger.error(
                                    "Error storing additional "
                                    "codeset info description"
                                )
                                raise Exception
                        if col[1][4] == "Type [null/int/string/bool/float]":
                            try:
                                vtype = str(col[1][5]).lower()
                            except Exception:
                                self.__logger.error(
                                    "Error storing additional " "code set info type"
                                )
                                raise Exception
                        else:
                            self.__logger.error(
                                "Additional Codeset " "Info missing type"
                            )
                            raise Exception
                        item = temp.loc[j, col[0]]
                        if vtype == "null":
                            newc.others[key].null_val = 0
                        elif vtype == "int":
                            newc.others[key].int_val = int(item)
                        elif vtype == "string":
                            newc.others[key].string_val = str(item)
                        elif vtype == "bool":
                            newc.others[key].bool_val = self.to_bool(item)
                        elif vtype == "float":
                            newc.others[key].dec_val = float(item)
                        else:
                            self.__logger.error(
                                "Additional Codeset Info "
                                "type is not one of "
                                "[null/int/string/bool/float]"
                            )
                            raise Exception

            # Add addtional info for fields
            for k in adin.iteritems():
                if str(k[1][1]) == "Additional Metadata Info" and str(k[1][6]) != "":
                    key = str(k[1][6])
                    if key == "":
                        self.__logger.error("Additional Metadata Info " "missing name")
                        raise Exception
                    if k[1][2] == "Description":
                        try:
                            new.info.aux.meta[key].description = str(k[1][3])
                        except Exception:
                            self.__logger.error(
                                "Error storing "
                                "additional metadata "
                                "info description"
                            )
                            raise Exception
                    if k[1][4] == "Type [null/int/string/bool/float]":
                        try:
                            mtype = str(k[1][5]).lower()
                        except Exception:
                            self.__logger.error(
                                "Error storing " "additional metadata " "info type"
                            )
                            raise Exception
                    else:
                        self.__logger.error("Additional Metadata " "Info missing type")
                        raise Exception
                    info = temp.loc[i, k[0]]

                    if mtype == "null":
                        new.info.aux.meta[key].null_val = 0
                    elif mtype == "int":
                        new.info.aux.meta[key].int_val = int(info)
                    elif mtype == "string":
                        new.info.aux.meta[key].string_val = str(info)
                    elif mtype == "bool":
                        new.info.aux.meta[key].bool_val = self.to_bool(info)
                    elif mtype == "float":
                        new.info.aux.meta[key].dec_val = float(info)
                    else:
                        self.__logger.error(
                            "Additional Metadata Info "
                            "type is not one of "
                            "[null/int/string/bool/float]"
                        )
                        raise Exception

        return table

    def to_bool(self, cell):
        """
        Convert a cell to boolean, y is true, blank and n are false

        Parameters
        ----------
        cell: a cell value in Excel

        Returns
        ---------
        True/False
        """
        temp = str(cell).lower()
        if temp in ["y", "n", ""]:
            return temp == "y"
        else:
            self.__logger.error("Boolean field contains unexpected value: " + temp)
            raise Exception

    def mmap(self, ds, name):
        """
        Metadata map
        Search name in metadata name column in store_dataset
        If found, returns the corresponding value

        Parameters
        ----------
        ds: Excel sheet that is named Dataset Info, usually the first sheet
        name: Name of metadata

        Returns
        ---------
        value in the corresponding cell
        """
        try:
            return ds.loc[ds[1] == name, 2].item()
        except ValueError:
            self.__logger.error(name + " is not in Dataset Schema.")
        except Exception:
            self.__logger.error("Unknown error in storing Dataset Schema")
