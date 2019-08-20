# excel-schema-reader
Read user-defined dataset schema from a default excel template and returns a Object containing
* *DatasetObjectInfo*
* list of *Table*

Both are defined in Cronus metadata model

### Dependencies
* Cronus metadata model
* Pandas ExcelFile

### Excel
The template excel spread sheet is available in the template directory.
There's an example spreadsheet in the test directory
###### Instructions
* Fill blank cells with information, leave information blank if not needed
* Each workbook/file should only contain information on one dataset
* First sheet is for dataset metadata, from the second sheet and on, one table per sheet
* Duplicate the template table sheet for extra table
* Duplicate columns for Additional Codeset Info and Additional Metadata Info if needed
* Make sure to enter the metadata name on row 7 (the orange row), metadata without name will be ignored
* Don't need to repeatedly enter variable names, Excel Reader will read all metadata except codeset value/description/lable/additional info from first row of each variable

### Code

```bash
python excel_reader.py --location <path-to-excel-file>
```

or

```bash
python excel_reader.py
>>> Location: <path-to-excel-file>
```
