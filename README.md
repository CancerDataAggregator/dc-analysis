# dc-analysis
This is a collection of codes to do analysis on the data from the DC's currently supported by the CDA. To start we attempt to determine the most populated fields for each entity in each DC. This will help inform the team which fields to focus on bringing into the CDA schema.

## Completed
IDC analysis - analyzes a simple copy of the IDC view as a table
GDC extraction code
Code to be able to clean up data extracted - in jsonl.gz file
There is a repo to generate a BigQuery schema using ALL lines of a data file (must use --keep_nulls option).
pip install bigquery_schema_generator
https://github.com/bxparks/bigquery-schema-generator
GDC data uploaded in 3 tables: gdc_cases_1, gdc_cases_2, gdc_files
## To Do
Need to create a PDC extraction code as well as an analysis code that can handle nested data. Count how many times each record is populated, and how frequently each field in the record is populated.
