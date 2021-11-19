# dc-analysis
This is a collection of codes to do analysis on the data from the DC's currently supported by the CDA. To start we attempt to determine the most populated fields for each entity in each DC. This will help inform the team which fields to focus on bringing into the CDA schema.

## Completed
IDC analysis - analyzes a simple copy of the IDC view as a table
GDC extraction code
Code to be able to clean up data extracted - in jsonl.gz file
## To Do
BigQuery has some issues auto-detecting sparsely populated fields. This is not good and now we need to find a way to extract a JSON schema and transform it into a BQ schema to uploaded with the extracted data. Thankfully we are part of the way there with the JSON schema program used in the load portion of the transform repo. Need to write code to extract the schema from the GDC API and write each entity to a json file. Also need to alter the JSON schema program to read from a list of fields instead of a mapping file.
