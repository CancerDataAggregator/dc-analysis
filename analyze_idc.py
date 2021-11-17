import pandas as pd
from yaml import Loader
import argparse
import math
from google.cloud import bigquery
from google.oauth2 import service_account
from cdatransform.lib import get_case_ids


class DC_Analyze:
    def __init__(
        self,
        gsa_key="../../GCS-service-account-key.json",
        gsa_info=None,
        patients_file=None,
        patient=None,
        count_dest_table_id="gdc-bq-sample.DC_Analysis.idc_analysis_count",
        values_dest_table_id="gdc-bq-sample.DC_Analysis.idc_analysis_values",
        source_table="gdc-bq-sample.DC_Analysis.idc_v4",
    ) -> None:
        self.gsa_key = gsa_key
        self.gsa_info = gsa_info
        self.count_dest_table_id = count_dest_table_id
        self.values_dest_table_id = values_dest_table_id
        self.source_table = source_table
        self.unique_value_batch_column_size = 5
        self.service_account_cred = self._service_account_cred()
        self.fields = self._init_fields()
        self.patient_ids = get_case_ids(case=patient, case_list_file=patients_file)
        self.count_and_percent_query = self._count_and_percent_query_build()
        #self.unique_value_query = self._unique_value_query_build()

    def _service_account_cred(self):
        key_path = self.gsa_key
        gsa_info = self.gsa_info
        try:
            credentials = service_account.Credentials()
        except Exception:
            if self.gsa_info is not None:
                credentials = service_account.Credentials.from_service_account_info(
                    gsa_info
                )
            else:
                credentials = service_account.Credentials.from_service_account_file(
                    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
        return credentials

    def _init_fields(self):
        credentials = self.service_account_cred
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        table = self.source_table.split('.')[-1]
        sql = "SELECT column_name, data_type FROM `gdc-bq-sample.DC_Analysis.INFORMATION_SCHEMA.COLUMNS` "
        sql += "where table_name = 'idc_v4'"
        # Start the query, passing in the extra configuration.
        results = client.query(sql).result().to_dataframe()
        #results = query_job.result()
        print('init fields results')
        print(self.source_table)
        #print(results)
        return results

    def make_count_percent_table(self):
        credentials = self.service_account_cred
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        sql = self.count_and_percent_query
        query_job = client.query(sql)  # Make an API request.
        df = query_job.result().to_dataframe()
        df = df.transpose()
        df.columns = df.iloc[0]
        df.reset_index(level=0, inplace=True)
        df = df[1:]
        df = df.rename(columns={'index':'field_name'})
        first_column = df.pop('field_name')
        df.insert(0, 'field_name', first_column)
        df = df.sort_values(by='percent', ascending=False)#.to_json('idc_count_percent.jsonl', orient='records',lines=True)
        ## Upload this DataFrame to BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )

        #with open('idc_count_percent.jsonl', "rb") as source_file:
        #    job = client.load_table_from_file(
        #        source_file, self.count_dest_table_id, job_config=job_config
        #    )
        job = client.load_table_from_dataframe(df.sort_values(by='percent', ascending=False), self.count_dest_table_id)
        job.result()  # Waits for the job to complete.
        table = client.get_table(self.count_dest_table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), self.count_dest_table_id
            )
        )

    def make_unique_values_table(self):
        credentials = self.service_account_cred
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        # Loop over batches of columns - BQ cannot handle all columns at once :(
        n = self.unique_value_batch_column_size
        result_df = pd.DataFrame(['unique_values'],columns = ['measurement'])
        for index in range(math.ceil(len(self.fields) / n)):
            index_start = index * n
            index_end = min(index_start + n, len(self.fields['column_name']))
            sql = self._unique_value_query_build(index_start, index_end)
            query_job = client.query(sql)
            temp = query_job.result().to_dataframe()
            result_df = pd.merge(result_df, temp, on='measurement', how='outer')
        result_df = result_df[1:]
        result_df.to_json('idc_unique_values.jsonl', orient='records',lines=True)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        #result_df.pop('measurement')
        #job = client.load_table_from_dataframe(result_df, self.count_dest_table_id)
        #job.result()  # Waits for the job to complete.
        #table = client.get_table(self.count_dest_table_id)  # Make an API request.
        #print(
        #    "Loaded {} rows and {} columns to {}".format(
        #        table.num_rows, len(table.schema), self.count_dest_table_id
        #    )
        #)
        with open('idc_unique_values.jsonl', "rb") as source_file:
            job = client.load_table_from_file(
                source_file, self.values_dest_table_id, job_config=job_config
            )
        job.result()  # Waits for the job to complete.
        table = client.get_table(self.values_dest_table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), self.values_dest_table_id
            )
        )

    def add_counts_of_fields(self):
        entity_string = "'count' as measurement,"
        index = 0
        for index in range(len(self.fields['column_name'])):
            entity_string += "countif(" + self.fields['column_name'].iloc[index] + " is NOT NULL) as " + self.fields['column_name'].iloc[index]
            if index < len(self.fields) - 1:
                entity_string += ", "
            index += 1
        return entity_string

    def add_percent_of_fields(self):
        entity_string = "'percent' AS measurement, "
        index = 0
        for index in range(len(self.fields['column_name'])):
            entity_string += self.fields['column_name'].iloc[index] + "/PatientID*100.0 as " + self.fields['column_name'].iloc[index]
            if index < len(self.fields) - 1:
                entity_string += ", "
            index += 1
        return entity_string

    def build_where_patients(self):
        where = ""
        if self.patient_ids is not None:
            where = """WHERE PatientID in ("""
            where += """','""".join(self.patient_ids) + """')"""
        return where

    def _count_and_percent_query_build(self):
        query = """with t1 as (SELECT """
        query += self.add_counts_of_fields()
        query += """ FROM `""" + self.source_table + """`"""
        query += """), t2 as (SELECT """
        query += self.add_percent_of_fields()
        query += """ FROM t1 ) SELECT * from t1 UNION ALL SELECT * FROM t2 """
        print(query)
        return query

    def _unique_value_query_build(self, index_start, index_end):    
        query = """with """
        for col_index in range(index_start, index_end):
            column = self.fields['column_name'].iloc[col_index]
            query += "t" + str(col_index) + " AS (SELECT 'unique_value' as measurement, "
            if self.fields['data_type'].iloc[col_index] in ['INT', 'INT64', 'FLOAT64', 'INTEGER', 'NUMERIC']:
                query += "array(SELECT DISTINCT IFNULL(" + column + ",0) FROM `"
            elif self.fields['data_type'].iloc[col_index] == 'DATE':
                query += "array(SELECT DISTINCT IFNULL(" + column + ", '1900-01-01') FROM `"
            else:
                query += "array(SELECT DISTINCT IFNULL(" + column + ",'') FROM `"
            query += self.source_table + "`"
            if self.fields['data_type'].iloc[col_index] == 'ARRAY<STRING>':
                query += ", unnest(" + column + ") AS " + column 
            query += " GROUP by " + column
            query += " LIMIT 200) AS " + column + " FROM `"
            query += self.source_table + "` GROUP BY measurement)"
            if col_index < index_end - 1:
                query += ", "
        query += " SELECT t" + str(index_start) + ".measurement, "
        query += ','.join(self.fields['column_name'][index_start:index_end]) + " FROM t" + str(index_start) 
        if index_end-index_start > 1:
            for col_index in range(index_start + 1, index_end):
                query += " join t" + str(col_index) + " on t" + str(index_start) + ".measurement = t" 
                query += str(col_index) + ".measurement "
        print(query)
        return query


def main():
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    parser.add_argument(
        "--count_dest_table_id",
        help="Permanent table destination after querying IDC",
        default="gdc-bq-sample.DC_Analysis.idc_analysis_count"
    )
    parser.add_argument(
        "--values_dest_table_id",
        help="Permanent table destination after querying IDC",
        default="gdc-bq-sample.DC_Analysis.idc_analysis_values"
    )
    parser.add_argument(
        "--source_table",
        help="IDC source table to be queried",
        default="gdc-bq-sample.DC_Analysis.idc_v4",
    )
    parser.add_argument("--gsa_key", help="Location of user GSA key")
    parser.add_argument("--gsa_info", help="json content of GSA key or github.secret")
    parser.add_argument("--patient", help="Extract just this patient", default=None)
    parser.add_argument(
        "--patients",
        help="Optional file with list of patient ids (one to a line)",
        default=None,
    )
    args = parser.parse_args()
    idc = DC_Analyze(
        gsa_key=args.gsa_key,
        gsa_info=args.gsa_info,
        count_dest_table_id=args.count_dest_table_id,
        values_dest_table_id=args.values_dest_table_id,
        patients_file=args.patients,
        patient=args.patient,
        source_table=args.source_table
    )
    idc.make_count_percent_table()
    idc.make_unique_values_table()


if __name__ == "__main__":
    main()