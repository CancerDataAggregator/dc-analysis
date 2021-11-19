import jsonlines
import gzip
import sys
import time
import argparse

from cdatransform.extract.lib import retry_get


def clean_fields(hit):
    if hit.get("age_at_diagnosis") is not None:
        hit["age_at_diagnosis"] = int(hit.get("age_at_diagnosis"))
    if hit.get("diagnoses") is not None:
        if isinstance(hit["diagnoses"],list):
            for iter in range(len(hit["diagnoses"])):
                if hit["diagnoses"][iter].get("days_to_last_follow_up") is not None:
                    hit["diagnoses"][iter]["days_to_last_follow_up"] = int(hit["diagnoses"][iter]["days_to_last_follow_up"])
        else:
            if hit["diagnoses"].get("days_to_last_follow_up") is not None:
                hit["diagnoses"]["days_to_last_follow_up"] = int(hit["diagnoses"]["days_to_last_follow_up"])
    if hit.get("source_center") == 'None':
        hit["source_center"] = 9999
    return hit


def get_total_number(endpoint):
    params = {"format": "json"}
    result = retry_get(endpoint, params=params)
    return result.json()["data"]["pagination"]["total"]

def case_out_file_names(file_name,index):
    new_name = file_name.split('.')
    new_name.insert(1,str(index))
    return '.'.join(new_name)

class GDC:
    def __init__(
        self,
        cases_endpoint="https://api.gdc.cancer.gov/v0/cases",
        files_endpoint="https://api.gdc.cancer.gov/files",
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint


    def save_entries(self, out_file, endpt, fields,  page_size=500):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            if endpt == 'case':
                for case in self._cases(fields, page_size):
                    writer.write(case)
                    n += 1
                    if n % page_size == 0:
                        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
            sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

            if endpt == 'file':
                for file in self._files(fields, page_size):
                    writer.write(file)
                    n += 1
                    if n % page_size == 0:
                        sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
            sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")


    def _cases(
        self,
        fields,
        page_size=10,
    ):
        fields = ",".join(fields)
        filt = None
        offset = 0
        while True:
            params = {
                "filters": filt,
                "format": "json",
                "fields": fields,
                "size": page_size,
                "from": offset,
            }

            # How to handle errors
            result = retry_get(self.cases_endpoint, params=params)
            hits = result.json()["data"]["hits"]
            page = result.json()["data"]["pagination"]
            p_no = page.get("page")
            p_tot = page.get("pages")

            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")

            for hit in hits:
                yield clean_fields(hit)

            if p_no >= p_tot:
                break
            else:
                offset += page_size
    
    def _files(
        self,
        file_fields,
        page_size=250,
    ):
        fields = ",".join(file_fields)
        filt = None
        offset = 0
        while True:
            params = {
                "filters": filt,
                "format": "json",
                "fields": fields,
                "size": page_size,
                "from": offset,
            }

            # How to handle errors
            result = retry_get(self.files_endpoint, params=params)
            hits = result.json()["data"]["hits"]
            page = result.json()["data"]["pagination"]
            p_no = page.get("page")
            p_tot = page.get("pages")

            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")

            for hit in hits:
                yield clean_fields(hit)

            if p_no >= p_tot:
                break
            else:
                offset += page_size
def main():
    parser = argparse.ArgumentParser(description="Pull data from GDC API.")
    parser.add_argument("case_endpt_file", help="Out file name. Should end with .gz")
    parser.add_argument("file_endpt_file", help="Use (or generate if missing) cache file.")

    args = parser.parse_args()
    #def case_fields
    case_fields = []
    file_fields = []
    with open('gdc_case_fields.txt') as file:
        for line in file:
            case_fields.append(line.rstrip())
    with open('gdc_file_fields.txt') as file:
        for line in file:
            file_fields.append(line.rstrip())
    #print(case_fields[101:])
    gdc = GDC()
    #gdc.save_entries(
    #    case_out_file_names(args.case_endpt_file,1), 'case', case_fields[0:100]
    #)
    gdc.save_entries(
        case_out_file_names(args.case_endpt_file,2), 'case', ['case_id'] + case_fields[100:]
    )
    gdc.save_entries(
        args.file_endpt_file, 'file', file_fields
    )


if __name__ == "__main__":
    main()