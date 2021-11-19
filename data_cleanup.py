import argparse
import gzip
import logging
import sys
import time

import yaml
from yaml import Loader
import jsonlines
import cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
from cdatransform.lib import get_case_ids
from cdatransform.transform.read_using_YAML import mergedicts


def filter_cases(reader, case_list):
    if case_list is None:
        cases = None
    else:
        cases = set(case_list)

    for case in reader:
        if cases is None:
            yield case
        elif len(cases) == 0:
            break
        elif case.get("id") in cases:
            cases.remove(case.get("id"))
            yield case

def det_tree_structs(lst_of_fields):
    paths_lst = []
    new_list = []
    for k in lst_of_fields:
        k = k.split(".")
        k.pop()
        if k != []:
            new_list.append('.'.join(k))
    new_list = list(set(new_list))
    new_list2 = []
    for k in new_list:
        k = k.split('.')
        new_list2.append(k)
    new_list2 = sorted(new_list2, key=len)
    for k in new_list2:
        temp = None
        for path in reversed(k):
            temp = dict({path: temp})
        paths_lst.append(temp)
    tree = paths_lst.pop()
    for paths in paths_lst:
        tree = dict(mergedicts(tree, paths))
    return tree

def clean_up(case, tree):
    for branch, leaves in tree.items():
        if isinstance(case.get(branch),dict):
            case[branch] = [case[branch]]
        if leaves is not None and case.get(branch) is not None:
            for record in case.get(branch):
                record = clean_up(record,leaves)
    return case
                
def main():

    parser = argparse.ArgumentParser(
        prog="Transform", description="Transform source DC jsonl to Harmonized jsonl"
    )
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("field_list", help="Mapping and Transformations file.")
    parser.add_argument("output", help="Output data file.")
    parser.add_argument("--DC", help="Data Commons source. (GDC, PDC, etc.)")
    parser.add_argument("--case", help="Transform just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    args = parser.parse_args()
    case_fields = []
    with open(args.field_list) as file:
        for line in file:
            case_fields.append(line.rstrip())
    nest_tree = det_tree_structs(case_fields)
    t0 = time.time()
    count = 0
    case_list = get_case_ids(case=args.case, case_list_file=args.cases)

    with gzip.open(args.input, "r") as infp:
        with gzip.open(args.output, "w") as outfp:
            reader = jsonlines.Reader(infp)
            writer = jsonlines.Writer(outfp)
            for case in filter_cases(reader, case_list=case_list):
                writer.write(clean_up(case,nest_tree))
                count += 1
                if count % 5000 == 0:
                    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")

    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")


if __name__ == "__main__":
    # execute only if run as a script
    main()
