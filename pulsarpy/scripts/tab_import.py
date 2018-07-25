#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###Author
#Nathaniel Watson
#2017-09-18
#nathankw@stanford.edu
###

"""
Given a tab-delimited sheet, creates new records of the specified Model into Pulsar LIMS or updates
existing records if the patch option is provided. Array values
should be comma-delimted as this program will split on the comma and add array literals. Array
fields are only assumed when the field name has an 'ids' suffix. 
"""
import argparse

import pulsarpy.models as models
import pulsarpy.utils

RECORD_ID_FIELD = "record_id"

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-m", "--model", required=True, help="""
      The name of the model to import the records to, i.e. Biosample or CrisprModification.""")
    parser.add_argument("-i", "--infile", required=True, help="""
      The tab-delimited input file containing records (1 per row). There must be a field-header line
      as the first row, and field names must match record attribute names. Any field names that start
      with a '#' will be skipped. Any rows that start with a '#' will also be skipped (apart from the
      header line).""")
    parser.add_argument("-p", "--patch", action="store_true", help="""
      Presence of this option means to PATCH instead of POST. The input file must contain a column
      by the name of {} to designate the existing record to PATCH.  You can use a record's primary
      ID or name as the identifier.""".format(RECORD_ID_FIELD))
 
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    model = getattr(models, args.model)
    patch = args.patch
    fh = open(infile)
    header = fh.readline().strip("\n").split("\t")
    if patch:
        if RECORD_ID_FIELD not in header:
            raise Exception("When in PATCH mode, the input file must provide the {} column.".format(RECORD_ID_FIELD))
    else:
        if RECORD_ID_FIELD in header:
            header.remove(RECORD_ID_FIELD) # No use for it in POST mode.
    field_positions = [header.index(x) for x in header if not x.startswith("#") and x.strip()]
    line_cnt = 1 # Already read header line
    for line in fh:
        line_cnt += 1
        if line.startswith("#"):
            continue
        payload = {}
        line = line.strip("\n").split("\t")
        for pos in field_positions:
            val = line[pos].strip()
            if not val and not patch:
               # Skip empty fields when POSTING, but not when PATCHING.
               continue
            field_name = header[pos]
            if field_name.endswith("ids"):
                # An array field (i.e. pooled_from_ids). Split on comma and convert to list:
                val = [x.strip() for x in val.split(",")]
            payload[header[pos]] = val
        print("Submitting line {}".format(line_cnt))
        if patch:
            rec_id = payload[RECORD_ID_FIELD]
            payload.pop(RECORD_ID_FIELD)
            rec = model(rec_id)
            res = rec.patch(payload)
        else:
            res = model.post(payload)
        print("Success: ID {}".format(res["id"]))

if __name__ == "__main__":
    main()


