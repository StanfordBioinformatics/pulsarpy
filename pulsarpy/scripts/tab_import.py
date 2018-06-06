#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###Author
#Nathaniel Watson
#2017-09-18
#nathankw@stanford.edu
###

"""
Given a tab-delimited sheet, imports records of the specified Model into Pulsar LIMS. 
"""
import argparse
import pdb

import pulsarpy.models as models
import pulsarpy.utils


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-m", "--model", required=True, help="""
      The name of the model to import the records to, i.e. Biosample or CrisprModification.""")
    parser.add_argument("-i", "--infile", required=True, help="""
      The tab-delimited input file containing records (1 per row). There must be a field-header line
      as the first row, and field names must match record attribute names. Any field names that start
      with a '#' will be skipped. Any rows that start with a '#' will also be skipped (apart from the
      header line).""")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    model = getattr(models, args.model)
    fh = open(infile)
    header = fh.readline().strip().split("\t")
    field_positions = [header.index(x) for x in header if not x.startswith("#") and x.strip()]
    line_cnt = 1 # Already read header line
    for line in fh:
        line_cnt += 1
        if line.startswith("#"):
            continue
        payload = {}
        line = line.strip().split("\t")
        for pos in field_positions:
            payload[header[pos]] = line[pos].strip()
        print("Submitting line {}".format(line_cnt))
        res = model.post(payload)
        print(res)

if __name__ == "__main__":
    main()


