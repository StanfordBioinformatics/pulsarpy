#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###Author
#Nathaniel Watson
#2018-l0-19
#nathankw@stanford.edu
###

"""
Given an input file containing record names, one per row, indicates whether the record exists by
outputting a 1 if it exists and a 0 if it doesn't. The input file of record names will be first
sorted and deduplicated.
"""
import argparse

import pulsarpy.models as models

RECORD_ID_FIELD = "record_id"

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-m", "--model", required=True, help="""
      The name of the model to import the records to, i.e. Biosample or CrisprModification.""")
    parser.add_argument("-i", "--infile", required=True, help="""
      One or more record names, one per line.""")
    parser.add_argument("-o", "--outfile", required=True, help="""
      The output file with two columns: 1) name, 2) status (1 for present, 0 for absent).""")
 
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    fout = open(args.outfile, 'w')
    model = getattr(models, args.model)
    fh = open(infile)
    names = []
    for line in fh:
        line = line.strip()
        if not line:
            continue
        names.append(line)
    names = set(names)
    for n in names:
        try:
            model(n)
            fout.write(n + "\t" + "1\n")
        except models.RecordNotFound:
            fout.write(n + "\t" + "0\n")
            print(0)
    fout.close()
        

if __name__ == "__main__":
    main()
