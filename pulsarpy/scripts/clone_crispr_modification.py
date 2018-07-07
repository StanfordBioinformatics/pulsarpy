#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###Author
#Nathaniel Watson
#2017-09-18
#nathankw@stanford.edu
###

"""
Clones the specified CrisprModification onto the desired biosamples.
"""
import argparse

import pdb
import pulsarpy.models as models
import pulsarpy.utils


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--infile", required=True, help="Tab-delimited input file where column 1 is the id or name of the CrisprModification to clone, and the second column is one or more comma-delimited biosample IDs or biosample names.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    fh= open(infile)
    for line in fh:
        if line.startswith("#"):
            continue
        line = line.strip()
        line = line.split("\t")
        cm = line[0]
        cm_record = models.CrisprModification(cm)
        biosamples = line[1].split(",")
        biosamples = [x.strip() for x in biosamples]
        for b in biosamples:
            cm_record.clone(biosample_id=b)

if __name__ == "__main__":
    main()
