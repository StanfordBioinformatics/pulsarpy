#!/usr/bin/env/python

###
# Nathaniel Watson
# 2019-07-24
###

"""
Deduplicates lines based on a given field position in a tab-delimited file.
"""

import argparse

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--infile", required=True, help="The tab-delimited input file")
    parser.add_argument("-o", "--outfile", required=True, help="The deduplicated, tab-delimited output file")
    parser.add_argument("-f", "--field", required=True, type=int, help="The 0-base field position that is used to determine duplicate lines.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    infile = args.infile
    outfile = args.outfile
    field = args.field

    names = {}
    fh = open(infile)
    fout = open(outfile, "w")
    for line in fh:
        line = line.strip().split("\t")
        if not line:
            continue
        name = line[field].strip()
        if name not in names:
            names[name] = 1
        else:
            continue
        fout.write("\t".join(line) + "\n")
    fout.close()
    fh.close()

if __name__ == "__main__":
    main()
