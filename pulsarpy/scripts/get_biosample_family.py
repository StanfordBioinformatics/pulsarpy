#!/usr/bin/env python3

"""
Writes Biosample details to a tabular file for a given Biosample and all descendents.  
"""

import argparse
import os

import pulsarpy.models as m

class BiosampleDetails():

    #: The fields in the header line of the output file.
    HEADER = ["Name", "ID", "WT?", "Control?", "Parent", "Pooled From"]

    def __init__(self, outfile):
        """
        Args:
            outfile: `str`. The name of the output file which will be opened in append mode. 
        """
        self.biosamples_seen = []
        self.outfile = outfile
        outfile_exists = os.path.exists(outfile)
        self.fout = open(self.outfile, 'a')
        if not outfile_exists:
            # Update header fields/ordering when fields change in log_entry().
            self.fout.write("\t".join(self.HEADER) + "\n")

    def log_entry(self, biosample):
        fields = []
        fields.append(biosample.name)
        fields.append(str(biosample.id))
        fields.append(str(biosample.wild_type))
        fields.append(str(biosample.control))
        fields.append(str(biosample.part_of_id))
        fields.append(",".join([str(x) for x in biosample.pooled_from_biosample_ids]))
        self.fout.write("\t".join(fields) + "\n")
    
    def process(self, bid):
        if bid in self.biosamples_seen:
            return
        b = m.Biosample(bid)
        self.log_entry(b)
        self.biosamples_seen.append(bid)
        children = b.biosample_part_ids + b.pooled_biosample_ids
        if children:
            for bid in children:
                self.process(bid)

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-b", "--biosample-ids", nargs="+", required=True, help="One or more Biosample IDs.")
    parser.add_argument("-o", "--outfile", required=True, help="The output file. Will be opened in append mode.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    biosample_ids = args.biosample_ids
    outfile = args.outfile
    bt = BiosampleDetails(outfile=outfile)
    for bid in biosample_ids:
        bt.process(bid)
    bt.fout.close()

if __name__ == "__main__":
    main()
