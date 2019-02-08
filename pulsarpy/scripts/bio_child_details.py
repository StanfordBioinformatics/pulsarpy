"""
Writes Biosample details to a tabular file for a given Biosample and all descendents.  
"""

import argparse
import os

import pulsarpy.models as m

def entry(biosample):
    fields = []
    fields.append(biosample.name)
    fields.append(str(biosample.id))
    fields.append(str(biosample.wild_type))
    fields.append(str(biosample.control))
    fields.append(str(biosample.part_of_id))
    fields.append(",".join(biosample.pooled_from_biosample_ids))
    return "\t".join(fields) + "\n"

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-o", "--outfile", required=True, help="Output file. Will be opended in append mode.")
    parser.add_argument("-b", "--biosample-ids", nargs="+", required=True, help="One or more Biosample IDs.")
    return parser
    

def main():
    parser = get_parser()
    args = parser.parse_args()
    outfile = args.outfile
    biosample_ids = args.biosample_ids

    header = ["Name", "ID", "WT?", "Control?", "Parent", "Pooled From"]
    outfile_exists = os.path.exists(outfile)
    fout = open(outfile, "a")
    if not outfile_exists:
        # Update header fields/ordering when fields change in entry().
        fout.write("\t".join(header) + "\n")
    for bid in biosample_ids:
        b = m.Biosample(bid)
        fout.write(entry(b))
        for child_id in b.biosample_part_ids + b.pooled_biosample_ids:
            child_biosample = m.Biosample(child_id)
            fout.write(entry(child_biosample))
    fout.close()

if __name__ == "__main__":
    main()
