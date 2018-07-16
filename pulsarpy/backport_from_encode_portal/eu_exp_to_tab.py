#!/usr/bin/env python3


"""
Fetches the given experiment from the Portal in JSON format, and serializes it to tab-delimited
format.
"""

import argparse
import os

import encode_utils as euc
from encode_utils.parent_argparser import dcc_login_parser

# Check that Python3 is being used
v = sys.version_info
if v < (3, 3):
    raise Exception("Requires Python 3.3 or greater.")

EXP_TAB = "experiments.txt"
# name
# upstream_identifier
# description
# #target_name
# document_ids
# submitter_comments
# notes
REP_TAB = "replicates.txt"
# name
# upstream_identifier
# chipseq_experiment_id
# biosample_id
# #biosample_alias
# biological_replicate_number
# technical_replicate_number
# antibody_id
# #antibody_accession
# notes
BIO_TAB = "biosamples.txt"
# name
# upstream_identifier
# part_of_id
# nih_institutional_certification
# pooled_from_biosample_ids
# treatment_ids
# document_ids
# biosample_type_id
# biosample_term_name_id
# vendor_id
# vendor_product_identifier
# donor_id
# passage_number
# date_biosample_taken
# notes


def portal_ids_to_aliases(ids):
    """
    Given a list of identifiers from the Portal, gets the first alias of each record specified by
    the identifier and returns the result in a list. If a particular record doesn't have any
    aliases, then the original identifier provided is used in place.
    """
    res = []
    for i in ids:
        rec = conn.get(i)
        aliases = rec["aliases"]
        if not aliases:
            res.append(i)
        else:
            res.append(aliases[0])
    return res
        


def get_parser():
    parser = argparse.ArgumentParser(
        description = __doc__,
        parents=[dcc_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-e", "--exp", required=True, help="An identifier for an experiment record.")
    parser.add_argument("-o", "--outdir", required=True, help="Output directory.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    exp = args.exp
    outdir = args.outdir
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    dcc_mode = args.dcc_mode

    if dcc_mode:
        conn = euc.Connection(dcc_mode)
    else:
        # Default dcc_mode taken from environment variable DCC_MODE.
        conn = euc.Connection()

    exp_file = os.path.join(outdir, EXP_TAB)
    expfh = open(exp_file, "a")
    rec = conn.get(exp)
    expfh.write("\t") # emtpy for name field in Pulsar
    exp_alias = exp["aliases"][0]
    expfh.write(exp_alias + "\t")
    expfh.write(exp["description"] + "\t")
    expfh.write(exp["target"]["name"] + "\t")
    document_aliases = portal_ids_to_aliases(exp["documents"])
    expfh.write(",".join(document_aliases) + "\t")
    submitter_comments = exp.get("submitter_comment")
    expfh.write(submitter_comments + "\t")
    expfh.write("\t") # empty for notes field in Pulsar
    # START REPLICATE FILE
    rep_file = os.path.join(outdir, REP_TAB)
    repfh = open(rep_file, "a")
    reps = exp["replicates"]
    for i in reps:
        repfh.write("\t") # empty for name field in Pulsar
        repfh.write(i["aliases"][0]
        repfh.write(exp_alias + "\t")
        lib = i["library"]
        bio = lib["biosample"]
        repfh.write("\t") # empty for biosample_id fkey field in Pulsar
        biosample_alias = bio["aliases"][0]
        repfh.write(biosample_alias + "\t")
        repfh.write(i["biological_replicate_number"]
        repfh.write(i["technical_replicate_number"]
        repfh.write("\t") # empty for antibody_id fkey field in Pulsar
        repfh.write(i["antibody"]["accession"] + "\t")
        repfh.write("\t") # empty for notes field in Pulsar
        # START BIOSAMPLE FILE
        bio_file = os.path.join(outdir, BIO_TAB)
        biofh = open(bio_file, "a")
        biofh.write("\t") # empty for name field in Pulsar
        biofh.write(bio["aliases"][0] + "\t")
        biofh.write(bio.get("part_of") + "\t")
        biofh.write(bio.get("nih_institutional_certification") + "\t")
        biofh.write(bio.get("pooled_from") + "\t")
        treatment_aliases = portal_ids_to_aliases(bio["treatments"])
        biofh.write(",".join(treatments_aliases) + "\t")
        document_aliases = portal_ids_to_aliases(bio["documents"])
        biofh.write(",".join(document_aliases) + "\t")
        biofh.write(bio["biosample_type"] + "\t")
        biofh.write(bio["biosample_term_name"] + "\t")
        biofh.write(bio["source"]["name"] + "\t")
        biofh.write(bio.get("product_id") + "\t")
        biofh.write(bio["donor"]["aliases"][0] + "\t")
        biofh.write(bio.get("passage_number") + "\t")
        date_taken = bio.get("culture_start_date")
        if not date_taken:
            date_taken = bio.get("date_obtained")
        biofh.write(date_taken + "\t")
        biofh.write("\t") # empty for notes field in Pulsar
