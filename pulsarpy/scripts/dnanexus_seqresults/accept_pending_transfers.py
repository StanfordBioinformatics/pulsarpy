#!/usr/bin/env python3

###
#Nathaniel Watson
#Stanford School of Medicine
#Nov. 6, 2018
#nathankw@stanford.edu
###

"""
Accepts DNAnexus projects pending transfer to the ENCODE org, then downloads each of the projects to the 
local host at the designated output directory. In DNAnexus, a project property will be added to the 
project; this property is 'scHub' and will be set to True to indicate that the project was 
downloaded to the SCHub pod. Project downloading is handled by the script download_cirm_dx-project.py,
which sends out notification emails as specified in the configuration file {} in both successful 
and unsuccessful circomstances.".format(conf_file). See more details at 
https://docs.google.com/document/d/1ykBa2D7kCihzIdixiOFJiSSLqhISSlqiKGMurpW5A6s/edit?usp=sharing 
and https://docs.google.com/a/stanford.edu/document/d/1AxEqCr4dWyEPBfp2r8SMtz8YE_tTTme730LsT_3URdY/edit?usp=sharing.
"""

import os
import sys
import subprocess
import logging
import argparse
import json

import dxpy

import pulsarpy.models
import scgpm_seqresults_dnanexus.dnanexus_utils as du


#The environment module gbsc/gbsc_dnanexus/current should also be loaded in order to log into DNAnexus

ENCODE_ORG = "org-snyder_encode"


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    return parser

def main():
    get_parser()
    #parser.parse_args()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:   %(message)s')
    chandler = logging.StreamHandler(sys.stdout)
    chandler.setLevel(logging.DEBUG)
    chandler.setFormatter(formatter)
    logger.addHandler(chandler)
    
    # Add debug file handler
    fhandler = logging.FileHandler(filename="log_debug_dx-seq-import.txt",mode="a")
    fhandler.setLevel(logging.DEBUG)
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)

    # Add error file handler
    err_h = logging.FileHandler(filename="log_error_dx-seq-import.txt" ,mode="a")
    err_h.setLevel(logging.ERROR)
    err_h.setFormatter(formatter)
    logger.addHandler(err_h)

    #accept pending transfers
    transferred = du.accept_project_transfers(dx_username=DX_USER,access_level="ADMINISTER",queue="ENCODE",org=ENCODE_ORG,share_with_org="CONTRIBUTE")
    #transferred is a dict. identifying the projects that were transferred to the specified billing account. Keys are the project IDs, and values are the project names.
    logger.debug("The following projects were transferred to {org}:".format(org=ENCODE_ORG))
    logger.debug(transferred)
    
    if not transferred: #will be an empty dict otherwise.
        return
    transferred_proj_ids = transferred.keys()
    for t in transferred_proj_ids:
        dxres = du.DxSeqResults(dx_project_id=t)
        proj_props = dxres.dx_project.describe(input_params={"properties": True})["properties"]
        library_name = proj_props["library_name"]
        # First search by name, then by ID if the former fails.
        # Lab members submit a name by the name of SREQ-ID, where SREQ is Pulsar's 
        # abbreviation for the SequencingRequest model, and ID is the database ID of a
        # SequencingRequest record. This gets stored into the library_name property of the 
        # corresponding DNanexus project. Problematically, this was also done in the same way when
        # we were on Syapse, and we have backported some Sypase sequencing requests into Pulsar. Such
        # SequencingRequests have been given the name as submitted in Syapse times, and this is
        # evident when the SequencingRequest's ID is different from the ID in the SREQ-ID part. 
        # Find pulsar SequencingRequest with library_name
        sreq = ppy_models.SequencingRequest.find_by({"name": library_name})
        if not sreq:
            # Search by ID. The lab sometimes doen't add a value for SequencingRequest.name.
            sreq = ppy_models.SequencingRequest.find_by("id": library_name.split("-")[1])
        if not sreq:
            logger.debug("Can't find Pulsar SequencingRequest for DNAnexus project {} ({}).".format(t, dxres.name))

        # upload results to Pulsar
        
         

if __name__ == "__main__":
    main()
