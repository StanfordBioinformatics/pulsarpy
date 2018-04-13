# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###Author                                                                                              
#Nathaniel Watson                                                                                      
#2017-09-18                                                                                            
#nathankw@stanford.edu                                                                                 
###                                                                                                    

import re

import pulsarpy.models

protocol_regx = re.compile(r'protocol', re.IGNORECASE)

UPSTREAM_PROP = "upstream_identifier"

def document(rec): 
    """
    Example document: https://www.encodeproject.org/documents/716003cd-3ce7-41ce-b1e3-6f203b6632a0/?format=json
    """
    aliases = rec["aliases"]
    if aliases:
        upstream_identifier = aliases[0]
    else:
        upstream_identifier = rec["uuid"]
    payload = {}
    payload["description"] = rec["description"]
    payload[UPSTREAM_PROP] = upstream_identifier
    document_type = rec["document_type"]
    # Determine whether this is a protocol document and set Document.is_protocol accordingly. 
    protocol = False
    if protocol_regx.search(document_type):
        protocol = True
    payload["is_protocol"] = protocol

def treatment(rec):
    """
    The required properties in the ENCODE Portal are:
    
    1. treatment_term_name, 
    2. treatment_type. 

    Example on the Portal: https://www.encodeproject.org/treatments/933a1ff2-43a2-4a54-9c87-aad228d0033e/.
    """
    payload = {}
    #check if upstream exists already in Pulsar:
    upstream = rec["aliases"][0]
    exists = models.Treatment.find_by({UPSTREAM_PROP: upstream})
    if exists:
        return exists

    payload[UPSTREAM_PROP] = upstream
    payload["concentration"] = rec["amount"]
    amount_units = rec["amount_units"]
    amount_units_rec = models.ConcentrationUnit.find_by(params={"name": amount_units})
    payload["concentration_unit_id"] = amount_units_rec["id"]
    payload["duration"] = rec["duration"]
    payload["duration_units"] = rec["duration_units"]
    ttn = rec["treatment_term_name"]
    ttn_rec = models.TreatmentTermName.find_by(params={"name": ttn)
    if not ttn_id:
        raise Exception("TreatmentTermName {} couldn't be found.".format(ttn))
    payload["treatment_term_name_id"] = ttn_rec["id"]
    payload["treatment_type"] = rec["treatment_type"]
    payload[UPSTREAM_PROP] = rec["aliases"][0]
    return models.Treatment.post(data=payload)
