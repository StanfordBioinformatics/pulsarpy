# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###Author                                                                                              
#Nathaniel Watson                                                                                      
#2017-09-18                                                                                            
#nathankw@stanford.edu                                                                                 
###                                                                                                    

import json
import pdb
import re

import encode_utils.connection as euc
import encode_utils.utils as euu
import pulsarpy.models as models
import pulsarpy.utils

protocol_regx = re.compile(r'protocol', re.IGNORECASE)

ACCESSION_PROP = "accession"
ALIASES_PROP = "aliases"
UPSTREAM_PROP = "upstream_identifier"
UUID_PROP = "uuid"
ENC_CONN = euc.Connection("prod")

# Biosamples to import for Jessika:
# https://www.encodeproject.org/search/?type=Biosample&lab.title=Michael+Snyder%2C+Stanford&award.rfa=ENCODE4&biosample_type=tissue

def set_name(rec):
    """
    Most of the models in Pulsar have a name attribute, and most of the time it is required. 
    When backporting a record from the ENCODE Portal, we need some value to use as the record's name,
    and records in the Portal don't have a name prop, so we need to use some other propery value. 
   
    The approach taken here is to use the record's "accession" properaty as a name if that is present. 
    If the record doesn't have an accession, then use the first alias in the "aliases" property.
    If there aren't any aliases, then use the record's "uuid" property. If for some reason none of
    these properties are set, an Exception is raised.

    Args:
        rec_id: `str`. An identifier for some record on the ENCODE Portal. 

    Returns:
        `str` designating the value to use for a Pulsar LIMS record's 'name' attribute. 

    Raises:
        `Exception`: A value for name couldn't be set. 
        
    """
    if ACCESSION_PROP in rec:
        return rec[ACCESSION_PROP]
    elif ALIASES_PROP in rec:
        return rec[ALIASES_PROP][0]
    elif rec[UUID_PROP] in rec:
        return rec[UUID_PROP]
    raise Exception("Can't set name for record {}".format(json.dumps(rec, indent=4)))

def biosample(rec_id):
    """
    Backports a biosample record belonging to 
    https://www.encodeproject.org/profiles/biosample.json into the Pulsar model called
    Biosample. 

    Identifying properties on the Portal are "accession", "aliases", and "uuid".
    Portal's required props are: award, biosample_term_id, biosample_term_name, biosample_type lab,
                                 organism, source

    Args:
        rec_id: `str`. An identifier for a document record on the ENCODE Portal. 

    Returns:
        `dict`: The JSON representation of the existing Document if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    rec = ENC_CONN.get(rec_id, ignore404=False)
    aliases = rec[ALIASES_PROP]
    accession = rec[ACCESSION_PROP]
    # Check if upstream exists already in Pulsar:
    pulsar_rec = models.Biosample.find_by({UPSTREAM_PROP: [*aliases, accession, rec[UUID_PROP]]})
    if pulsar_rec:
        return pulsar_rec 
    payload = {}
    payload[UPSTREAM_PROP] = accession
    payload["name"] = set_name(rec)
    btn = rec["biosample_term_name"]
    bti = rec["biosample_term_id"]
    pulsar_btn_rec = biosample_term_name(biosample_term_name=btn, biosample_term_id=bti)
    payload["biosample_term_name_id"] = pulsar_btn_rec["id"]
    payload["biosample_type_id"] = models.BiosampleType.find_by({"name": rec["biosample_type"]})["id"]
    payload["description"] = rec["description"]
    payload["donor_id"] = donor(ENC_CONN.get(rec["donor"]))["id"]
    payload["lot_identifier"] = rec["lot_id"]
    payload["passage_number"] = rec["passage_number"]
    payload["vendor_id"] = vendor(rec["source"])["id"]
    del vendor_rec
    payload["vendor_product_identifier"] = rec["product_id"]
   
    treatments = rec["treatments"]
    payload["treatment_ids"] = []
    for treat in treatments:
        pulsar_treat_rec = treatment(treat["uuid"])
        payload["treatment_ids"].append(pulsar_treat_rec["id"])
        
    
    post_response = models.Biosample.post(payload)
    # Check if any CRISPR genetic_modifications and if so, associate with biosample. 
    # In Pulsar, a biosample can only have one CRISPR genetic modification, so if there are
    # several here specified from the Portal, than that is a problem. 
    genetic_modifications = rec["genetic_modifications"]
    for g in genetic_modifications:
        gm = ENC_CONN.get(g["accession"], ignoreo404=False)
        method = gm["method"]
        if method != "CRISPR":
            continue
        crispr_modification(pulsar_biosample_id=post_response["id"], encode_gm_json=gm)
    return post_response
    
def crispr_modification(pulsar_biosample_id, encode_gm_json)
    """
    Backports a CRISPR genetic_modification record belonging to 
    https://www.encodeproject.org/profiles/genetic_modification.json into the Pulsar model called
    CripsrModification. A CRISPR genetic_modification 
    has the "method" property set to "CRISPR".

    Identifying properties on the Portal are "accession", "aliases", and "uuid".
    Requird properties on the Portal include "category", "method", and "purpose".

    Args:
        pulsar_biosample_id: `int`. The ID of the Biosample record in Pulsar with which to
            associate the genetic modification.
        encode_gm_json: `dict`. The JSON serialization of a genetic_modification record from
            the ENCODE Portal.

    Returns:
        `dict`: The JSON representation of the existing Document if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    raise Exception("Backporting a CRISPR genetic_modification from the Portal is not fully implemented at this time.")
    aliases = rec[ALIASES_PROP]
    accession = rec[ACCESSION_PROP]
    # Check if upstream exists already in Pulsar:
    pulsar_rec = models.CrisprModification.find_by({UPSTREAM_PROP: [*aliases, accession, rec[UUID_PROP]]})
    if pulsar_rec:
        return pulsar_rec 
    payload = {}
    method = rec["method"]
    if method != "CRISPR":
        raise Exception("Only CRISPR gentetic_modifications can be backported into Pulsar at this time.")
    payload[UPSTREAM_PROP] = accession
    payload["name"] = set_name(rec)
    payload["category"] = rec["category"]
    payload["purpose"] = rec["purpose"]
    
    

def biosample_term_name(biosample_term_name, biosample_term_id):
    """
    On the ENCODE Portal, a biosample record has a biosample_term_name property and a biosample_term_id
    property. These two properties in Pulsar fall under the BiosampleTermName model with names
    'biosample_term_name' and 'accession', respectivly. 
    There isn't a corresponding model for BiosampleTermName on the ENCODE Portal, and to be able to 
    determine whether Pulsar already has the provided biosample_term_name, a lookup in Pulsar will
    be done to try and match up the provided biosample_term_id with BiosampleTermName.accession.

    Args:
        biosample_term_id: `str`. The value of a biosample's 'biosample_term_id' property on the Portal. 
        biosample_term_name: `str`. The value of a biosample's 'biosample_term_name' property on the Portal. 

    Returns:
        `dict`: The JSON representation of the existing BiosampleTermName if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    payload = {}
    payload["name"] = biosample_term_name
    payload[ACCESSION_PROP] = biosample_term_id
    # Check if upstream exists already in Pulsar:
    pulsar_rec = models.BiosampleTermName.find_by({ACCESSION_PROP: biosample_term_id})
    if pulsar_rec:
        return pulsar_rec
    return models.BiosampleTermName.post(payload)


def document(rec_id): 
    """
    Backports a document record belonging to 
    https://www.encodeproject.org/profiles/document.json.
    Example document: https://www.encodeproject.org/documents/716003cd-3ce7-41ce-b1e3-6f203b6632a0/?format=json
  
    Identifying properties on the Portal are "aliases" and "uuid".

    Args:
        rec_id: `str`. An identifier for a document record on the ENCODE Portal. 

    Returns:
        `dict`: The JSON representation of the existing Document if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    raise Exception("Due to a bug in the ENCODE Portal, can't fetch document '{}' via the REST API.".format(rec))
    rec = ENC_CONN.get(rec_id, ignore404=False)
    aliases = rec[ALIASES_PROP]
    # Check if upstream exists already in Pulsar:
    pulsar_rec = models.Document.find_by({UPSTREAM_PROP: [*aliases, rec[UUID_PROP]]})
    if pulsar_rec:
        return pulsar_rec 
    payload = {}
    payload["description"] = rec["description"]
    if aliases:
        upstream_identifier = aliases[0]
    else:
        upstream_identifier = rec[UUID_PROP]
    payload[UPSTREAM_PROP] = upstream_identifier
    document_type = rec["document_type"]
    # Determine whether this is a protocol document and set Document.is_protocol accordingly. 
    protocol = False
    if protocol_regx.search(document_type):
        protocol = True
    payload["is_protocol"] = protocol

def donor(rec_id):
    """
    Backports a huma-donor record belonging to 
    https://www.encodeproject.org/profiles/human_donor.json.

    The record will be checked for existence in Pulsar by doing a search on the field
    `donor.upstread_identifer`` using as a query value the record's accession on the ENCODE Portal, 
    and also its aliases alias.

    Args:
        rec_id: `str`. An identifier (alias or uuid) for a human-donor record on the ENCODE Portal.

    Returns:
        `dict`: The JSON representation of the existing Donor if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    rec = ENC_CONN.get(rec_id, ignore404=False)
    accession = rec[ACCESSION_PROP]
    aliases = rec[ALIASES_PROP]
    # Check if upstream exists already in Pulsar:
    pulsar_rec = models.Donor.find_by({UPSTREAM_PROP: [accession, *aliases]})
    if pulsar_rec:
        return pulsar_rec 
    payload = {}
    AGE_PROP = "age"
    if AGE_PROP in rec:
        payload[AGE_PROP] = rec[AGE_PROP]
    GENDER_PROP = "sex"
    if GENDER_PROP in rec:
        payload["gender"] = rec[GENDER_PROP]
    payload[UPSTREAM_PROP] = accession
    payload["name"] = euu.strip_alias_prefix(aliases[0])
    return models.Donor.post(payload)

def treatment(rec_id):
    """
    Backports a treatement record belonging to
    https://www.encodeproject.org/profiles/treatment.json.
    The required properties in the ENCODE Portal are:
    
    1. treatment_term_name, 
    2. treatment_type. 

    An example on the Portal: https://www.encodeproject.org/treatments/933a1ff2-43a2-4a54-9c87-aad228d0033e/.
    Identifying properties on the Portal are 'aliases' and 'uuid'.

    Args:
        rec_id: `str`. An identifier (alias or uuid) for a treatment record on the ENCODE Portal. 

    Returns:
        `dict`: The JSON representation of the existing Treatment if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    rec = ENC_CONN.get(rec_id, ignore404=False)
    aliases = rec[ALIASES_PROP]
    #check if upstream exists already in Pulsar:
    pulsar_rec = models.Treatment.find_by({UPSTREAM_PROP: [*aliases, rec[UUID_PROP]]})
    if pulsar_rec:
        return pulsar_rec 

    payload = {}
    documents = rec["documents"]
    # Add any linked documents that aren't in Pulsar already:
    for doc in documents:
        document(doc)
    if aliases:
        upstream_identifier = rec[ALIASES_PROP][0]
    else:
        upstream_identifier = rec[UUID_PROP]
    payload[UPSTREAM_PROP] = upstream_identifier
    payload["concentration"] = rec["amount"]
    amount_units = rec["amount_units"]
    pulsar_amount_units_rec = models.ConcentrationUnit.find_by(payload={"name": amount_units})
    if not pulsar_amount_units_rec:
        raise Exception("ConcentrationUnit '{}' couldn't be found.".format(amount_units))
    pulsar_amount_units_rec = pulsar_amount_units_rec["concentration_unit"]
    payload["concentration_unit_id"] = pulsar_amount_units_rec["id"]
    payload["duration"] = rec["duration"]
    payload["duration_units"] = rec["duration_units"]
    temp_prop_name = "temperature"
    if temp_prop_name  in rec:
        temp = rec[temp_prop_name]
        temp_units = rec["temperature_units"]
        if temp_units == "Kelvin":
            temp = pulsarpy.utils.kelvin_to_celsius(temp)
        elif temp_units == "Fahrenheit":
            temp = pulsarpy.utils.fahrenheit_to_celsius(temp)
        payload[temp_prop_name] = rec[temp_prop_name]
    ttn = rec["treatment_term_name"]
    tti = rec["treatment_term_id"]
    #Create new TreatmentTermName if not found in Pulsar.
    pulsar_ttn_rec = treatment_term_name(ttn, tti)["treatment_term_name"]
    payload["treatment_term_name_id"] = pulsar_ttn_rec["id"]
    payload["treatment_type"] = rec["treatment_type"]
    #The Portal's treatment model doesn't include a name prop or a description prop. 
    # Thus, 'name' shall be set to be equal to 'upstream_identifier'.
    payload["name"] = euu.strip_alias_prefix(upstream)
    return models.Treatment.post(payload)

def treatment_term_name(treatment_term_name, treatment_term_id):
    """
    On the ENCODE Portal, a treatment record has a treatment_term_name property and a treatment_term_id
    property. These two properties in Pulsar fall under the TreatmentTermName model with names
    'treatment_term_name' and 'accession', respectivly. 

    There isn't a corresponding model for TreatmentTermName on the ENCODE Portal, and to be able to 
    determine whether Pulsar already has the provided treatment_term_name, a lookup in Pulsar will
    be done to try and match up the provided treatment_term_id with TreatmentTermName.accession.

    Args:
        treatment_term_id: `str`. The value of a treatment's 'treatment_term_id' property on the Portal. 
        treatment_term_name: `str`. The value of a treatment's 'treatment_term_name' property on the Portal. 

    Returns:
        `dict`: The JSON representation of the existing TreatmentTermName if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    payload = {}
    payload["name"] = treatment_term_name
    payload[ACCESSION_PROP] = treatment_term_id
    pulsar_rec = models.TreatmentTermName.find_by({ACCESSION_PROP: treatment_term_id})
    if pulsar_rec:
        return pulsar_rec
    return models.TreatmentTermName.post(payload)
    
def vendor(rec_id):
    """
    Backports a source record belonging to 
    https://www.encodeproject.org/profiles/source.json.

    Identifying properties on the Portal are "name", and "uuid".
    Portal's required props are: "name", and "title". 

    Args:
        rec_id: `str`. An identifier (name or uuid) for a source record on the ENCODE Portal.

    Returns:
        `dict`: The JSON representation of the existing source if it already exists in
        in Pulsar, otherwise the POST response.  
    """
    rec = ENC_CONN.get(rec_id, ignore404=False)
    name = rec["name"]
    uuid = rec["uuid"]
    pulsar_rec = models.Vendor.find_by({UPSTREAM_PROP: [name, uuid]})
    if pulsar_rec:
        return pulsar_rec 
    payload = {}
    payload["description"] = rec["description"]
    payload["name"] = name
    payload[UPSTREAM_PROP] = name
    payload["url"] = rec["url"]
    return models.Vendor.post(payload)
    
