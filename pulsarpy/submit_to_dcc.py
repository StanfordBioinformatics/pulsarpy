# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Required Environment Variables
  1) Those that are required in the pulsarpy.models module to submit data to the ENCODE Portal::
     -PULSAR_API_URL and PULSAR_TOKEN
  2) Those that are required in the encode_utils.connection module to read data out of Pulsar:
     -DCC_API_KEY and DCC_SECRET_KEY
"""

from pulsarpy import models
import encode_utils.connection as euc

ENC_CONN = euc.Connection("prod")


def filter_standard_attrs(payload):
    attrs = ["created_at", "id", "owner_id", "updated_at", "user_id"]
    for i in attrs:
        if i in payload:
            payload.pop(i)
    for i in payload:
        if i.startswith("_"):
            payload.pop(i)
    return payload

def get_upstreamid_from_post(post_response):
    if "aliases" in post_response:
        upstream = post_response["aliases"][0]
    elif "accession" in post_response:
        upstream = post_response["accession"]
    elif "uuid" in post_response:
        upstream = post_response["uuid"]
    return upstream

def patch(payload, raise_403=True, extend_array_values=False):
    """Updates a record in the ENCODE Portal based on its state in Pulsar.

    Args:
        payload: `dict`. containing the attribute key and value pairs to patch.
        raise_403: `bool`. `True` means to raise a ``requests.exceptions.HTTPError`` if a 403 status
            (forbidden) is returned.
            If set to `False` and there still is a 403 return status, then the object you were
            trying to PATCH will be fetched from the Portal in JSON format as this function's
            return value.
        extend_array_values: `bool`. Only affects keys with array values. `True` (default) means to
            extend the corresponding value on the Portal with what's specified in the payload.
            `False` means to replace the value on the Portal with what's in the payload.

    Returns:
        `dict`. Will be empty if the record to PATCH wasn't found on the ENCODE Portal or if the
        connection object to the ENCODE Portal has the dry-run feature turned on. If the PATCH
        operation returns a 403 Forbidden status and the ignore403 argument is set, then the
        record as it presently exists on the Portal will be returned.
    """
    upstream_id = payload["upstream_identifier"]
    payload[ENC_CONN.ENCID_KEY] = upstream_id
    res = ENC_CONN.patch(payload=payload, raise_403=raise_403, extend_array_values=extend_array_values)
    # res will be {} if record doesn't exist on the ENCODE Portal.
    if not res:
        print("Warning: Could not PATCH {} as its upstream identifier {} was not found on the ENCODE Portal.".format(rec_id, upstream_id))

def post(payload, dcc_profile, pulsar_rec_id):
    """
    Uploads a new record from Pulsar to the ENCODE Portal and updates Pulsar's record's 
    'upstream_identifier' attribute.

    Args:
        payload: `dict`. The new record attributes to submit.
        dcc_profile: `str`. The name of the ENCODE Profile for this record, i.e. 'biosample',
            'genetic_modification'.
    Returns:
        `dict`. The POST response if the record didn't yet exist on the ENCODE Portal, or the
        record itself if it does already exist. Note that the dict. will be empty if the connection
        object to the ENCODE Portal has the dry-run feature turned on.
    """
    model = models.model_lookup(pulsar_rec_id)
    payload[ENC_CONN.PROFILE_KEY] = dcc_profile
    response_json = ENC_CONN.post(payload)
    if "aliases" in response_json:
        upstream = response_json["aliases"][0]
    elif "accession" in response_json:
        upstream = response_json["accession"]
    elif "uuid" in response_json:
        upstream = response_json["uuid"]
    models.model_record_lookup(pulsar_rec_id).patch({"upstream_identifier": upstream})
    return response_json

def post_biosample(rec_id, patch=False):
    b = models.Biosample.get(rec_id)
    payload = {}
    payload["aliases"] = [b["name"], b["tube_label"]]
    res = post(payload=payload, dcc_profile="biosample", pulsar_rec_id=rec_id)


    rec = ENC_CONN.get(rec_id, ignore404=False)
    aliases = rec[ALIASES_PROP]
    accession = rec[ACCESSION_PROP]
    # Check if upstream exists already in Pulsar:
    pulsar_rec = models.Biosample.find_by({UPSTREAM_PROP: [*aliases, accession, rec[UUID_PROP], rec["@id"]]})
    if pulsar_rec:
        return pulsar_rec
    payload = {}
    payload[UPSTREAM_PROP] = accession
    payload["name"] = set_name(rec)
    btn = rec["biosample_term_name"]
    bti = rec["biosample_term_id"]
    pulsar_btn_rec = biosample_term_name(biosample_term_name=btn, biosample_term_id=bti)
    payload["biosample_term_name_id"] = pulsar_btn_rec["id"]
    # biosample_type should already be in Pulsar.biosample_type, so won't check to add it first.
    payload["biosample_type_id"] = models.BiosampleType.find_by({"name": rec["biosample_type"]})["id"]
    date_obtained = rec.get("date_obtained")
    if not date_obtained:
        date_obtained = rec.get("culture_harvest_date")
    payload["date_biosample_taken"] = date_obtained
    payload["description"] = rec.get("description")
    payload["donor_id"] = donor(rec["donor"])["id"]
    payload["lot_identifier"] = rec.get("lot_id")
    payload["nih_institutional_certification"] = rec.get("nih_institutional_certification")
    part_of_biosample = rec.get("part_of")
    if part_of_biosample:
       # Backport the parent.
       pulsar_parent = biosample(part_of_biosample)
       payload["part_of_biosample_id"] = pulsar_parent["id"]
    payload["tissue_preservation_method"] = rec.get("preservation_method")
    payload["passage_number"] = rec.get("passage_number")
    payload["starting_amount"] = rec.get("starting_amount")
    payload["starting_amount_units"] = rec.get("starting_amount_units")
    payload["vendor_id"] = vendor(rec["source"])["id"]
    payload["vendor_product_identifier"] = rec.get("product_id")

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
