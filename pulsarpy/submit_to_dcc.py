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

import base64

from pulsarpy import models
import encode_utils as eu
import encode_utils.connection as euc
import pdb


class UpstreamNotSet(Exception):
    pass


class Submit():
    UPSTREAM_ATTR = "upstream_identifier"

    def __init__(self, dcc_mode=None):
        if not dcc_mode:
            try:                                                                                    
                dcc_mode = os.environ["DCC_MODE"]                                                   
                print("Utilizing DCC_MODE environment variable.")                 
            except KeyError:                                                                        
                print("ERROR: You must supply the `dcc_mode` argument or set the environment variable DCC_MODE.")
                sys.exit(-1)                                                                        
        self.dcc_mode = dcc_mode
        self.ENC_CONN = euc.Connection(self.dcc_mode)
    
    def filter_standard_attrs(self, payload):
        attrs = ["created_at", "id", "owner_id", "updated_at", "user_id"]
        for i in attrs:
            if i in payload:
                payload.pop(i)
        for i in payload:
            if i.startswith("_"):
                payload.pop(i)
        return payload
    
    def patch(self, pulsar_rec_id,  payload, raise_403=True, extend_array_values=False):
        """Updates a record in the ENCODE Portal based on its state in Pulsar. 
    
        Args:
            payload: `dict`. containing the attribute key and value pairs to patch.
            pulsar_rec_id: `str`. The Pulsar record identifier of the record we are POSTING to the DCC. Must
                contain the model prefix, i.e. DOC-3 for the document record with primary ID 3.   
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

        Raises:
            pulsarpy.submit_to_dcc.UpstreamNotSet: The Pulsar record's 'upstream_identifier' property
              isn't set. 
            
        """
        upstream = payload.pop(self.UPSTREAM_ATTR)
        if not upstream:
            raise UpstreamNotSet("Pulsar record '{}' has no upstream value set.".format(pulsar_rec_id))
        payload[self.ENC_CONN.ENCID_KEY] = upstream
        res = self.ENC_CONN.patch(payload=payload, raise_403=raise_403, extend_array_values=extend_array_values)
        # res will be {} if record doesn't exist on the ENCODE Portal.
        if not res:
            print("Warning: Could not PATCH '{}' as its upstream identifier was not found on the ENCODE Portal.".format(upstream))
        return res
    
    def post(self, payload, dcc_profile, pulsar_model, pulsar_rec_id):
        """
        POSTS a record from Pulsar to the ENCODE Portal and updates the Pulsar record's
        'upstream_identifier' attribute to reference the new object on the Portal (but only if 
        in production mode where `self.dcc_mode = "prod"`). Before attempting
        a POST, first checks whether the record in Pulsar already has the upstream value set, in
        which case no POST will be made and the upstream value will be returned. In order for this 
        mandatory check to work, the provided payload must include a key for ``self.UPSTREAM_ATTR``.
    
        Args:
            payload: `dict`. The new record attributes to submit.
            dcc_profile: `str`. The name of the ENCODE Profile for this record, i.e. 'biosample',
                'genetic_modification'.
            pulsar_model: One of the defined subclasses of the ``models.Model`` class, i.e. 
                ``models.Model.Biosample``, which will be used to set the Pulsar record's 
                upstream_identifier attribute after a successful POST to the ENCODE Portal.
            pulsar_rec_id: `str`. The identifier of the Pulsar record to POST to the DCC.
        Returns:
            `str`: The identifier of the new record on the ENCODE Portal, or the existing
            record identifier if the record already exists. The identifier of either the new record 
            or existing record on the ENCODE Portal is determined to be the 'accession' if that property is present, 
            otherwise it's the first alias if the 'aliases' property is present, otherwise its 
            the 'uuid' property.
        """
        # Make sure the record's UPSTREAM_ATTR isn't set, which would mean that it was already POSTED
        upstream = payload.pop(self.UPSTREAM_ATTR)
        if upstream:
            print("Will not POST '{}' since it was already submitted as '{}'.".format(pulsar_rec_id, upstream))
            return upstream
        payload[self.ENC_CONN.PROFILE_KEY] = dcc_profile
    
        # `dict`. The POST response if the record didn't yet exist on the ENCODE Portal, or the
        # record itself if it does already exist. Note that the dict. will be empty if the connection
        # object to the ENCODE Portal has the dry-run feature turned on.
        print(payload)
        response_json = self.ENC_CONN.post(payload)
        if "accession" in response_json:
            upstream = response_json["accession"]
        elif "aliases" in response_json:
            upstream = response_json["aliases"][0]
        elif "uuid" in response_json:
            upstream = response_json["uuid"]
        # Set value of the Pulsar record's upstream_identifier, but only if we are in prod mode since
        # we don't want to set it to an upstream identifiers from any of the ENOCODE Portal test servers. 
        if self.dcc_mode == eu.DCC_PROD_MODE:
            print("Setting the Pulsar record's upstream_identifier attribute to '{}'.".format(upstream))
            pulsar_model.patch(uid=pulsar_rec_id, payload={"upstream_identifier": upstream})
        return upstream
    
    def post_crispr_modification(self, rec_id, patch=False):
        rec = models.CrisprModification.get(rec_id)
        payload = {}
        res = self.post(payload=payload, dcc_profile="genetic_modification", pulsar_model=models.CrisprModification, pulsar_rec_id=rec_id)
        return res
    

    def get_upstream_id(self, rec):
        return rec[self.UPSTREAM_ATTR]
    
    def post_document(self, rec_id, patch=False):
        rec = models.Document.get(rec_id)
        alias = models.Document.MODEL_ABBR + "-" + str(rec["id"])
        payload = {}
        payload[self.UPSTREAM_ATTR] = rec[self.UPSTREAM_ATTR]
        payload["aliases"] = [rec["name"], alias]
        payload["description"] = rec["description"]
        payload["document_type"] = rec["document_type"]["name"]
        content_type = rec["content_type"]
        # Create attachment for the attachment prop
        file_contents = models.Document.download(rec_id)
        data = base64.b64encode(file_contents)
        temp_uri = str(data, "utf-8")
        href = "data:{mime_type};base64,{temp_uri}".format(mime_type=content_type, temp_uri=temp_uri)
        attachment = {}
        attachment["download"] = rec["name"]
        attachment["type"] = content_type 
        attachment["href"] = href
        payload["attachment"] = attachment
        if patch:
            res = self.patch(payload=payload, pulsar_rec_id=rec_id)
        else:
            res = self.post(payload=payload, dcc_profile="document", pulsar_model=models.Document, pulsar_rec_id=rec_id)
        return res

    def post_treatment(self, rec_id, patch=False):
        rec = models.Treatment.get(rec_id)
        alias = models.Treatment.MODEL_ABBR + "-" + str(rec["id"])
        payload = {}
        payload[self.UPSTREAM_ATTR] = rec[self.UPSTREAM_ATTR]
        payload["aliases"] = [rec["name"], alias]
        conc = rec.get("concentration")
        if conc:
            payload["amount"] = conc
            payload["amount_units"] = rec["concentration_unit"]["name"]
        duration = rec.get("duration") 
        if duration:
            payload["duration"] = duration
            payload["duration_units"] = rec["duration_units"]
        temp = rec.get("temperature_celsius")
        if temp:
            payload["temperature"] = temp
            payload["temperature_units"] = "Celsius"
        payload["treatment_term_id"] = rec["treatment_term_name"]["accession"]
        payload["treatment_term_name"] = rec["treatment_term_name"]["name"]
        payload["treatment_type"] = rec["treatment_type"]

        documents = rec["documents"]
        doc_upstreams = []
        for doc in documents:
            doc_upstream = doc[self.UPSTREAM_ATTR]
            if not doc_upstream:
                doc_upstream = post_document(doc)
            doc_upstreams.append(doc_upstream)
        payload["documents"] = doc_upstreams
        # Submit
        if patch:
            res = self.patch(payload=payload, pulsar_rec_id=rec_id)
        else:
            res = self.post(payload=payload, dcc_profile="treatment", pulsar_model=models.Treatment, pulsar_rec_id=rec_id)
        return res

    
    def post_vendor(self, rec_id, patch=False):
        """
        """
        rec = models.Vendor.get(rec_id)
        alias = models.Vendor.MODEL_ABBR + "-" + str(rec["id"])
        payload = {}
        payload[self.UPSTREAM_ATTR] = rec[self.UPSTREAM_ATTR]
        payload["aliases"] = [rec["name"], alias]
        payload["description"] = rec["description"]
        payload["name"] = rec["name"]
        payload["url"] = rec["url"]
        payload["title"] = rec["name"]
        if patch:
            res = self.patch(payload=payload, dcc_profile="source", pulsar_rec_id=rec_id)
        else:
            res = self.post(payload=payload, dcc_profile="source", pulsar_model=models.Vendor, pulsar_rec_id=rec_id)
        return res
    
    def post_biosample(self, rec_id, patch=False):
        rec = models.Biosample.get(rec_id)
        alias = models.Biosample.MODEL_ABBR + "-" + str(rec["id"])
        payload = {}
        payload[self.UPSTREAM_ATTR] = rec[self.UPSTREAM_ATTR]
        # The alias lab prefixes will be set in the encode_utils package if the DCC_LAB environment
        # variable is set.
        payload["aliases"] = [rec["name"], rec["tube_label"], alias]
        payload["biosample_term_name"] = rec["biosample_term_name"]["name"]
        payload["biosample_term_id"] = rec["biosample_term_name"]["accession"]
        payload["biosample_type"] = rec["biosample_type"]["name"]
        date_biosample_taken = rec["date_biosample_taken"]
        if date_biosample_taken:
            payload["culture_harvest_date"] = date_biosample_taken
        payload["description"] = rec["description"]
        lot_id = rec["lot_identifier"] 
        if lot_id:
            payload["lot_id"] = lot_id
        payload["nih_institutional_certification"] = rec["nih_institutional_certification"]
        payload["organism"] = "human"
        passage_number = rec["passage_number"]
        if passage_number:
            payload["passage_number"] = passage_number
        starting_amount = rec["starting_amount"] 
        if starting_amount:
            payload["starting_amount"] = starting_amount
            payload["starting_amount_units"] = rec["starting_amount_units"]
        submitter_comment = rec["submitter_comments"]
        if submitter_comment:
            payload["submitter_comment"] = submitter_comment
        preservation_method = rec["tissue_preservation_method"]
        if preservation_method:
            payload["preservation_method"] = rec["tissue_preservation_method"]
        prod_id = rec["vendor_product_identifier"]
        if prod_id:
            payload["product_id"] = prod_id
    
        crispr_modification = rec["crispr_modification"]
        if crispr_modification:
            crispr_mod_upstream = crispr_modification[self.UPSTREAM_ATTR]
            if not crispr_mod_upstream:
                crispr_mod_upstream = post_crispr_modification(crispr_modification)
            payload["genetic_modifications"] = crispr_mod_upstream
    
        documents = rec["documents"]
        doc_upstreams = []
        for doc in documents:
            doc_upstream = doc[self.UPSTREAM_ATTR]
            if not doc_upstream:
                doc_upstream = self.post_document(doc["id"])
            doc_upstreams.append(doc_upstream)
        payload["documents"] = doc_upstreams
    
        donor_upstream = rec["donor"][self.UPSTREAM_ATTR]
        if not donor_upstream:
            raise Exception("Donor '{}' of biosample '{}' does not have its upstream set. Donors must be registered with the DCC directly.".format(rec["donor"]["id"], rec_id))
        payload["donor"] = donor_upstream
    
    
        part_of_biosample_id = rec["part_of_biosample_id"]
        if part_of_biosample_id:
            part_of_biosample = models.Biosample.get(part_of_biosample_id)
            pob_upstream = part_of_biosample[self.UPSTREAM_ATTR]
            if not pob_upstream:
                pob_upstream = self.post_biosample(part_of_biosample["id"])
            payload["part_of"] = pob_upstream
    
        pooled_from_biosamples = rec["pooled_from_biosamples"]
        if pooled_from_biosamples:
            payload["pooled_from"] = []
            for p in pooled_from_biosamples:
                p_upstream = p[self.UPSTREAM_ATTR]
                if not p_upstream:
                    p_upstream = self.post_biosample(p["id"])
                payload["pooled_from"].append(p_upstream)
    
        vendor_upstream = rec["vendor"][self.UPSTREAM_ATTR]
        if not vendor_upstream:
            vendor_upstream = self.post_vendor(rec["vendor"]["id"])
        payload["source"] = vendor_upstream
    
        treatments = rec["treatments"]
        treat_upstreams = []
        for treat in treatments:
            treat_upstream = treat[self.PSTREAM_ATTR]
            if not treat_upstream:
                treat_upstream = post_treatment(treat["id"])
            treat_upstreams.append(treat_upstream)
        payload["treatments"] = treat_upstreams
   
        if patch:  
            res = self.patch(payload=payload, pulsar_rec_id=rec_id)
        else:
            res = self.post(payload=payload, dcc_profile="biosample", pulsar_model=models.Biosample, pulsar_rec_id=rec_id)
        return res
