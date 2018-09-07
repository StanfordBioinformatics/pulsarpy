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
import logging

from pulsarpy import models
import pulsarpy.utils
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

    def clean_name(self, name):
        """
        Removes characters from a string that are not allowed when submitting to certain properties,
        such as aliases. Some characters are replaced with allowed
        character equivalents rather than removed.

        Examples:  
            "hi #1" -> "hi 1"
            "6/24/18" -> "6-24-18" 

        Args:
            name: `str`. The value to clean.

        Returns:
            `str`. The cleaned value that is submission acceptable. 
        """
        return name.replace("#","").replace("/","-")
                  

    def patch(self, payload, raise_403=True, extend_array_values=False):
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

        Raises:
            pulsarpy.submit_to_dcc.UpstreamNotSet: The Pulsar record's 'upstream_identifier' property
              isn't set. 
            
        """
        upstream = payload.pop(self.UPSTREAM_ATTR)
        if not upstream:
            raise UpstreamNotSet("Payload '{}' has no upstream value set.".format(payload))
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
        upstream = ""
        if self.UPSTREAM_ATTR in payload:
            upstream = payload.pop(self.UPSTREAM_ATTR)
        if upstream:
            print("Will not POST '{}' since it was already submitted as '{}'.".format(pulsar_rec_id, upstream))
            return upstream
        payload[self.ENC_CONN.PROFILE_KEY] = dcc_profile
    
        # `dict`. The POST response if the record didn't yet exist on the ENCODE Portal, or the
        # record itself if it does already exist. Note that the dict. will be empty if the connection
        # object to the ENCODE Portal has the dry-run feature turned on.
        response_json = self.ENC_CONN.post(payload)
        if "accession" in response_json:
            upstream = response_json["accession"]
        elif "aliases" in response_json:
            upstream = response_json["aliases"][0]
        else:
            upstream = response_json["uuid"]
        # Set value of the Pulsar record's upstream_identifier, but only if we are in prod mode since
        # we don't want to set it to an upstream identifiers from any of the ENOCODE Portal test servers. 
        if self.dcc_mode == eu.DCC_PROD_MODE:
            print("Setting the Pulsar record's upstream_identifier attribute to '{}'.".format(upstream))
            pulsar_rec = pulsar_model(pulsar_rec_id)
            pulsar_rec.patch(payload={"upstream_identifier": upstream})
            print("upstream_identifier attribute set successfully.")
        return upstream
    
    def post_crispr_modification(self, rec_id, patch=False):
        rec = models.CrisprModification(rec_id)
        aliases = []
        aliases.append(rec.abbrev_id())
        aliases.append(self.clean_name(rec["name"]))
        payload = {}
        payload["aliases"] = aliases
        res = self.post(payload=payload, dcc_profile="genetic_modification", pulsar_model=models.CrisprModification, pulsar_rec_id=rec_id)
        return res
    
    def get_upstream_id(self, rec):
        return rec.attrs.get(self.UPSTREAM_ATTR)
    
    def post_document(self, rec_id, patch=False):
        rec = models.Document(rec_id)
        aliases = []
        aliases.append(rec.abbrev_id())
        name = rec.name
        if name:
            aliases.append(self.clean_name(name))
        payload = {}
        payload["aliases"] = aliases
        payload[self.UPSTREAM_ATTR] = self.get_upstream_id(rec)
        payload["description"] = rec.description
        doc_type = models.DocumentType(rec.document_type_id)
        payload["document_type"] = doc_type.name
        content_type = rec.content_type
        # Create attachment for the attachment prop
        file_contents = rec.download()
        data = base64.b64encode(file_contents)
        temp_uri = str(data, "utf-8")
        href = "data:{mime_type};base64,{temp_uri}".format(mime_type=content_type, temp_uri=temp_uri)
        attachment = {}
        attachment["download"] = rec.name
        attachment["type"] = content_type 
        attachment["href"] = href
        payload["attachment"] = attachment
        if patch:
            res = self.patch(payload=payload)
        else:
            res = self.post(payload=payload, dcc_profile="document", pulsar_model=models.Document, pulsar_rec_id=rec_id)
        return res

    def post_treatment(self, rec_id, patch=False):
        rec = models.Treatment(rec_id)
        aliases = []
        aliases.append(rec.abbrev_id())
        name = rec.name
        if name:
            aliases.append(self.clean_name(name))
        payload = {}
        payload["aliases"] = aliases
        payload[self.UPSTREAM_ATTR] = self.get_upstream_id(rec) 
        conc = rec.concentration
        if conc:
            payload["amount"] = conc
            conc_unit = models.Unit(rec.concentration_unit_id)
            payload["amount_units"] = conc_unit.name
        duration = rec.duration
        if duration:
            payload["duration"] = duration
            payload["duration_units"] = rec.duration_units
        temp = rec.temperature_celsius
        if temp:
            payload["temperature"] = temp
            payload["temperature_units"] = "Celsius"
        ttn = models.TreatmentTermName(rec.treatment_term_name_id)
        payload["treatment_term_id"] = ttn["accession"]
        payload["treatment_term_name"] = ttn["name"]
        payload["treatment_type"] = rec.treatment_type

        doc_ids = rec.document_ids
        docs = [models.Document(d) for d in doc_ids]
        doc_upstreams = []
        for doc in docs:
            doc_upstream = self.get_upstream_id(doc) 
            if not doc_upstream:
                doc_upstream = post_document(doc)
            doc_upstreams.append(doc_upstream)
        payload["documents"] = doc_upstreams
        # Submit
        if patch:
            res = self.patch(payload=payload)
        else:
            res = self.post(payload=payload, dcc_profile="treatment", pulsar_model=models.Treatment, pulsar_rec_id=rec_id)
        return res

    
    def post_vendor(self, rec_id, patch=False):
        """
        Vendors must be registered directly by the DCC personel. 
        """
        raise Exception("Vendors must be registered directly by the DCC personel.")

    def post_biosample(self, rec_id, patch=False):
        rec = models.Biosample(rec_id)
        # The alias lab prefixes will be set in the encode_utils package if the DCC_LAB environment
        # variable is set.
        aliases = []
        aliases.append(rec.abbrev_id())
        name = rec.name
        if name: 
          aliases.append(self.clean_name(name))
        payload = {}
        payload["aliases"] = aliases
        btn = models.BiosampleTermName(rec.biosample_term_name_id)
        payload["biosample_term_name"] = btn.name.lower() #Portal requires lower-case
        payload["biosample_term_id"] = btn.accession
        bty = models.BiosampleType(rec.biosample_type_id)
        payload["biosample_type"] = bty.name
        date_biosample_taken = rec.date_biosample_taken
        if date_biosample_taken:
            if bty.name == "tissue":
                payload["date_obtained"] = date_biosample_taken
            else:
                payload["culture_harvest_date"] = date_biosample_taken
        desc = rec.description
        if desc:
            payload["description"] = desc
        lot_id = rec.lot_identifier
        if lot_id:
            payload["lot_id"] = lot_id
        nih_cert = rec.nih_institutional_certification
        if nih_cert:
            payload["nih_institutional_certification"] = nih_cert
        payload["organism"] = "human"
        passage_number = rec.passage_number
        if passage_number:
            payload["passage_number"] = passage_number
        starting_amount = rec.starting_amount
        if starting_amount:
            payload["starting_amount"] = starting_amount
            payload["starting_amount_units"] = models.Unit(rec.starting_amount_units_id).name
        submitter_comment = rec.submitter_comments
        if submitter_comment:
            payload["submitter_comment"] = submitter_comment
        preservation_method = rec.tissue_preservation_method
        if preservation_method:
            payload["preservation_method"] = preservation_method
        prod_id = rec.vendor_product_identifier
        if prod_id:
            payload["product_id"] = prod_id
    
        cm_id = rec.crispr_modification_id
        if cm_id:
            cm = models.CrisprModification(cm_id)
            cm_upstream = self.get_upstream_id(cm) 
            if not cm_upstream:
                cm_upstream = self.post_crispr_modification(cm_id)
            payload["genetic_modifications"] = cm_upstream
    
        doc_ids = rec.document_ids
        if doc_ids:
            docs = [models.Document(d) for d in doc_ids]
            doc_upstreams = []
            for doc in doc:
                doc_upstream = self.get_upstream_id(doc) 
                if not doc_upstream:
                    doc_upstream = self.post_document(doc.id)
                doc_upstreams.append(doc_upstream)
            payload["documents"] = doc_upstreams
    
        donor = models.Donor(rec.donor_id)
        donor_upstream = self.get_upstream_id(donor) 
        if not donor_upstream:
            raise Exception("Donor '{}' of biosample '{}' does not have its upstream set. Donors must be registered with the DCC directly.".format(donor.id, rec_id))
        payload["donor"] = donor_upstream
    
        part_of_biosample_id = rec.part_of_id
        if part_of_biosample_id:
            part_of_biosample = models.Biosample(part_of_biosample_id)
            pob_upstream = self.get_upstream_id(part_of_biosample) 
            if not pob_upstream:
                pob_upstream = self.post_biosample(part_of_biosample_id)
            payload["part_of"] = pob_upstream
    
        pooled_from_biosample_ids = rec.pooled_from_biosample_ids
        if pooled_from_biosample_ids:
            pooled_from_biosamples = [models.Biosample(b) for p in pooled_from_biosample_ids]
            payload["pooled_from"] = []
            for p in pooled_from_biosamples:
                p_upstream = self.get_upstream_id(p) 
                if not p_upstream:
                    p_upstream = self.post_biosample(p.id)
                payload["pooled_from"].append(p_upstream)
    
        vendor = models.Vendor(rec.vendor_id)
        vendor_upstream = self.get_upstream_id(vendor) 
        if not vendor_upstream:
            raise Exception("Biosample '{}' has a vendor without an upstream set: Vendors are requied to be registered by the DCC personel, and Pulsar needs to have the Vendor record's '{}' attribute set.".format(rec_id, self.UPSTREAM_ATTR))
        payload["source"] = vendor_upstream
    
        treatment_ids = rec.treatment_ids
        treat_upstreams = []
        if treatment_ids:
            treatments = [models.Treatment(t) for t in treatment_ids]
            treat_upstreams = []
            for treat in treatments:
                treat_upstream = self.get_upstream_id(treat)
                if not treat_upstream:
                    treat_upstream = self.post_treatment(treat.id)
                treat_upstreams.append(treat_upstream)
            payload["treatments"] = treat_upstreams
   
        if patch:  
            res = self.patch(payload=payload)
        else:
            res = self.post(payload=payload, dcc_profile="biosample", pulsar_model=models.Biosample, pulsar_rec_id=rec_id)
        return res

    def post_library(self, rec_id, patch=False):
        """
        This method will check whether the biosample associated to this library is submitted. If it
        isn't, it will first submit the biosample. 

        Args:
        """
        rec = models.Library(rec_id)
        aliases = []
        aliases.append(rec.abbrev_id())
        name = rec.name
        if name: 
          aliases.append(self.clean_name(name))
        payload = {}
        payload["aliases"] = aliases
        payload[self.UPSTREAM_ATTR] = self.get_upstream_id(rec)
        biosample = models.Biosample(rec.biosample_id)
        # If this Library record is a SingleCellSorting.library_prototype, then the Biosample it will
        # be linked to is the SingleCellSorting.sorting_biosample.
        biosample_upstream = self.get_upstream_id(biosample) 
        if not biosample_upstream:
            biosample_upstream = self.post_biosample(rec_id=rec.biosample_id, patch=False)
        payload["biosample"] = biosample_upstream
        doc_ids = rec.document_ids
        docs = [models.Document(d) for d in doc_ids]
        doc_upstreams = []
        for d in docs:
            upstream = self.get_upstream_id(d) 
            if not upstream:
                upstream = self.post_document(rec_id=d.id, patch=False)
                doc_upstreams.append(upstream)
        if doc_upstreams:
            payload["documents"] = doc_upstreams
        fragmentation_method_id = rec.library_fragmentation_method_id
        if fragmentation_method_id:
            fragmentation_method = models.LibraryFragmentationMethod(fragmentation_method_id).name
            payload["fragmentation_method"] = fragmentation_method.name
        payload["lot_id"] = rec.lot_identifier
        payload["nucleic_acid_term_name"] = models.NucleicAcidTerm(rec.nucleic_acid_term_id).name
        payload["product_id"] = rec.vendor_product_identifier
        payload["size_range"] = rec.size_range
        payload["strand_specificity"] = rec.strand_specific
        vendor_id = rec.vendor_id
        if vendor_id:
            vendor_upstream = self.get_upstream_id(vendor_id)
            if not vendor_upstream:
                vendor_upstream = self.post_vendor(rec_id=vendor_id)
            payload["source"] = vendor_upstream
        ssc_id = rec.single_cell_sorting_id
        if ssc_id:
           barcode_details = self.get_barcode_details_for_ssc(ssc_id=ssc_id)
           payload["barcode_details"] = barcode_details

        # Submit payload
        if patch:  
            res = self.patch(payload=payload)
        else:
            res = self.post(payload=payload, dcc_profile="library", pulsar_model=models.Biosample, pulsar_rec_id=rec_id)
        return res

    def post_replicate(self, library_upstream, patch=False):
        """
        Todo: Check what value to use for technical_replicate_number. 
        Args:
            library_upstream - The identifier of a Library record on the ENCODE Portal, which is
                 stored in Pulsar via the upstream_identifier attribute of a Library record.
        """
        # Required fields to submit to a replicate are:
        #  -biological_replicate_number
        #  -experiment
        #  -technical_replicate_number

        #dcc_lib = self.ENC_CONN.get(ignore404=False, rec_ids=dcc_library_id)       
        lib = models.Library(upstream=library_upstream})
        biosample_id = lib.biosample_id
        biosample = models.Biosample(biosample_id)
        payload = {}
        payload["antibody"] = "ENCAB728YTO" #AB-9 in Pulsar
        #payload["aliases"] = 
        # Set biological_replicate_number and technical_replicate_number. For ssATAC-seq experiments,
        # these two attributes don't really make sense, but they are required to submit, so ...
        brn = biosample.replicate_number
        if not brn:
            # Check if this is a SingleCellSorting.library_prototype - if so, replicate_number isn't
            # really meaningful in Pulsar for the linked biosample, just default it to 1:
            if lib.single_cell_sorting_id:
                brn = 1
            else:
               raise Exception("Can't submit replicate object for library '{}' since the associated biosample doesn't have the replicate_number attribute set.".format(library_upstream))
        payload["biological_replicate_number"] = brn
        payload["technical_replicate_number"] = 1
        # Figure out the experiment that this Biosample is associated to
        exp_rec = pulsarpy.utils.get_exp_of_biosample(biosample)
        if not exp_rec.upstream_identifier:
            raise Exception("Can't submit replicate when the experiment it is linked to hasn't been submitted.")
        payload["experiment"] = experiment_upstream
        payload["library"] = library_upstream
        # Submit payload
        if patch:  
            res = self.patch(payload=payload)
        else:
            res = self.post(payload=payload, dcc_profile="replicate", pulsar_model=models.Biosample, pulsar_rec_id=rec_id)
        #return res
        return payload

    def post_file(self, pulsar_sres_id, pulsar patch=False):
        """
        Creates a file record on the ENCODE Portal. 

    def get_barcode_details_for_ssc(self, ssc_id):
        """
        This purpose of this method is to provide a value to the library.barcode_details property
        of the Library profile on the ENCODE Portal. That property taks an array of objects whose
        properties are the 'barcode', 'plate_id', and 'plate_location'. 

        Args:
            ssc_id: The Pulsar ID for a SingleCellSorting record.
        """
        ssc = models.SingleCellSorting(ssc_id)
        lib_prototype_id = ssc.library_prototype_id
        lib = models.Library(lib_prototype_id)
        paired_end = lib.paired_end
        plate_ids = ssc.plate_ids
        plates = [models.Plate(p) for p in plate_ids]
        results = []
        for p in plates:
            for well_id in p.well_ids:
                well = models.Well(well_id)
                details = {}
                details["plate_id"] = p.name
                details["plate_location"] = well.name
                well_biosample = models.Biosample(well.biosample_id)
                lib_id = well_biosample.library_ids[-1]
                # Doesn't make sense to have more than one library for single cell experiments. 
                lib = models.Library(lib_id)
                if not paired_end:
                    barcode_id = lib.barcode_id
                    barcode = models.Barcode(barcode_id).sequence
                else:
                    pbc_id = lib.paired_barcode_id
                    pbc = models.PairedBarcode(pbc_id)
                    barcode = pbc.index1["sequence"] + "-" + pbc.index2["sequence"]
                details["barcode"] = barcode
                results.append(details)
        return results

    def post_single_cell_sorting(self, rec_id, patch=False):
        rec = models.SingleCellSorting(rec_id)
        aliases = []
        aliases.append(rec.abbrev_id())
        name = rec.name
        if name: 
          aliases.append(self.clean_name(name))
        sorting_biosample_id = rec.sorting_biosample_id
        sorting_biosample = models.Biosample(sorting_biosample_id)
        payload = {}
        # Set the explicitly required properties first:
        payload["aliases"] = aliases
        payload["assay_term_name"] = "single-cell ATAC-seq"
        payload["biosample_type"] = sorting_biosample["biosample_type"]["name"]
        payload["experiment_classification"] = "functional genomics"
        # And now the rest
        payload["biosample_term_name"] = sorting_biosample["biosample_term_name"]["name"]
        payload["biosmple_term_id"] = sorting_biosample["biosample_term_name"]["accession"]
        payload["description"] = rec.description
        docs = rec.documents
        doc_upstreams = []
        for d in docs:
            upstream = self.post_document(rec_id=d)
            doc_upstreams.append(upstream)
        payload["documents"] = doc_upstreams

        # Submit biosample
        self.post_biosample(rec_id=sorting_biosample.id, patch=patch)
        # Submit library_prototype (linked to sorting_biosample)
        library_prototype_id = rec.library_prototype_id
        library_upstream = self.post_library(rec_id=library_prototype_id, patch=patch)
        # Submit replicate
        self.post_replicate(library_upstream=library_upstream, patch=patch)
