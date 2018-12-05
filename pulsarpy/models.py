# -*- coding: utf-8 -*-

# Author
# Nathaniel Watson
# 2017-09-18
# nathankw@stanford.edu
###

# pip install reflection.
# Ported from RoR's inflector.
# See https://inflection.readthedocs.io/en/latest/.
"""
A client that contains classes named after each model in Pulsar to handle RESTful communication with
the Pulsar API.
"""

import base64
from importlib import import_module
import inflection
import json
import logging
import os
import requests
import urllib3
import pdb

import pulsarpy as p
import pulsarpy.elasticsearch_utils

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

THIS_MODULE = import_module(__name__)
# Note that using the json param in a HTTP request via the requests module will cause the
# header 'content-type': 'application/json' to be set. Nonetheless, I'll explicitly set it here in case
# the data parameter is ever mistakingly used instead of the json one.
HEADERS = {'accept': 'application/json', 'content-type': 'application/json', 'Authorization': 'Token token={}'.format(p.API_TOKEN)}

# Curl Examples
#
# 1) Create a construct tag:
#
#    curl -X POST
#       -d "construct_tags[name]=plasmid3"
#       -H "Accept: application/json"
#       -H "Authorization: Token token=${API_TOKEN}" http://localhost:3000/api/construct_tags
#
# 2) Update the construct tag with ID of 3:
#
#     curl -X PUT
#       -d "construct_tag[name]=AMP"
#       -H "Accept: application/json"
#       -H "Authorization: Token token=${API_TOKEN}" http://localhost:3000/api/construct_tags/3"
#
# 2) Get a construct_tag:
#
#     curl -H "Accept: application/json"
#        -H "Authorization: Token token=${API_TOKEN}" http://localhost:3000/api/construct_tags/5
###

# Python examples using the 'requests' module
#
# HEADERS = {'content-type': 'application/json', 'Authorization': 'Token token={}'.format(API_TOKEN)}
# URL="http://localhost:3000/api/construct_tags"
# 1) Call 'index' method of a construct_tag:
#
#    requests.get(url=URL,headers=HEADERS, verify=False)
#
#  2) Call 'show' method of a construct_tags
#
#    >>>i requests.get(url=URL + "/1",headers=HEADERS, verify=False)
#
# 3) Create a new construct_tag
#
#    payload = {'name': 'test_tag_ampc', 'description': "C'est bcp + qu'un simple ..."}
#    r = requests.post(url=url, headers=HEADERS, verify=False, data=json.dumps({"construct_tag": {"name": "nom"}}))


class RecordNotFound(Exception):
    """"
    Raised when looking up a record by name, id, etc. and it isn't found on the server.
    """


class RecordNotUnique(Exception):
    """
    Raised when posting a record and the Rails server returns with the exception ActiveRecord::RecordNotUnique.
    """


def remove_model_prefix(uid):
    """
    Removes the optional model prefix from the given primary ID. For example, given the biosample
    record whose primary ID is 8, and given the fact that the model prefix for the Biosample model
    is "B-", the record ID can be specified either as 8 or B-8. However, the model prefix needs to
    be stripped off prior to making API calls.
    """

    return str(uid).split("-")[-1]

def get_model_attrs(model_name):
    url = os.path.join(p.URL, "utils/model_attrs")
    payload = {"model_name": model_name}
    response = requests.get(url=url,headers=HEADERS, verify=False, json=payload)
    response.raise_for_status()
    return response.json()

class Meta(type):
    @staticmethod
    def get_logfile_name(tag):
        """
        Creates a name for a log file that is meant to be used in a call to
        ``logging.FileHandler``. The log file name will incldue the path to the log directory given
        by the `p.LOG_DIR` constant. The format of the file name is: 'log_$HOST_$TAG.txt', where

        $HOST is the hostname part of the URL given by ``URL``, and $TAG is the value of the
        'tag' argument. The log directory will be created if need be.

        Args:
            tag: `str`. A tag name to add to at the end of the log file name for clarity on the
                log file's purpose.
        """
        if not os.path.exists(p.LOG_DIR):
            os.mkdir(p.LOG_DIR)
        filename = "log_" + p.HOST + "_" + tag + ".txt"
        filename = os.path.join(p.LOG_DIR, filename)
        return filename

    @staticmethod
    def add_file_handler(logger, level, tag):
        """
        Adds a ``logging.FileHandler`` handler to the specified ``logging`` instance that will log
        the messages it receives at the specified error level or greater.  The log file name will
        be of the form log_$HOST_$TAG.txt, where $HOST is the hostname part of the URL given
        by ``p.URL``, and $TAG is the value of the 'tag' argument.

        Args:
            logger: The `logging.Logger` instance to add the `logging.FileHandler` to.
            level:  `int`. A logging level (i.e. given by one of the constants `logging.DEBUG`,
                `logging.INFO`, `logging.WARNING`, `logging.ERROR`, `logging.CRITICAL`).
            tag: `str`. A tag name to add to at the end of the log file name for clarity on the
                log file's purpose.
        """
        f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')
        filename = Meta.get_logfile_name(tag)
        handler = logging.FileHandler(filename=filename, mode="a")
        handler.setLevel(level)
        handler.setFormatter(f_formatter)
        logger.addHandler(handler)

    def __init__(newcls, classname, supers, classdict):
        #: Used primarily for setting the lower-cased and underscored model name in the payload
        #: for post and patch operations.
        newcls.MODEL_NAME = inflection.underscore(newcls.__name__)
        #: Elasticsearch index name for the Pulsar model.
        newcls.ES_INDEX_NAME = inflection.pluralize(newcls.MODEL_NAME)
        newcls.URL = os.path.join(p.URL, inflection.pluralize(newcls.MODEL_NAME))


class Model(metaclass=Meta):
    """
    The superclass of all model classes. A model subclass is defined for each Rails model.
    An instance of a model class represents a record of the given Rails model.

    Subclasses don't typically define their own init method, but if they do, they need to make a call
    to 'super' to run the init method defined here as well.

    Subclasses must be instantiated with the rec_id argument set to a record's ID. A GET will
    immediately be done and the record's attributes will be stored in the self.attrs `dict`.
    The record's attributes can be accessed as normal instance attributes (i.e. ``record.name) rather than explicitly
    indexing the attrs dictionary, thanks to the employment of ``__getattr__()``. Similarly, record
    attributes can be updated via normal assignment operations, i.e. (``record.name = "bob"``),
    thanks to employment of ``__setattr__()``.

    Required Environment Variables:
        1) PULSAR_API_URL
        2) PULSAR_TOKEN
    """
    #: Most models have an attribute alled upstream_identifier that is used to store the value of the
    #: record in an "upstream" database that is submitted to, i.e. the ENCODE Portal. Not all models
    #: have this attribute since not all are used for submission to an upstream portal.
    UPSTREAM_ATTR = "upstream_identifier"
    MODEL_ABBR = ""  # subclasses define

    #: Abstract attribute of type `dict` that each subclass should fulfull if it has any foreign keys.
    #: Each key is a foreign key name and the value is the class name of the model it refers to.
    FKEY_MAP = {}

    #: This class adds a file handler, such that all messages sent to it are logged to this
    #: file in addition to STDOUT.
    debug_logger = logging.getLogger(p.DEBUG_LOGGER_NAME)

    # Add debug file handler to debug_logger:
    Meta.add_file_handler(logger=debug_logger, level=logging.DEBUG, tag="debug")

    #: A ``logging`` instance with a file handler for logging terse error messages.
    #: The log file resides locally within the directory specified by the constant
    #: ``p.LOG_DIR``. Accepts messages >= ``logging.ERROR``.
    error_logger = logging.getLogger(p.ERROR_LOGGER_NAME)
    log_level = logging.ERROR
    error_logger.setLevel(log_level)
    Meta.add_file_handler(logger=error_logger, level=log_level, tag="error")

    #: A ``logging`` instance with a file handler for logging successful POST operations.
    #: The log file resides locally within the directory specified by the constant
    #: ``p.LOG_DIR``. Accepts messages >= ``logging.INFO``.
    post_logger = logging.getLogger(p.POST_LOGGER_NAME)
    log_level = logging.INFO
    post_logger.setLevel(log_level)
    Meta.add_file_handler(logger=post_logger, level=log_level, tag="posted")
    log_msg = "-----------------------------------------------------------------------------------"

    # Check if neccessary environment variables are set:
    if not p.URL:
        debug_logger.debug("Warning: Environment variable PULSAR_API_URL not set.")
    elif not p.API_TOKEN:
        debug_logger.debug("Warning: Environment variable PULSAR_TOKEN not set.")
    debug_logger.debug(log_msg)
    error_logger.error(log_msg)
    log_msg = "Connecting to {}".format(p.URL)
    error_logger.error(log_msg)
    debug_logger.debug(log_msg)

    #: Connection to Elasticsearch. Expects that the envrionment variables ES_URL, ES_USER, and
    #: ES_PW are set, which signifiy the Elasticsearch cluster URL, login username and login
    #: password, respectively.
    ES = pulsarpy.elasticsearch_utils.Connection()

    def __init__(self, uid=None, upstream=None):
        """
        Find the record of the given model specified by self.MODEL_NAME. The record can be looked up
        in a few ways, depending on which argument is specified (uid or upstream). If both are specified,
        then the upstream argument will be ignored.

        Args:
            uid: The database identifier of the record to fetch, which can be specified either as the
                primary id (i.e. 8) or the model prefix plus the primary id (i.e. B-8).
                Could also be the record's name if it has a name attribute (not all models do)
                and if so will be converted to the record ID.
            upstream: If set, then the record will be searched on its upstream_identifier attribute.
        """
        # self.attrs will store the actual record's attributes. Initialize value now to empty dict
        # since it is expected to be set already in self.__setattr__().
        self.__dict__["attrs"] = {}

        # rec_id could be the record's name. Check for that scenario, and convert to record ID if
        # necessary.
        if uid:
            rec_id = self.__class__.replace_name_with_id(uid)
            rec_json = self._get(rec_id=rec_id)
        elif upstream:
            rec_json = self._get(upstream=upstream)
        else:
            raise ValueError("Either the 'uid' or 'upstream' parameter must be set.")
        self.rec_id = rec_json["id"]
        self.__dict__["attrs"] = rec_json #avoid call to self.__setitem__() for this attr.

    def __getattr__(self, name):
        """
        Treats database attributes for the record as Python attributes. An attribute is looked up
        in self.attrs.
        """
        return self.attrs[name]

    def __setattr__(self, name, value):
        """
        Sets the value of an attribute in self.attrs.
        """
        if name not in self.attrs:
            return object.__setattr__(self, name, value)
        object.__setattr__(self, self.attrs[name], value)
        #self.__dict__["attrs"][name] = value #this works too


    def _get(self, rec_id=None, upstream=None):
        """
        Fetches a record by the record's ID or upstream_identifier.

        Raises:
            `pulsarpy.models.RecordNotFound`: A record could not be found.
        """
        if rec_id:
            record_url = os.path.join(self.URL, str(rec_id))
            self.debug_logger.debug("GET {} record with ID {}: {}".format(self.__class__.__name__, rec_id, record_url))
            response = requests.get(url=record_url, headers=HEADERS, verify=False)
            if not response.ok and response.status_code == requests.codes.NOT_FOUND:
                raise RecordNotFound("Search for {} record with ID '{}' returned no results.".format(self.__class__.__name__, rec_id))
            self.write_response_html_to_file(response,"get_bob.html")
            response.raise_for_status()
            return response.json()
        elif upstream:
            rec_json = self.__class__.find_by({"upstream_identifier": upstream}, require=True)
        return rec_json

    @classmethod
    def log_post(cls, res_json):
        msg = cls.__name__ + "\t" + str(res_json["id"]) + "\t"
        name = res_json.get("name")
        if name:
            msg += name
        cls.post_logger.info(msg)

    @classmethod
    def replace_name_with_id(cls, name):
        """
        Used to replace a foreign key reference using a name with an ID. Works by searching the
        record in Pulsar and expects to find exactly one hit. First, will check if the foreign key
        reference is an integer value and if so, returns that as it is presumed to be the foreign key.

        Raises:
            `pulsarpy.elasticsearch_utils.MultipleHitsException`: Multiple hits were returned from the name search.
            `pulsarpy.models.RecordNotFound`: No results were produced from the name search.
        """
        try:
            int(name)
            return name #Already a presumed ID.
        except ValueError:
            #Not an int, so maybe a name.
            try:
                result = cls.ES.get_record_by_name(cls.ES_INDEX_NAME, name)
                if result:
                    return result["id"]
            except pulsarpy.elasticsearch_utils.MultipleHitsException as e:
                raise
            raise RecordNotFound("Name '{}' for model '{}' not found.".format(name, cls.__name__))


    @classmethod
    def add_model_name_to_payload(cls, payload):
        """
        Checks whether the model name in question is in the payload. If not, the entire payload
        is set as a value of a key by the name of the model.  This method is useful when some
        server-side Rails API calls expect the parameters to include the parameterized model name.
        For example, server-side endpoints that handle the updating of a biosample record or the
        creation of a new biosmample record will expect the payload to be of the form::

            { "biosample": {
                "name": "new biosample",
                "donor": 3,
                ...
               }
            }

        Args:
            payload: `dict`. The data to send in an HTTP request.

        Returns:
            `dict`.
        """
        if not cls.MODEL_NAME in payload:
            payload = {cls.MODEL_NAME: payload}
        return payload

    @staticmethod
    def check_boolean_fields(payload):
        for key in payload:
            val = payload[key]
            if type(val) != str:
                continue
            val = val.lower()
            if val in ["yes", "true"]:
                val = True
                payload[key] = val
            elif val == ["no",  "false"]:
                val = False
                payload[key] = val
        return payload

    def get_upstream(self):
        return self.attrs.get(Model.UPSTREAM_ATTR)

    def abbrev_id(self):
        """
        This method is called when posting to the ENCODE Portal to grab an alias for the record
        being submitted. The alias here is composed of the record ID in Pulsar (i.e. B-1 for the Biosample
        with ID 1). However, the record ID is prefexed with a 'p' to designate that this record was
        submitted from Pulsar. This is used to generate a unique alias considering that we used to
        uses a different LIMS (Syapse) to submit records.  Many of the models in Syapse used the same
        model prefix as is used in Pulsar, i.e. (B)Biosample and (L)Library. Thus, w/o the 'p' prefix,
        the same alias could be generated in Pulsar as a previous one used in Syapse.
        """
        pulsar_lims_prefix = "p"
        return pulsar_lims_prefix +  self.MODEL_ABBR + "-" + str(self.id)

    def delete(self):
        """Deletes the record.
        """
        res = requests.delete(url=self.record_url, headers=HEADERS, verify=False)
        #self.write_response_html_to_file(res,"bob_delete.html")
        if res.status_code == 204:
            #No content. Can't render json:
            return {}
        return res.json()

    @classmethod
    def find_by(cls, payload, require=False):
        """
        Searches the model in question by AND joining the query parameters.

        Implements a Railsy way of looking for a record using a method by the same name and passing
        in the query as a dict. as well. Only the first hit is returned, and there is no particular
        ordering specified in the server-side API method.

        Args:
            payload: `dict`. The attributes of a record to restrict the search to.
            require: `bool`. True means to raise a `pulsarpy.models.RecordNotFound` exception if no
                record is found.

        Returns:
            `dict`: The JSON serialization of the record, if any, found by the API call.
            `None`: If the API call didnt' return any results and the `found` parameter is False.

        Raises:
            `pulsarpy.models.RecordNotFound`: No records were found, and the `require` parameter is
                True.
        """
        if not isinstance(payload, dict):
            raise ValueError("The 'payload' parameter must be provided a dictionary object.")
        url = os.path.join(cls.URL, "find_by")
        payload = {"find_by": payload}
        cls.debug_logger.debug("Searching Pulsar {} for {}".format(cls.__name__, json.dumps(payload, indent=4)))
        res = requests.post(url=url, json=payload, headers=HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()
        res_json = res.json()
        if res_json:
           try:
               res_json = res_json[cls.MODEL_NAME]
           except KeyError:
               # Key won't be present if there isn't a serializer for it on the server.
               pass
        else:
            if require:
                raise RecordNotFound("Can't find any {} records with search criteria: '{}'.".format(cls.__name__, payload))
        return res_json

    @classmethod
    def find_by_or(cls, payload):
        """
        Searches the model in question by OR joining the query parameters.

        Implements a Railsy way of looking for a record using a method by the same name and passing
        in the query as a string (for the OR operator joining to be specified).

        Only the first hit is returned, and there is not particular ordering specified in the server-side
        API method.

        Args:
            payload: `dict`. The attributes of a record to search for by using OR operator joining
                for each query parameter.

        Returns:
            `dict`: The JSON serialization of the record, if any, found by the API call.
            `None`: If the API call didnt' return any results.
        """
        if not isinstance(payload, dict):
            raise ValueError("The 'payload' parameter must be provided a dictionary object.")
        url = os.path.join(cls.URL, "find_by_or")
        payload = {"find_by_or": payload}
        cls.debug_logger.debug("Searching Pulsar {} for {}".format(cls.__name__, json.dumps(payload, indent=4)))
        res = requests.post(url=url, json=payload, headers=HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        if res:
           try:
               res = res[cls.MODEL_NAME]
           except KeyError:
               # Key won't be present if there isn't a serializer for it on the server.
               pass
        return res

    @classmethod
    def index(cls):
        """Fetches all records.

        Returns:
            `dict`. The JSON formatted response.

        Raises:
            `requests.exceptions.HTTPError`: The status code is not ok.
        """
        res = requests.get(cls.URL, headers=HEADERS, verify=False)
        res.raise_for_status()
        return res.json()

    def patch(self, payload, append_to_arrays=True):
        """
        Patches current record and udpates the current instance's 'attrs'
        attribute to reflect the new changes.

        Args:
            payload - hash. This will be JSON-formatted prior to sending the request.

        Returns:
            `dict`. The JSON formatted response.

        Raises:
            `requests.exceptions.HTTPError`: The status code is not ok.
        """
        if not isinstance(payload, dict):
            raise ValueError("The 'payload' parameter must be provided a dictionary object.")
        payload = self.__class__.set_id_in_fkeys(payload)
        if append_to_arrays:
            for key in payload:
                val = payload[key]
                if type(val) == list:
                    val.extend(getattr(self, key))
                    payload[key] = list(set(val))
        payload = self.check_boolean_fields(payload)
        payload = self.__class__.add_model_name_to_payload(payload)
        self.debug_logger.debug("PATCHING payload {}".format(json.dumps(payload, indent=4)))
        res = requests.patch(url=self.record_url, json=payload, headers=HEADERS, verify=False)
        self.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()
        json_res = res.json()
        self.attrs = json_res
        return json_res

    @classmethod
    def set_id_in_fkeys(cls, payload):
        """
        Looks for any keys in the payload that end with either _id or _ids, signaling a foreign
        key field. For each foreign key field, checks whether the value is using the name of the
        record or the acutal primary ID of the record (which may include the model abbreviation, i.e.
        B-1). If the former case, the name is replaced with
        the record's primary ID.

        Args:
            payload: `dict`. The payload to POST or PATCH.

        Returns:
            `dict`. The payload.
        """

        for key in payload:
            val = payload[key]
            if not val:
               continue
            if key.endswith("_id"):
                model = getattr(THIS_MODULE, cls.FKEY_MAP[key])
                rec_id = model.replace_name_with_id(name=val)
                payload[key] = rec_id
            elif key.endswith("_ids"):
                model = getattr(THIS_MODULE, cls.FKEY_MAP[key])
                rec_ids = []
                for v in val:
                   rec_id = model.replace_name_with_id(name=v)
                   rec_ids.append(rec_id)
                payload[key] = rec_ids
        return payload

    @classmethod
    def prepost_hooks(cls, payload):
        return payload


    @classmethod
    def post(cls, payload):
        """Posts the data to the specified record.

        Args:
            payload: `dict`. This will be JSON-formatted prior to sending the request.

        Returns:
            `dict`. The JSON formatted response.

        Raises:
            `Requests.exceptions.HTTPError`: The status code is not ok.
            `RecordNotUnique`: The Rails server returned the exception ActiveRecord::RecordNotUnique.
        """
        if not isinstance(payload, dict):
            raise ValueError("The 'payload' parameter must be provided a dictionary object.")
        payload = cls.set_id_in_fkeys(payload)
        payload = cls.check_boolean_fields(payload)
        payload = cls.add_model_name_to_payload(payload)
        # Run any pre-post hooks:
        payload = cls.prepost_hooks(payload)
        cls.debug_logger.debug("POSTING payload {}".format(json.dumps(payload, indent=4)))
        res = requests.post(url=cls.URL, json=(payload), headers=HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        if not res.ok:
            cls.log_error(res.text)
            res_json = res.json()
            if "exception" in res_json:
                exc_type = res_json["exception"]
                if exc_type == "ActiveRecord::RecordNotUnique":
                    raise RecordNotUnique()
        res.raise_for_status()
        res = res.json()
        cls.log_post(res)
        return res

    @classmethod
    def log_error(cls, msg):
        """
        Logs the provided error message to both the error logger and the debug logger logging
        instances.

        Args:
            msg: `str`. The error message to log.
        """
        cls.error_logger.error(msg)
        cls.debug_logger.debug(msg)

    @staticmethod
    def write_response_html_to_file(response,filename):
        """
        An aid in troubleshooting internal application errors, i.e.  <Response [500]>, to be mainly
        beneficial when developing the server-side API. This method will write the response HTML
        for viewing the error details in the browesr.

        Args:
            response: `requests.models.Response` instance.
            filename: `str`. The output file name.
        """
        fout = open(filename,'w')
        fout.write(response.text)
        fout.close()

class Address(Model):
    MODEL_ABBR = "AD"


class Antibody(Model):
    MODEL_ABBR = "AB"


class AntibodyPurification(Model):
    MODEL_ABBR = "AP"


class Barcode(Model):
    MODEL_ABBR = "BC"


class Biosample(Model):
    MODEL_ABBR = "B"
    FKEY_MAP = {}
    FKEY_MAP["biosample_term_name_id"] = "BiosampleTermName"
    FKEY_MAP["biosample_type_id"] = "BiosampleType"
    FKEY_MAP["chipseq_experiment_id"] = "ChipseqExperiment"
    FKEY_MAP["crispr_modification_id"] = "CrisprModification"
    FKEY_MAP["donor_id"] = "Donor"
    FKEY_MAP["owner_id"] = "Owner"
    FKEY_MAP["part_of_id"] = "Biosample"
    FKEY_MAP["transfected_by_id"] = "User"
    FKEY_MAP["vendor_id"] = "Vendor"
    FKEY_MAP["document_ids"] = "Document"
    FKEY_MAP["pooled_from_biosample_ids"] = "Biosample"
    FKEY_MAP["treatment_ids"] = "Treatment"


class BiosampleOntology(Model):
    MODEL_ABBR = "BO"

class BiosampleTermName(Model):
    MODEL_ABBR = "BTN"


class BiosampleType(Model):
    MODEL_ABBR = "BTY"

class ChipBatch(Model):
    MODEL_ABBR = "CB"
    FKEY_MAP = {}
    FKEY_MAP["user_id"] = "User"
    FKEY_MAP["analyst_id"] = "User"
    FKEY_MAP["chip_batch_item_ids"] = "ChipBatch"

class ChipBatchItem(Model):
    MODEL_ABBR = "CBI"
    FKEY_MAP = {}
    FKEY_MAP["user_id"] = "User"
    FKEY_MAP["biosample_id"] = "Biosample"
    FKEY_MAP["chip_batch_id"] = "ChipBatch"
    FKEY_MAP["concentration_unit_id"] = "Unit"

class ChipseqExperiment(Model):
    MODEL_ABBR = "CS"
    FKEY_MAP = {}
    FKEY_MAP["document_ids"] = "Document"
    FKEY_MAP["control_replicate_ids"] = "Biosample"
    FKEY_MAP["replicate_ids"] = "Biosample"
    FKEY_MAP["starting_biosample_id"] = "Biosample"
    FKEY_MAP["target_id"] = "Target"
    FKEY_MAP["user_id"] = "User"
    FKEY_MAP["wild_type_control_id"] = "Biosample"

    def control_map(self):
        """
        Creates a dict. where each key is the ID of a non-control Biosample record on the 
        ChipseqExperiment, and each value is the 

        Returns:
            `dict`. 
        """
        pass


class DataStorage(Model):
    MODEL_ABBR = "DS"
    FKEY_MAP = {}
    FKEY_MAP["data_storage_provider_id"] = "DataStorageProvider"


class DataStorageProvider(Model):
    MODEL_ABBR = "DSP"


class Document(Model):
    MODEL_ABBR = "DOC"


class DocumentType(Model):
    MODEL_ABBR = "DOCTY"


class Unit(Model):
    MODEL_ABBR = "UN"


class ConstructTag(Model):
    MODEL_ABBR = "CT"


class CrisprConstruct(Model):
    MODEL_ABBR = "CC"


class CrisprModification(Model):
    MODEL_ABBR = "CRISPR"
    FKEY_MAP = {}
    FKEY_MAP["biosample_ids"] = "Biosample"
    FKEY_MAP["crispr_construct_ids"] = "CrisprConstruct"
    FKEY_MAP["document_ids"] = "Document"
    FKEY_MAP["donor_construct_id"] = "DonorConstruct"
    FKEY_MAP["from_prototype_id"] = "CrisprModification"
    FKEY_MAP["part_of_id"] = "CrisprModification"
    FKEY_MAP["target_id"] = "Target"
    FKEY_MAP["user_id"] = "User"

    def clone(self, biosample_id):
       biosample_id = self.__class__.replace_name_with_id(name=biosample_id)
       url = self.record_url +  "/clone"
       self.debug_logger.debug("Cloning with URL {}".format(url))
       payload = {"biosample_id": biosample_id}
       res = requests.post(url=url, json=payload, headers=HEADERS, verify=False)
       res.raise_for_status()
       self.write_response_html_to_file(res,"bob.html")
       self.debug_logger.debug("Cloned GeneticModification {}".format(self.rec_id))
       return res.json()


class Donor(Model):
    MODEL_ABBR = "DON"


class DonorConstruct(Model):
    MODEL_ABBR = "DONC"


class Document(Model):
    MODEL_ABBR = "DOC"

    def download(self):
        # The sever is Base64 encoding the payload, so we'll need to base64 decode it.
        url = self.record_url + "/download"
        res = requests.get(url=url, headers=HEADERS, verify=False)
        res.raise_for_status()
        data = base64.b64decode(res.json()["data"])
        return data


class FileReference(Model):
    MODEL_ABBR = "FR"


class Library(Model):
    MODEL_ABBR = "L"
    FKEY_MAP = {}
    # belongs_to/ has_one
    FKEY_MAP["barcode_id"] = "Barcode"
    FKEY_MAP["biosample_id"] = "Biosample"
    FKEY_MAP["concentration_unit_id"] = "Unit"
    FKEY_MAP["from_prototype_id"] = "Library"
    FKEY_MAP["library_fragmentation_method_id"] = "LibraryFragmentationMethod"
    FKEY_MAP["nucleic_acid_term_id"] = "NucleicAcidTerm"
    FKEY_MAP["paired_barcode_id"] = "PairedBarcode"
    FKEY_MAP["sequencing_library_prep_kit_id"] = "SequencingLibraryPrepKit"
    FKEY_MAP["sequencing_request_ids"] = "SequencingRequest"
    FKEY_MAP["single_cell_sorting_id"] = "SingleCellSorting"
    FKEY_MAP["user_id"] = "User"
    FKEY_MAP["vendor_id"] = "Vendor"
    FKEY_MAP["well_id"] = "Well"
    # has_many
    FKEY_MAP["document_ids"] = "Document"

    def get_barcode_sequence(self):
        if self.barcode_id:
            return Barcode(self.barcode_id).sequence
        elif self.paired_barcode_id:
            return PairedBarcode(self.paired_barcode_id).sequence
        return


class LibraryFragmentationMethod(Model):
    MODEL_ABBR = "LFM"


class NucleicAcidTerm(Model):
    MODEL_ABBR = "NAT"


class PairedBarcode(Model):
    MODEL_ABBR = "PBC"


class Plate(Model):
    MODEL_ABBR = "PL"

class SequencingCenter(Model):
    MODEL_ABBR = "SC"

class SequencingLibraryPrepKit(Model):
    MODEL_ABBR = "SLPK"

class SequencingRequest(Model):
    MODEL_ABBR = "SREQ"
    FKEY_MAP = {}
    FKEY_MAP["concentration_unit_id"] = "Unit"
    FKEY_MAP["sequencing_platform_id"] = "SequencingPlatform"
    FKEY_MAP["sequencing_center_id"] = "SequencingCenter"
    FKEY_MAP["submitted_by_id"] = "User"


class SequencingPlatform(Model):
    MODEL_ABBR = "SP"


class SequencingRun(Model):
    MODEL_ABBR = "SRUN"
    FKEY_MAP = {}
    FKEY_MAP["data_storage_id"] = "DataStorage"
    FKEY_MAP["sequencing_request_id"] = "SequencingRequest"
    FKEY_MAP["submitted_by_id"] = "User"

    def library_sequencing_results(self):
        """
        Generates a dict. where each key is a Library ID on the SequencingRequest and each value
        is the associated SequencingResult. Libraries that aren't yet with a SequencingResult are
        not inlcuded in the dict.
        """
        sres_ids = self.sequencing_result_ids
        res = {}
        for i in sres_ids:
            sres = SequencingResult(i)
            res[sres.library_id] = sres
        return res
            
class SequencingResult(Model):
    MODEL_ABBR = "SRES"
    FKEY_MAP = {}
    FKEY_MAP["library_id"] = "Library"
    FKEY_MAP["sequencing_run_id"] = "SequencingRun"
    FKEY_MAP["analysis_ids"] = "SequencingRun"


class Shipping(Model):
    MODEL_ABBR = "SH"
    FKEY_MAP = {}
    FKEY_MAP["biosample_id"] = "Biosample"
    FKEY_MAP["from_id"] = "Address"
    FKEY_MAP["to_id"] = "Address"


class SingleCellSorting(Model):
    MODEL_ABBR = "SCS"
    FKEY_MAP = {}
    FKEY_MAP["analysis_ids"] = "Analysis"
    FKEY_MAP["document_ids"] = "Document"
    FKEY_MAP["library_prototype_id"] = "Library"
    FKEY_MAP["plate_ids"] = "Plate"
    FKEY_MAP["sorting_biosample_id"] = "Biosample"
    FKEY_MAP["starting_biosample"] = "Biosample"
    FKEY_MAP["user_id"] = "User"


class Target(Model):
    MODEL_ABBR = "TRG"
    FKEY_MAP = {}
    FKEY_MAP["user_id"] = "User"
    FKEY_MAP["antibody_ids"] = "Antibody"
    FKEY_MAP["crispr_construct_ids"] = "CrisprConstruct"
    FKEY_MAP["donor_construct_ids"] = "DonorConstruct"


class Treatment(Model):
    MODEL_ABBR = "TRT"


class TreatmentTermName(Model):
    MODEL_ABBR = "TTN"


class User(Model):

    def archive_user(self, user_id):
        """Archives the user with the specified user ID.

        Args:
            user_id: `int`. The ID of the user to archive.

        Returns:
            `NoneType`: None.
        """
        url = self.record_url + "/archive"
        res = requests.patch(url=url, json={"user_id": user_id}, headers=HEADERS, verify=False)
        self.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()

    def unarchive_user(self, user_id):
        """Unarchives the user with the specified user ID.

        Args:
            user_id: `int`. The ID of the user to unarchive.

        Returns:
            `NoneType`: None.
        """
        url = self.record_url + "/unarchive"
        res = requests.patch(url=url, json={"user_id": user_id}, headers=HEADERS, verify=False)
        self.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()

    def generate_api_key(self):
        """
        Generates an API key for the user, replacing any existing one.

        Returns:
            `str`: The new API key.
        """
        url = self.record_url + "/generate_api_key"
        res = requests.patch(url=url, headers=HEADERS, verify=False)
        self.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()
        return res.json()["token"]

    def remove_api_key(self):
        """
        Removes the user's existing API key, if present, and sets the current instance's 'api_key'
        attribute to the empty string.

        Returns:
            `NoneType`: None.
        """
        url = self.record_url + "/remove_api_key"
        res = requests.patch(url=url, headers=HEADERS, verify=False)
        res.raise_for_status()
        self.api_key = ""


class Vendor(Model):
    MODEL_ABBR = "V"


class Well(Model):
    MODEL_ABBR = "WELL"

#if __name__ == "__main__":
    # pdb.set_trace()
    #b = Biosample()
    #res = b.get(uid=1716)
    #res = b.patch(uid=1772,payload={"name": "bobq_a"})
    #res = b.delete(uid=1719)
    #c = ConstructTag()
    #res = c.post(payload={"name": "howdy there partner"})
