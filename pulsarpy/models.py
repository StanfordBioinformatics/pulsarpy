# -*- coding: utf-8 -*-

# Author
# Nathaniel Watson
# 2017-09-18
# nathankw@stanford.edu
###

import os
import json
import requests
import pdb

import inflection

# pip install reflection.
# Ported from RoR's inflector.
# See https://inflection.readthedocs.io/en/latest/.

# Curl Examples
#
# 1) Create a construct tag:
#
#    curl -X POST
#       -d "construct_tags[name]=plasmid3"
#       -H "Accept: application/json"
#       -H "Authorization: Token token=${TOKEN}" http://localhost:3000/api/construct_tags
#
# 2) Update the construct tag with ID of 3:
#
#     curl -X PUT
#       -d "construct_tag[name]=AMP"
#       -H "Accept: application/json"
#       -H "Authorization: Token token=${TOKEN}" http://localhost:3000/api/construct_tags/3"
#
# 2) Get a construct_tag:
#
#     curl -H "Accept: application/json"
#        -H "Authorization: Token token=${TOKEN}" http://localhost:3000/api/construct_tags/5
###

# Python examples using the 'requests' module
#
# HEADERS = {'content-type': 'application/json', 'Authorization': 'Token token={}'.format(TOKEN)}
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


class Meta(type):
    def __init__(newcls, classname, supers, classdict):
        newcls.URL = os.path.join(newcls.URL, inflection.pluralize(newcls.MODEL_NAME))


class Model(metaclass=Meta):
    URL = os.environ["PULSAR_API_URL"]
    TOKEN = os.environ["PULSAR_TOKEN"]
    HEADERS = {'content-type': 'application/json', 'Authorization': 'Token token={}'.format(TOKEN)}
    MODEL_NAME = ""  # subclasses define

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

    @classmethod
    def record_url(cls, uid):
        """Given a record identifier, returns the URL to the record.

        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
        """
        uid = str(uid)
        return os.path.join(cls.URL, uid)

    @classmethod
    def delete(cls, uid):
        """Deletes the record.

        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
        """
        url = cls.record_url(uid)
        res = requests.delete(url=url, headers=Model.HEADERS, verify=False)
        return res.json()

    @classmethod
    def find_by(cls, payload):
        """
        Searches the model in question by AND joining the query parameters.

        Implements a Railsy way of looking for a record using a method by the same name and passing
        in the query as a dict. as well. 

        Only the first hit is returned, and there is not particular ordering specified in the server-side
        API method. 

        Args:
            payload: `dict`. The attributes of a record to restrict the search to.

        Returns:
            `dict`: The JSON serialization of the record, if any, found by the API call.
            `None`: If the API call didnt' return any results.
        """
        url = os.path.join(cls.URL, "find_by")
        payload = cls.add_model_name_to_payload(payload)
        print("Searching Pulsar {} for {}".format(cls.__name__, json.dumps(payload, indent=4)))
        res = requests.post(url=url, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        return res.json()

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
        url = os.path.join(cls.URL, "find_by_or")
        payload = cls.add_model_name_to_payload(payload)
        print("Searching Pulsar {} for {}".format(cls.__name__, json.dumps(payload, indent=4)))
        res = requests.post(url=url, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        return res.json()

    @classmethod
    def get(cls, uid):
        """Fetches a record by the records ID.

        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
        """

        url = cls.record_url(uid)
        print("Getting {} record with ID {}: {}".format(cls.__name__, uid, url))
        res = requests.get(url=url, headers=Model.HEADERS, verify=False)
        return res.json()

    @classmethod
    def index(cls):
        """Fetches all records.
        """
        res = requests.get(cls.URL, headers=Model.HEADERS, verify=False)
        return res.json()

    @classmethod
    def patch(cls, uid, payload):
        """Patches the payload to the specified record.

        Args     : uid - The database identifier of the record to patch. Will be converted to a string.
                   payload - hash. This will be JSON-formatted prior to sending the request.
        """
        url = cls.record_url(uid)
        payload = cls.add_model_name_to_payload(payload)
        res = requests.patch(url=url, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        return res.json()

    @classmethod
    def post(cls, payload):
        """Posts the data to the specified record.

        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
                   payload - hash. This will be JSON-formatted prior to sending the request.
        """
        payload = cls.add_model_name_to_payload(payload)
        res = requests.post(url=cls.URL, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        return res.json()

    @classmethod
    def write_response_html_to_file(cls,response,filename):
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

class Antibody(Model):
    MODEL_NAME = "antibody"

class AntibodyPurification(Model):
    MODEL_NAME = "antibody_purification"

class Biosample(Model):
    MODEL_NAME = "biosample"

class BiosampleTermName(Model):
    MODEL_NAME = "biosample_term_name"

class BiosampleType(Model):
    MODEL_NAME = "biosample_type"

class ConcentrationUnit(Model):
    MODEL_NAME = "concentration_unit"

class ConstructTag(Model):
    MODEL_NAME = "construct_tag"

class CrisprModification(Model):
    MODEL_NAME = "genetic_modification"

class Donor(Model):
    MODEL_NAME = "donor"

class Document(Model):
    MODEL_NAME = "document"

class Treatment(Model):
    MODEL_NAME = "treatment"

class TreatmentTermName(Model):
    MODEL_NAME = "treatment_term_name"

class Vendor(Model):
    MODEL_NAME = "vendor"


if __name__ == "__main__":
    # pdb.set_trace()
    #b = Biosample()
    res = b.get(uid=1716)
    #res = b.patch(uid=1772,payload={"name": "bobq_a"})
    #res = b.delete(uid=1719)
    #c = ConstructTag()
    #res = c.post(payload={"name": "howdy there partner"})
