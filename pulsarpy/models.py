# -*- coding: utf-8 -*-

# Author
# Nathaniel Watson
# 2017-09-18
# nathankw@stanford.edu
###

"""
Required Environment Variables:
  1) PULSAR_API_URL
  2) PULSAR_TOKEN
"""

import base64
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


def remove_model_prefix(uid):
    """
    Removes the optional model prefix from the given primary ID. For example, given the biosample
    record whose primary ID is 8, and given the fact that the model prefix for the Biosample model
    is "B-", the record ID can be specified either as 8 or B-8. However, the model prefix needs to
    be stripped off prior to making API calls. 
    """
    
    return str(uid).split("-")[-1]

class Meta(type):
    def __init__(newcls, classname, supers, classdict):
        newcls.URL = os.path.join(newcls.URL, inflection.pluralize(newcls.MODEL_NAME))


class Model(metaclass=Meta):
    URL = os.environ["PULSAR_API_URL"]
    TOKEN = os.environ["PULSAR_TOKEN"]
    HEADERS = {'content-type': 'application/json', 'Authorization': 'Token token={}'.format(TOKEN)}
    MODEL_NAME = ""  # subclasses define

    def __init__(self, rec_id):
        """
        Args: 
            uid: The database identifier of the record to fetch, which can be specified either as the
                primary id (i.e. 8) or the model prefix plus the primary id (i.e. B-8).
        """
        self.__dict__["attrs"] = {}
        self.rec_id = str(rec_id).split("-")[-1]
        self.record_url = os.path.join(self.URL, self.rec_id)
        self.__dict__["attrs"] = self._get() #avoid call to self.__setitem__() for this attr. 

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


    def _get(self):
        """Fetches a record by the records ID.
        """
        print("Getting {} record with ID {}: {}".format(self.__class__.__name__, self.rec_id, self.record_url))
        res = requests.get(url=self.record_url, headers=Model.HEADERS, verify=False)
        self.write_response_html_to_file(res,"get_bob.html")
        res.raise_for_status()
        #pdb.set_trace()
        #return res.json()
        return res.json()

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

    def delete(self):
        """Deletes the record.
        """
        res = requests.delete(url=self.record_url, headers=Model.HEADERS, verify=False)
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
        payload = {"find_by": payload}
        print("Searching Pulsar {} for {}".format(cls.__name__, json.dumps(payload, indent=4)))
        res = requests.post(url=url, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        res_json = res.json()
        if res_json:
           try:
               res_json = res_json[cls.MODEL_NAME]
           except KeyError:
               # Key won't be present if there isn't a serializer for it on the server. 
               pass
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
        url = os.path.join(cls.URL, "find_by_or")
        payload = {"find_by_or": payload}
        print("Searching Pulsar {} for {}".format(cls.__name__, json.dumps(payload, indent=4)))
        res = requests.post(url=url, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        res_json = res.json()
        if res_json:
           try:
               res_json = res_json[cls.MODEL_NAME]
           except KeyError:
               # Key won't be present if there isn't a serializer for it on the server. 
               pass
        return res_json

    @classmethod
    def index(cls):
        """Fetches all records.

        Returns:
            `dict`. The JSON formatted response. 

        Raises:
            `requests.exceptions.HTTPError`: The status code is not ok.
        """
        res = requests.get(cls.URL, headers=Model.HEADERS, verify=False)
        res.raise_for_status()
        return res.json()

    def patch(self,payload):
        """
        Patches the payload to the specified record, and udpates the current instance's 'attrs'
        attribute to reflect the new changes. 

        Args: 
            payload - hash. This will be JSON-formatted prior to sending the request.

        Returns:
            `NoneType`: None. 

        Raises:
            `requests.exceptions.HTTPError`: The status code is not ok.
        """
        payload = self.__class__.add_model_name_to_payload(payload)
        res = requests.patch(url=self.record_url, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        self.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()
        self.attrs = res.json()

    @classmethod
    def post(cls, payload):
        """Posts the data to the specified record.

        Args: 
            payload: `dict`. This will be JSON-formatted prior to sending the request.

        Returns:
            `dict`. The JSON formatted response. 

        Raises:
            `Requests.exceptions.HTTPError`: The status code is not ok.
        """
        #Add user to payload 
        payload["user_id"] = 1 #admin user
        payload = cls.add_model_name_to_payload(payload)
        res = requests.post(url=cls.URL, data=json.dumps(payload), headers=Model.HEADERS, verify=False)
        cls.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()
        return res.json()

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

class Antibody(Model):
    MODEL_NAME = "antibody"
    MODEL_ABBR = "AB"

class AntibodyPurification(Model):
    MODEL_NAME = "antibody_purification"
    MODEL_ABBR = "AP"

class Biosample(Model):
    MODEL_NAME = "biosample"
    MODEL_ABBR = "B"

class BiosampleOntology(Model):
    MODEL_NAME = "biosample_ontology"
    MODEL_ABBR = "BO"

class BiosampleTermName(Model):
    MODEL_NAME = "biosample_term_name"
    MODEL_ABBR = "BTN"

class BiosampleType(Model):
    MODEL_NAME = "biosample_type"
    MODEL_ABBR = "BTY"

class ConcentrationUnit(Model):
    MODEL_NAME = "concentration_unit"
    MODEL_ABBR = "CU"

class ConstructTag(Model):
    MODEL_NAME = "construct_tag"
    MODEL_ABBR = "CT"

class CrisprModification(Model):
    MODEL_NAME = "crispr_modification"
    MODEL_ABBR = "CRISPR"

class Donor(Model):
    MODEL_NAME = "donor"
    MODEL_ABBR = "DON"

class Document(Model):
    MODEL_NAME = "document"
    MODEL_ABBR = "DOC"

    def download(rec_id):
        # The sever is Base64 encoding the payload, so we'll need to base64 decode it. 
        url = self.record_url + "/download"
        res = requests.get(url=url, headers=Model.HEADERS, verify=False)
        res.raise_for_status()
        data = base64.b64decode(res.json()["data"])
        return data

class Target(Model):
    MODEL_NAME = "target"
    MODEL_ABBR = "TRG"

class Treatment(Model):
    MODEL_NAME = "treatment"
    MODEL_ABBR = "TRT"

class TreatmentTermName(Model):
    MODEL_NAME = "treatment_term_name"
    MODEL_ABBR = "TTN"

class User(Model):
    MODEL_NAME = "users"

    def archive_user(self, user_id):
        """Archives the user with the specified user ID. 

        Args:
            user_id: `int`. The ID of the user to archive.

        Returns:
            `NoneType`: None. 
        """
        url = self.record_url + "/archive"
        res = requests.patch(url=url, data=json.dumps({"user_id": user_id}), headers=Model.HEADERS, verify=False)
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
        res = requests.patch(url=url, data=json.dumps({"user_id": user_id}), headers=Model.HEADERS, verify=False)
        self.write_response_html_to_file(res,"bob.html")
        res.raise_for_status()
    
    def generate_api_key(self):
        """
        Generates an API key for the user, replacing any existing one. 

        Returns:
            `str`: The new API key.
        """
        url = self.record_url + "/generate_api_key"
        res = requests.patch(url=url, headers=Model.HEADERS, verify=False)
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
        res = requests.patch(url=url, headers=Model.HEADERS, verify=False)
        res.raise_for_status()
        self.api_key = ""


class Vendor(Model):
    MODEL_NAME = "vendor"
    MODEL_ABBR = "V"

if __name__ == "__main__":
    # pdb.set_trace()
    #b = Biosample()
    res = b.get(uid=1716)
    #res = b.patch(uid=1772,payload={"name": "bobq_a"})
    #res = b.delete(uid=1719)
    #c = ConstructTag()
    #res = c.post(payload={"name": "howdy there partner"})
