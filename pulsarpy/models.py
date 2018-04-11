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
#    data = {'name': 'test_tag_ampc', 'description': "C'est bcp + qu'un simple ..."}
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
    def record_url(cls, uid):
        """
        Function : Given a record identifier, returns the URL to the record.
        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
        """
        uid = str(uid)
        return os.path.join(cls.URL, uid)

    @classmethod
    def delete(cls, uid):
        """
        Function : Deletes the record.
        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
        """
        url = cls.record_url(uid)
        res = requests.delete(url=url, headers=cls.HEADERS, verify=False)
        return res.json()

    @classmethod
    def find_by(cls, params):
        """
        Implements a Railsy way of looking for a record using a method by the same name.

        Args:
            params: `dict`. The attributes of a record to restrict the search to.

        Returns:
            `dict`: The JSON serialization of the records found by the API call.
            `None`: If the API call didnt' return any results.
        """
        url = os.path.join(cls.URL, "find_by")
        res = requests.get(url=url, params=params, headers=cls.HEADERS, verify=False)
        #return res.json
        cls.write_response_html_to_file(res,"bob.html")
        return res

    @classmethod
    def get(cls, uid):
        """
        Function : Fetches a record by the records ID.
        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
        """

        res = requests.get(url=url, headers=cls.HEADERS, verify=False)
        return res.json()

    @classmethod
    def patch(cls, uid, data):
        """
        Function : Patches the data to the specified record.
        Args     : uid - The database identifier of the record to patch. Will be converted to a string.
                   data - hash. This will be JSON-formatted prior to sending the request.
        """
        url = cls.record_url(uid)
        if not cls.MODEL_NAME in data:
            data = {cls.MODEL_NAME: data}
        res = requests.patch(url=url, data=json.dumps(data), headers=cls.HEADERS, verify=False)
        return res

    @classmethod
    def post(cls, data):
        """
        Function : Posts the data to the specified record.
        Args     : uid - The database identifier of the record to fetch. Will be converted to a string.
                   data - hash. This will be JSON-formatted prior to sending the request.
        """
        if not cls.MODEL_NAME in data:
            data = {cls.MODEL_NAME: data}
        return requests.post(url=cls.URL, data=json.dumps(data), headers=cls.HEADERS, verify=False)

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


class ConstructTag(Model):
    MODEL_NAME = "construct_tag"


class Document(Model):
    MODEL_NAME = "document"


if __name__ == "__main__":
    # pdb.set_trace()
    b = Biosample()
    pdb.set_trace()
    res = b.get(uid=1716)
    #res = b.patch(uid=1772,data={"name": "bobq_a"})
    #res = b.delete(uid=1719)
    #c = ConstructTag()
    #res = c.post(data={"name": "howdy there partner"})
    pdb.set_trace()
