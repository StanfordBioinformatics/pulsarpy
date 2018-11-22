# -*- coding: utf-8 -*-

# Author
# Nathaniel Watson
# 2018-10-14
# nathankw@stanford.edu
###

import os

import pulsarpy
from elasticsearch import Elasticsearch


class MultipleHitsException(Exception):
    """
    Raised when a search that is expected to return as most 1 hit has more than this.
    """
    pass

class Connection():

    def __init__(self):
        ES_URL = os.environ.get("ES_URL", None)
        if not ES_URL:
            print("Warning: environment variable ES_URL not set.")
        ES_USER = os.environ.get("ES_USER", "")
        if not ES_USER:
            print("Warning: environment variable ES_USER not set.")
        ES_PW = os.environ.get("ES_PW", "")
        if not ES_PW:
            print("Warning: environment variable ES_PW not set.")
        ES_AUTH = (ES_USER, ES_PW)
        self.ES = Elasticsearch(ES_URL, http_auth=ES_AUTH)


    def get_record_by_name(self, index, name):
        """
        Searches for a single document in the given index on the 'name' field .
        Performs a case-insensitive search by utilizing Elasticsearch's `match_phrase` query.
    
        Args:
            index: `str`. The name of an Elasticsearch index (i.e. biosamples).
            name: `str`. The value of a document's name key to search for.
    
        Returns:
            `dict` containing the document that was indexed into Elasticsearch.
    
        Raises:
            `MultipleHitsException`: More than 1 hit is returned.
        """
        result = self.ES.search(
            index=index,
            body={
                "query": {
                    "match_phrase": {
                        "name": name,
                    }
                }
            }
        )
        hits = result["hits"]["hits"]
        if not hits:
            return {}
        elif len(hits) == 1:
            return hits[0]["_source"]
        else:
            msg = "match_phrase search found multiple records matching query '{}' for index '{}'.".format(name, index)
            raise MultipleHitsException(msg)
    
