# -*- coding: utf-8 -*-

###Author
#Nathaniel Watson
#2017-09-18
#nathankw@stanford.edu
###

import requests

import pulsarpy

SREQ_STATUSES = ["not started", "started", "trouble-shooting", "failed", "finished"]

def send_mail(form, from_name):
    """
    Sends a mail using the configured mail server for Pulsar.  See mailgun documentation at
    https://documentation.mailgun.com/en/latest/user_manual.html#sending-via-api for specifics.
 
    Args:
        form: `dict`. The mail form fields, i.e. 'to', 'from', ...

    Returns: 
        `requests.models.Response` instance.

    Raises: 
        `requests.exceptions.HTTPError`: The status code is not ok.
        `Exception`: The environment variable MAILGUN_DOMAIN or MAILGUN_API_KEY isn't set. 

    Example::

        payload = {
            "from"="{} <mailgun@{}>".format(from_name, pulsarpy.MAIL_DOMAIN), 
            "subject": "mailgun test", 
            "text": "howdy there",
            "to": "nathankw@stanford.edu", 
        }
        send_mail(payload)

    """
    form["from"] = "{} <mailgun@{}>".format(from_name, pulsarpy.MAIL_DOMAIN),
    if not pulsarpy.MAIL_SERVER_URL:
        raise Exception("MAILGUN_DOMAIN environment variable not set.")
    if not pulsarpy.MAIL_AUTH[1]:
        raise Exception("MAILGUN_API_KEY environment varible not set.")
    res = requests.post(pulsarpy.MAIL_SERVER_URL, data=form, auth=pulsarpy.MAIL_AUTH)
    res.raise_for_status()
    return res


def fahrenheit_to_celsius(temp):
    return (temp - 32) * (5.0/9)

def kelvin_to_celsius(temp):
    return -273.15 + temp

def get_exp_of_biosample(biosample_rec):
    """
    Determines whether the biosample is part of a ChipseqExperiment or SingleCellSorting
    Experiment, and if so, returns the associated experiment as a models.Model instance that
    is one of those two classes. The biosample is determined to be part of a ChipseqExperiment if the Biosample.chipseq_experiment_id
    attribute is set, meaning that the biosample can be associated to the ChipseqExperiment
    as a replicate via any of of the following ChipseqExperiment attributes:

        ChipseqExperiment.replicates
        ChipseqExperiment.control_replicates

    The biosample will be determined to be part of a SingleCellSorting experiment if the
    Biosample.sorting_biosample_single_cell_sorting attribute is set, meaning that it is the
    SingleCellSorting.sorting_biosample.

    Args:
        biosample_rec: `dict`. A Biosample record as returned by instantiating `models.Biosample`.

    Raises:
        `Exception`: An experiment is not associated to this biosample.
    """
    chip_exp_id = biosample_rec.chipseq_experiment_id
    ssc_id = biosample_rec.sorting_biosample_single_cell_sorting_id
    if chip_exp_id:
        return models.ChipseqExperiment(chip_exp_id)
    elif ssc_id:
        return models.SingleCellSorting(ssc_id)
    raise Exception("Biosample {} is not on an experiment.".format(biosample_rec["id"]))

def sreqs_by_status(status):
    """
    Returns an array of all SequencingRequest objects whose status attribute is set to the specified
    state. Status must be one of {}.
    """.format(SREQ_STATUSES)
    pass
    
