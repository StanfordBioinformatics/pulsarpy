# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
The official Python client for Pulsar LIMS.

Required Environment Variables:
    1) PULSAR_API_URL
    2) PULSAR_TOKEN
"""

import logging
import os
import sys
from urllib.parse import urlparse

# MAIL
MAIL_DOMAIN = os.environ.get("MAILGUN_DOMAIN","")
MAIL_SERVER_URL = os.path.join("https://api.mailgun.net/v3/{}/messages".format(MAIL_DOMAIN))
if not MAIL_DOMAIN:
    # We don't want utils.send_mail trying to send mails when there isn't a domain specified.
    # I tried this, and we strangly get a 200 back, so this could cause issues. 
    MAIL_SERVER_URL = ""
MAIL_AUTH = ("api", os.environ.get("MAILGUN_API_KEY",""))
DEFAULT_TO = ["nathankw@stanford.edu"]

#: The directory that contains the log files created by the `Model` class.
LOG_DIR = "Pulsarpy_Logs"
URL = os.environ.get("PULSAR_API_URL", "")
HOST = ""
if URL:
    HOST = urlparse(URL).hostname
API_TOKEN = os.environ.get("PULSAR_TOKEN", "")

#: The name of the debug ``logging`` instance.
DEBUG_LOGGER_NAME = "ppy_debug"
#: The name of the error ``logging`` instance created in the ``pulsarpy.models.Model`` class.
#: and referenced elsewhere.
ERROR_LOGGER_NAME = "ppy_error"
#: The name of the POST ``logging`` instance created in the ``pulsarpy.models.Model`` claass.
#: and referenced elsewhere.
POST_LOGGER_NAME = "ppy_post"

#: A ``logging`` instance that logs all messages sent to it to STDOUT.
debug_logger = logging.getLogger(DEBUG_LOGGER_NAME)
level = logging.DEBUG
debug_logger.setLevel(level)
f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(level)
ch.setFormatter(f_formatter)
debug_logger.addHandler(ch)
