# -*- coding: utf-8 -*-                                                                             
                                                                                                    
###                                                                                                 
# © 2018 The Board of Trustees of the Leland Stanford Junior University                             
# Nathaniel Watson                                                                                  
# nathankw@stanford.edu                                                                             
### 

"""
Required Environment Variables:                                                                    
    1) PULSAR_API_URL                                                                              
    2) PULSAR_TOKEN
"""

import logging
import os
import sys
from urllib.parse import urlparse


#: The directory that contains the log files created by the `Model` class. 
LOG_DIR = "Pulsarpy_Logs" 
URL = os.environ["PULSAR_API_URL"]                                                                 
HOST = urlparse(URL).hostname                                                                      
API_TOKEN = os.environ["PULSAR_TOKEN"] 

#: The name of the debug ``logging`` instance.                                                         
DEBUG_LOGGER_NAME = "debug"                                                                            
#: The name of the error ``logging`` instance created in the ``pulsarpy.models.Model`` class.
#: and referenced elsewhere.                                                                           
ERROR_LOGGER_NAME = "error"                                                                            
#: The name of the POST ``logging`` instance created in the ``pulsarpy.models.Model`` claass.
#: and referenced elsewhere.                                                                           
POST_LOGGER_NAME = "post"                                                                              
                                                                                                       
#: A ``logging`` instance that logs all messages sent to it to STDOUT.                                 
debug_logger = logging.getLogger(DEBUG_LOGGER_NAME)                                                    
level = logging.DEBUG                                                                                  
debug_logger.setLevel(level)                                                                           
f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')                                  
ch = logging.StreamHandler(stream=sys.stdout)                                                          
ch.setLevel(level)                                                                                     
ch.setFormatter(f_formatter)                                                                        
debug_logger.addHandler(ch)
