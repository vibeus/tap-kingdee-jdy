import singer
import json
import time
from datetime import datetime
from singer import metadata

from .base import Base

LOGGER = singer.get_logger()

class ApPreCredit(Base):
    def __init__(self):
        self._start_date = ""
        self._state = {}

    @property
    def name(self):
        return "ap_precredit"

    @property
    def state(self):
        return self._state
    
    @property
    def specific_api(self):
        return "/arap/ap_precredit"

