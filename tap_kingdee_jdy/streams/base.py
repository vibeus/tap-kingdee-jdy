import singer
import json
import time
from singer import metadata

from datetime import datetime, timedelta
from dateutil.parser import parse

import requests
from requests.exceptions import RequestException, HTTPError 

LOGGER = singer.get_logger()
DEFAULT_BACKOFF_SECONDS = 60

BASE_URL = "http://api.kingdee.com/jdy"

class Base:
    def __init__(self):
        self._start_date = ""
        self._state = {}

    @property
    def name(self):
        return "base_stream"

    @property
    def key_properties(self):
        return ["id", "billno"]

    @property
    def replication_key(self):
        return "modifytime"

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def state(self):
        return self._state
    
    @property
    def specific_api(self):
        return "/"


    def get_metadata(self, schema):
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.key_properties,
            valid_replication_keys=[self.replication_key],
            replication_method=self.replication_method,
        )

        return mdata

    def get_tap_data(self, config, state):
        base_headers = {
            "Content-Type": "application/json",
            "charset": "utf-8",
            "groupName": config["groupName"],
            # "accountId": config["accountId"],
        }
        params = {"access_token": config["access_token"]}

        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        self._start_date = config.get("start_date", today) # config start date
        self._backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)
        self._state = state.copy()

        for accountId in config["accountIds"]:
            headers = base_headers.copy()
            headers["accountId"] = accountId
            yield from self.get_account_data(headers, params)
    
    def get_account_data(self, headers, params):
        state_date = self._state.get(headers['accountId'], self._start_date) # state start date
        start = max(parse(self._start_date), parse(state_date))
        max_rep_key = start
        LOGGER.info(f"start from {start.isoformat()} for account {headers['accountId']}")
        page = 1 
        while True:
            try:
                resp = requests.post(url=f"{BASE_URL}{self.specific_api}_list",
                    headers=headers, params=params,
                    json={"pagesize": 100, "page": page, "begindate": start.strftime("%Y-%m-%d")})
                LOGGER.info(f"{self.specific_api}_list status_code: {resp.status_code}")
                if resp.status_code == 200:
                    resp = resp.json()
                    if resp["success"] == True:
                        if len(resp["data"]["rows"]) == 0:
                            break
                        for row in resp["data"]["rows"]:
                            data = self.get_detail_data(row["id"], headers, params)
                            if data:
                                data["accountId"] = headers["accountId"]
                                rep_key = data.get(self.replication_key)
                                if rep_key and parse(rep_key) > max_rep_key:
                                    max_rep_key = parse(rep_key)
                                yield data
                
                page += 1
            
            except RequestException as e:
                LOGGER.warning(f"{e} Waiting {self._backoff_seconds} seconds...")
                time.sleep(self._backoff_seconds)

        self._state[headers['accountId']] = max_rep_key.isoformat()

    def get_detail_data(self, id, headers, params):
        resp = requests.post(url=f"{BASE_URL}{self.specific_api}_detail",
            headers=headers, params=params, 
            json={"id": id})
        LOGGER.info(f"{self.specific_api}_detail status_code: {resp.status_code}")
        if resp.status_code == 200:
            resp = resp.json()
            if resp["success"] == True:
                return resp["data"]
