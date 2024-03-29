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
MAX_TRIES = 10

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
        return ["accountId", "id"]

    @property
    def replication_key(self):
        return "modifytime"

    @property
    def replication_method(self):
        # return "FULL_TABLE"
        return "INCREMENTAL"

    @property
    def state(self):
        return self._state

    @property
    def specific_api(self):
        return "/jdy"


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
        }

        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        self._start_date = config.get("start_date", today) # config start date
        self._backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)
        self._state = state.copy()

        for account in config["accounts"]:
            headers = base_headers.copy()
            headers["accountId"] = account['account_id']
            headers["groupName"] = account['group_name']
            params = {"access_token": account["access_token"]}
            yield from self.get_account_data(headers, params)

    def get_account_data(self, headers, params):
        state_date = self._state.get(headers['accountId'], self._start_date) # state start date
        start = max(parse(self._start_date), parse(state_date))
        max_rep_key = start
        # lookback 95 days on every Thursday
        # if datetime.utcnow().weekday() == 3:
        #     start = min(start, datetime.utcnow() - timedelta(days=95))
        page = 1
        body = {"pagesize": 100, "begindate": start.strftime("%Y-%m-%d")}
        LOGGER.info(f"start from {start.isoformat()} for account {headers['accountId']}")

        while True:
            try:
                body.update({"page": page})
                resp = requests.post(url=f"{BASE_URL}{self.specific_api}_list",
                    headers=headers, params=params, json=body)
                LOGGER.info(f"{self.specific_api}_list status_code: {resp.status_code}")
                if resp.status_code == 519:
                    break
                if resp.status_code == 200:
                    resp = resp.json()
                    if "success" in resp and resp["success"] == True:
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
                    else:
                        LOGGER.warning(f"{resp}")

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
            if "success" in resp and resp["success"] == True:
                return resp["data"]
            else:
                LOGGER.warning(f"{resp}")
