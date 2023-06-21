#!/usr/bin/env python3
import os
import re
import json
import requests
import singer
from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_kingdee_jdy.streams import create_stream

import hmac
import base64
import hashlib

import time
from copy import copy
from datetime import datetime
from urllib.parse import quote, quote_plus


REQUIRED_CONFIG_KEYS = ["start_date", "client_id", "client_secret", "accounts"]
LOGGER = singer.get_logger()

BASE_URL = "http://api.kingdee.com/jdy"
HOST= "https://api.kingdee.com"
GET_APP_AUTH = "/jdyconnector/app_management/push_app_authorize"
GET_AUTH_TOKEN = "/jdyconnector/app_management/kingdee_auth_token"


def expand_env(config):
    assert isinstance(config, dict)

    def repl(match):
        env_key = match.group(1)
        return os.environ.get(env_key, "")

    def expand(v):
        assert not isinstance(v, dict)
        if isinstance(v, str):
            return re.sub(r"env\[(\w+)\]", repl, v)
        else:
            return v

    copy = {}
    for k, v in config.items():
        if isinstance(v, dict):
            copy[k] = expand_env(v)
        elif isinstance(v, list):
            copy[k] = [expand_env(x) if isinstance(x, dict) else expand(x) for x in v]
        else:
            copy[k] = expand(v)

    return copy


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas

def get_timestamp():
    time_value = time.time()
    second, millisecond = str(time_value).split('.')
    return f'{second}{millisecond[:3]}'

def format_headers_key(header_key):
    split_list = header_key.split('-')
    return '-'.join([i.capitalize() for i in split_list])

def gen_signature(secret, data):
    signature_hex = hmac.new(key=secret.encode('utf-8'),
                             msg=data.encode('utf-8'),
                             digestmod=hashlib.sha256).hexdigest()
    signature_hex_base64 = base64.b64encode(signature_hex.encode('utf-8'))
    signature_result = signature_hex_base64.decode('utf-8')
    return signature_result

def format_signature_string(method, path, params=None, headers=None, **kwargs):
    upper_method = method.upper()
    quote_path = quote_plus(path)
    params_string = '&'.join([f'{k}={quote(quote(v))}' for k, v in params.items()]) if params else ''
    copy_headers = {k.lower(): v for k, v in copy(headers).items()}
    signature_headers = {
        'X-Api-Nonce': copy_headers['x-api-nonce'],
        'X-Api-TimeStamp': copy_headers['x-api-timestamp']
    }
    signature_headers_string = '\n'.join([f'{k.lower()}:{v}' for k, v in signature_headers.items()])
    signature_data_list = [upper_method, quote_path, params_string, signature_headers_string]
    signature_text_result = '\n'.join(signature_data_list)
    return signature_text_result + '\n'

def get_full_headers(client_id, client_secret, method, path, params, headers={}):
    signature_headers = {'x-api-nonce': "2530", 'x-api-timestamp': get_timestamp()}
    full_headers = {k.lower(): v for k, v in copy(headers).items()}
    full_headers.update(signature_headers)
    signature_text = format_signature_string(method=method, path=path, params=params, headers=signature_headers)
    signature_value = gen_signature(client_secret, signature_text)
    full_headers.update({
        'X-Api-Signature': signature_value,
        'X-Api-Auth-Version': '2.0',
        'X-Api-ClientID': client_id,
        'X-Api-SignHeaders': 'X-Api-Nonce,X-Api-TimeStamp',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.54'
    })
    return {format_headers_key(k): v for k, v in full_headers.items()}

def get_app_secret(client_id, client_secret, outer_instance_id):
    app_secret, domain, group_name = '', '', ''
    params = {"outerInstanceId": outer_instance_id}
    headers = get_full_headers(client_id, client_secret, 'POST', GET_APP_AUTH, params)
    resp = requests.request('POST', HOST+GET_APP_AUTH, headers=headers, params=params).json()
    if 'data' in resp and resp['data']:
        app_secret = resp['data'][0]['appSecret']
        domain = resp['data'][0]['domain']
        group_name = resp['data'][0]['groupName']
    return app_secret, domain, group_name

def get_access_token(client_id, client_secret, account):
    app_secret, domain, group_name = get_app_secret(client_id, client_secret, account['outer_instance_id'])
    params = {"app_key": account['app_key'], "app_signature": gen_signature(app_secret, account['app_key'])}
    headers = get_full_headers(client_id, client_secret, 'GET', GET_AUTH_TOKEN, params)
    resp = requests.request('GET', HOST+GET_AUTH_TOKEN, headers=headers, params=params).json()
    app_token, access_token = '', ''
    if 'data' in resp and resp['data']:
        app_token = resp['data']['app-token']
        access_token = resp['data']['access_token']
    return app_token, access_token, domain, group_name

def update_config(config):
    new_accounts = []
    for account in config["accounts"]:
        app_token, access_token, domain, group_name = get_access_token(config['client_id'], config['client_secret'], account)
        account['app_token'] = app_token
        account['access_token'] = access_token
        account['domain'] = domain
        account['group_name'] = group_name
        new_accounts.append(account)
    config["accounts"] = new_accounts
    LOGGER.warning(f"{config}")
    return config

def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream = create_stream(stream_id)

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=stream.key_properties,
                metadata=stream.get_metadata(schema.to_dict()),
                replication_key=stream.replication_key,
                replication_method=stream.replication_method,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
            )
        )
    return Catalog(streams)

def sync(config, state, catalog):

    """Sync data from tap source"""

    state_dict = {}

    for catalog_stream in catalog.get_selected_streams(state):
        stream_id = catalog_stream.tap_stream_id
        LOGGER.info("Syncing stream:" + stream_id)

        singer.write_schema(
            stream_name=stream_id,
            schema=catalog_stream.schema.to_dict(),
            key_properties=catalog_stream.key_properties,
        )

        stream = create_stream(stream_id)
        stream_state = state.get(stream_id, {})

        t = Transformer()
        for row in stream.get_tap_data(config, stream_state):
            schema = catalog_stream.schema.to_dict()
            mdata = metadata.to_map(catalog_stream.metadata)
            record = t.transform(row, schema, mdata)
            singer.write_records(stream_id, [record])

        state_dict[stream_id] = stream.state
        singer.write_state(state_dict)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        args.config = expand_env(args.config)
        updated_config = update_config(args.config)
        sync(updated_config, args.state, catalog)

if __name__ == "__main__":
    main()
