# tap-kingdee

This is a [Singer][1] tap that produces JSON-formatted data following the [Singer spec][2].

This tap:

- Pulls raw data from [Kingdee API][3]
- Extracts the following resources:
  - 
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Install

```
pip install .
```

## Usage
1. Follow [Singer.io Best Practices][5] for setting up separate `tap` and `target` virtualenvs to avoid version conflicts.
2. Create a [config file][6] ~/config.json with [Amazon Seller Partner API credentials][7].
3. Discover catalog:
```bash
tap-referralcandy -c config.json -d > catalog.json
```
4. Select `rferrals` stream in the generated `catalog.json` 
    ```
    ...
    "stream": "rferrals",
    "metadata": [
      {
        "breadcrumb": [],
        "metadata": {
          "table-key-properties": [
            "shopify_order_number"
          ],
          "forced-replication-method": "INCREMENTAL",
          "valid-replication-keys": [
            "referral_timestamp"
          ],
          "inclusion": "available",
          "selected": true
        }
      },
      ...
    ]
    ...
    ```
5. Use following command to sync all orders with order items, buyer info and shipping address (when available).
```bash
tap-referralcandy -c config.json --catalog catalog.json > output.txt
```

---

Copyright &copy; 2021 Vibe Inc

[1]: https://singer.io
[2]: https://github.com/singer-io/getting-started/blob/master/SPEC.md
[3]: https://open.jdy.com/#/files/api/detail?index=1&categrayId=1f51c576013945e2af68ef15d4245a48&id=aca1a92ed257455f8ccd7490c5106445
<!-- [4]:  -->
[5]: https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target
[6]: https://github.com/vibeus/tap-amazon-sp/blob/master/sample_config.json
[7]: https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#creating-and-configuring-iam-policies-and-entities