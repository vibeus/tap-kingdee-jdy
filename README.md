# tap-kingdee-jdy

This is a [Singer][1] tap that produces JSON-formatted data following the [Singer spec][2].

This tap:

- Pulls raw data from [Kingdee API][3]
- Extracts the following resources:
  - 采购订单列表/详情: /pur/pur_order_list, /pur/pur_order_detail
  - 采购入库单列表/详情: /pur/pur_inboundr_list, /pur/pur_inbound_detail
  - 付款单列表/详情: /arap/ap_creditr_list, /arap/ap_credit_detail
  - 预付款单列表/详情: /arap/ap_precreditr_list, /arap/ap_precredit_detail

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
tap-kingdee-jdy -c config.json -d > catalog.json
```
4. Select `pur_inbound` stream in the generated `catalog.json` 
    ```
    ...
    "stream": "pur_inbound",
    "metadata": [
      {
        "breadcrumb": [],
        "metadata": {
          "table-key-properties": [
            "id",
            "billno"
          ],
          "forced-replication-method": "INCREMENTAL",
          "valid-replication-keys": [
            "modifytime"
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
tap-kingdee-jdy -c config.json --catalog catalog.json > output.txt
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