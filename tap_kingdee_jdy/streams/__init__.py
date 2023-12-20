from .pur_order import PurOrder
from .pur_inbound import PurInbound
from .ap_precredit import ApPreCredit
from .ap_credit import ApCredit
from .material_group import MaterialGroup
from .material import Material

def create_stream(stream_id):
    if stream_id == "pur_order":
        return PurOrder()
    if stream_id == "pur_inbound":
        return PurInbound()
    if stream_id == "ap_precredit":
        return ApPreCredit()
    if stream_id == "ap_credit":
        return ApCredit()
    if stream_id == "material_group":
        return MaterialGroup ()
    if stream_id == "material":
        return Material()

    assert False, f"Unsupported stream: {stream_id}"
