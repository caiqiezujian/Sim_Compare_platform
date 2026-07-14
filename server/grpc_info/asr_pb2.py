from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

_sym_db = _symbol_database.Default()

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\tasr.proto"\xad\x01\n\nAsrRequest\x123\n\x0csessionParam\x18\x01 \x03(\x0b\x32\x1d.AsrRequest.SessionParamEntry\x12\x0f\n\x07samples\x18\x02 \x01(\x0c\x12\x13\n\x0bsamplesInfo\x18\x03 \x01(\t\x12\x0f\n\x07endFlag\x18\x04 \x01(\x08\x1a3\n\x11SessionParamEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x028\x01"K\n\tAsrResult\x12\x0f\n\x07message\x18\x01 \x01(\t\x12\x0e\n\x06status\x18\x02 \x01(\x05\x12\x0c\n\x04data\x18\x03 \x01(\t\x12\x0f\n\x07endFlag\x18\x04 \x01(\x08\x328\n\nAsrService\x12*\n\tcreateRec\x12\x0b.AsrRequest\x1a\n.AsrResult"\x00(\x010\x01b\x06proto3'
)

_ASRREQUEST = DESCRIPTOR.message_types_by_name["AsrRequest"]
_ASRREQUEST_SESSIONPARAMENTRY = _ASRREQUEST.nested_types_by_name["SessionParamEntry"]
_ASRRESULT = DESCRIPTOR.message_types_by_name["AsrResult"]

AsrRequest = _reflection.GeneratedProtocolMessageType(
    "AsrRequest",
    (_message.Message,),
    {
        "SessionParamEntry": _reflection.GeneratedProtocolMessageType(
            "SessionParamEntry",
            (_message.Message,),
            {
                "DESCRIPTOR": _ASRREQUEST_SESSIONPARAMENTRY,
                "__module__": "server.grpc_info.asr_pb2",
            },
        ),
        "DESCRIPTOR": _ASRREQUEST,
        "__module__": "server.grpc_info.asr_pb2",
    },
)
_sym_db.RegisterMessage(AsrRequest)
_sym_db.RegisterMessage(AsrRequest.SessionParamEntry)

AsrResult = _reflection.GeneratedProtocolMessageType(
    "AsrResult",
    (_message.Message,),
    {
        "DESCRIPTOR": _ASRRESULT,
        "__module__": "server.grpc_info.asr_pb2",
    },
)
_sym_db.RegisterMessage(AsrResult)
