import grpc

from . import asr_pb2 as asr__pb2


class AsrServiceStub(object):
    """Streaming ASR/S2TT service stub."""

    def __init__(self, channel):
        self.createRec = channel.stream_stream(
            "/AsrService/createRec",
            request_serializer=asr__pb2.AsrRequest.SerializeToString,
            response_deserializer=asr__pb2.AsrResult.FromString,
        )


class AsrServiceServicer(object):
    """Base servicer generated from asr.proto."""

    def createRec(self, request_iterator, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_AsrServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "createRec": grpc.stream_stream_rpc_method_handler(
            servicer.createRec,
            request_deserializer=asr__pb2.AsrRequest.FromString,
            response_serializer=asr__pb2.AsrResult.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("AsrService", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


class AsrService(object):
    """Experimental static client helper kept for generated-file compatibility."""

    @staticmethod
    def createRec(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/AsrService/createRec",
            asr__pb2.AsrRequest.SerializeToString,
            asr__pb2.AsrResult.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
