import logging

import grpc
from google.protobuf import empty_pb2, struct_pb2

from hummingbot.client.controller.hummingbot_controller import HummingbotController
from hummingbot.client.controller.rpc.hummingbot_controller_pb2_grpc import HummingbotControllerServicer
from hummingbot.logger import HummingbotLogger

s_logger = None


class HummingbotControllerRPCServer(HummingbotControllerServicer):
    """
    An adapter to the HummingbotController that exposes its functionality over
    an gRPC interface.
    """

    def __init__(self, controller: HummingbotController):
        super().__init__()
        self._controller = controller

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    async def start_command(self, request: empty_pb2, context: grpc.aio.ServicerContext) -> struct_pb2.Value:
        self.logger().info("Start command called.")
        await self._controller.start_command()
        return struct_pb2.Value()

    async def stop_command(self, request: empty_pb2, context: grpc.aio.ServicerContext) -> struct_pb2.Value:
        self.logger().info("Stop command called.")
        await self._controller.stop_command()
        return struct_pb2.Value()

    async def exit_command(self, request: empty_pb2, context: grpc.aio.ServicerContext) -> struct_pb2.Value:
        self.logger().info("Exit command called.")
        await self._controller.exit_command()
        return struct_pb2.Value()
