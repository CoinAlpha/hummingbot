#!/usr/bin/env python

import asyncio

import grpc
import path_util  # noqa: F401
from google.protobuf import empty_pb2

from hummingbot.client.config.config_helpers import read_system_configs_from_yml
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.client.controller.rpc.hummingbot_controller_pb2_grpc import HummingbotControllerStub


async def main():
    print("Starting the controller client...")
    await read_system_configs_from_yml()
    port = global_config_map["controller_server_port"].value
    print(f"Communicating over port {port}.")
    async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
        stub = HummingbotControllerStub(channel)
        empty = empty_pb2.Empty()

        commands = {
            "start": stub.start_command,
            "stop": stub.stop_command,
            "exit": stub.exit_command,
        }

        while True:
            command_input = input(
                "Enter bot command ('start', 'stop', 'exit') or input 'quit' to quit the demo: "
            )
            command = commands.get(command_input)
            if command is None:
                if command_input == "quit":
                    break
                print(f"Command {command_input} not recognized.")
            else:
                await command(empty)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
