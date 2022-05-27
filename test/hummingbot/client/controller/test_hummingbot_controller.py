import asyncio
import unittest
from test.mock.mock_cli import CLIMockingAssistant
from typing import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.client.config.config_helpers import read_system_configs_from_yml
from hummingbot.client.controller.hummingbot_controller import HummingbotController
from hummingbot.client.hummingbot_application import HummingbotApplication


class HummingbotControllerTest(unittest.TestCase):
    @patch("hummingbot.client.command.import_command.update_strategy_config_map_from_file")
    @patch("hummingbot.client.command.status_command.StatusCommand.status_check_all")
    @patch("hummingbot.core.utils.trading_pair_fetcher.TradingPairFetcher")
    def setUp(
        self, _: MagicMock, status_check_all_mock: AsyncMock, update_strategy_config_map_from_file: AsyncMock
    ) -> None:
        super().setUp()
        self.ev_loop = asyncio.get_event_loop()

        self.async_run_with_timeout(read_system_configs_from_yml())

        self.app = HummingbotApplication()
        self.cli_mock_assistant = CLIMockingAssistant(self.app.app)
        self.cli_mock_assistant.start()
        self.controller = HummingbotController(self.app)
        status_check_all_mock.return_value = True
        strategy_name = "some-strategy"
        strategy_file_name = f"{strategy_name}.yml"
        update_strategy_config_map_from_file.return_value = strategy_name
        self.async_run_with_timeout(self.app.import_config_file(strategy_file_name))

    def tearDown(self) -> None:
        self.cli_mock_assistant.stop()
        super().tearDown()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @patch("hummingbot.client.command.start_command.StartCommand.start_check")
    def test_start_command(self, start_check_mock: AsyncMock):
        self.async_run_with_timeout(self.controller.start_command())

        start_check_mock.assert_called_once()

    @patch("hummingbot.client.command.stop_command.StopCommand.stop_loop")
    def test_stop_command(self, stop_loop_mock: AsyncMock):
        self.async_run_with_timeout(self.controller.stop_command())

        stop_loop_mock.assert_called_once()

    @patch("hummingbot.client.command.exit_command.ExitCommand.exit_loop")
    def test_exit_command(self, exit_loop_mock: AsyncMock):
        self.async_run_with_timeout(self.controller.exit_command())

        exit_loop_mock.assert_called_once()
