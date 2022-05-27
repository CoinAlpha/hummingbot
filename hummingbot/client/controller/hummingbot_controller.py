from hummingbot.client.hummingbot_application import HummingbotApplication


class HummingbotController:
    """
    This class contains the logic to expose the bot commands for remote control.

    It is designed to be wrapped by a technology-specific adapter that will define
    the method of remote communication.
    """

    def __init__(self, app: HummingbotApplication):
        self._app = app

    async def start_command(self):
        await self._app.start_check()

    async def stop_command(self):
        await self._app.stop_loop()

    async def exit_command(self):
        await self._app.exit_loop()
