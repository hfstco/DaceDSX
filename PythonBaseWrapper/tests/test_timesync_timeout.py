
import unittest
from unittest.mock import MagicMock

import kafka_stub
from TimeSync import TimeSync


class TestTimeSyncTimeout(unittest.TestCase):

    def test_progress_from_zero_messages(self):
        sync = object.__new__(TimeSync)

        sync.expectedReceiveCount = {"grid.voltage": 5}
        sync.actualReceivedMsgCount = {}
        sync.receiveCountErrors = {}
        sync.lastObservedReceiveCount = {}

        sync.max_error_count = 2
        sync.timeoutHandler = MagicMock()
        sync.logging = False

        self.assertFalse(sync.msgCountOK())
        self.assertEqual(sync.lastObservedReceiveCount["grid.voltage"], 0)

        sync.actualReceivedMsgCount["grid.voltage"] = 1
        self.assertFalse(sync.msgCountOK())

        self.assertEqual(sync.receiveCountErrors["grid.voltage"], 0)
        sync.timeoutHandler.assert_not_called()


    def test_partial_messages_trigger_timeout(self):
        sync = object.__new__(TimeSync)

        sync.expectedReceiveCount = {"grid.voltage": 5}
        sync.actualReceivedMsgCount = {"grid.voltage": 2}

        sync.receiveCountErrors = {}
        sync.lastObservedReceiveCount = {}

        sync.max_error_count = 2
        sync.timeoutHandler = MagicMock()
        sync.logging = False

        self.assertFalse(sync.msgCountOK())
        self.assertFalse(sync.msgCountOK())

        sync.timeoutHandler.assert_called_once()


    def test_no_messages_trigger_timeout(self):
        sync = object.__new__(TimeSync)

        sync.expectedReceiveCount = {"grid.voltage": 5}
        sync.actualReceivedMsgCount = {}

        sync.receiveCountErrors = {}
        sync.lastObservedReceiveCount = {}

        sync.max_error_count = 2
        sync.timeoutHandler = MagicMock()
        sync.logging = False

        self.assertFalse(sync.msgCountOK())
        self.assertFalse(sync.msgCountOK())

        sync.timeoutHandler.assert_called_once()

    def test_progress_does_not_trigger_timeout(self):
        sync = object.__new__(TimeSync)

        sync.expectedReceiveCount = {"grid.voltage": 5}
        sync.actualReceivedMsgCount = {"grid.voltage": 1}

        sync.receiveCountErrors = {}
        sync.lastObservedReceiveCount = {}
        sync.max_error_count = 2
        sync.timeoutHandler = MagicMock()
        sync.logging = False

        self.assertFalse(sync.msgCountOK())
        sync.actualReceivedMsgCount["grid.voltage"] = 2
        self.assertFalse(sync.msgCountOK())
        sync.timeoutHandler.assert_not_called()


if __name__ == "__main__":
    unittest.main()