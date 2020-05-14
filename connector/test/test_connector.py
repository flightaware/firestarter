import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock
import main

def async_test(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coro(*args, **kwargs))
    return wrapper

# monkey patch MagicMock
async def async_magic():
    pass

Mock.__await__ = lambda x: async_magic().__await__()

async def resolve(val):
    return val

class EndTestNow(Exception):
    pass

class TestReconnect(unittest.TestCase):
    def setUp(self):
        self.init_cmd_limit = 2
        self.init_cmds = []
        self.env = patch.dict('os.environ', {'COMPRESSION':'', 'FH_USERNAME': 'testuser', 'FH_APIKEY': 'testapikey', 'KEEPALIVE': '60', 'INIT_CMD_ARGS': '', 'INIT_CMD_TIME': 'live', 'SERVER': 'testserver', 'PRINT_STATS_PERIOD': '0'})

    def tearDown(self):
        pass

    def save_init_cmd_stop_test(self, init_cmd):
        print("HERE")
        self.init_cmds.append(init_cmd)

        if len(self.init_cmds) >= self.init_cmd_limit:
            raise EndTestNow('Testing')

    def printhere(test):
        print("I AM HERE")

    @patch('main.open_connection')
    @patch('main.KafkaProducer')
    def test_eof(self, mock_KafkaProducer, mock_connection):
        mock_reader = Mock()
        mock_writer = Mock()
        mock_reader.readline.side_effect = [resolve(b'{"pitr":"1584126630","type":"position"}'), resolve(b"")]
        mock_writer.write.side_effect = self.save_init_cmd_stop_test
        mock_connection.side_effect = [resolve([mock_reader, mock_writer]), resolve([mock_reader, mock_writer])]
        #mock_connection.side_effect = self.printhere
        with self.assertRaises(EndTestNow), self.env:
            pitr = asyncio.run(main.main())

        self.assertEqual(self.init_cmds, [b'live username testuser password testapikey keepalive 60\n', b'pitr 1584126630 username testuser password testapikey keepalive 60\n'])
