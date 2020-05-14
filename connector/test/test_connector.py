import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock
import main

def CoroMock():
    coro = Mock(name="CoroutineResult")
    corofunc = Mock(name="CoroutineFunction", side_effect=asyncio.coroutine(coro))
    corofunc.coro = coro
    return corofunc

class EndTestNow(Exception):
    pass

class TestReconnect(unittest.TestCase):
    def setUp(self):
        self.init_cmd_limit = 2
        self.init_cmds = []
        self.env = patch.dict('os.environ', {'COMPRESSION':'', 'FH_USERNAME': 'testuser', 'FH_APIKEY': 'testapikey', 'KEEPALIVE': '60', 'INIT_CMD_ARGS': '', 'INIT_CMD_TIME': 'live', 'SERVER': 'testserver', 'PRINT_STATS_PERIOD': '0', 'KAFKA_TOPIC_NAME': 'topic1'})
        self.mock_reader = Mock()
        self.mock_reader.readline = CoroMock()
        self.mock_writer = Mock()
        self.mock_writer.drain = CoroMock()
        self.mock_writer.write.side_effect = self.save_init_cmd_stop_test

    def tearDown(self):
        pass

    def save_init_cmd_stop_test(self, init_cmd):
        self.init_cmds.append(init_cmd)

        if len(self.init_cmds) >= self.init_cmd_limit:
            raise EndTestNow()

    @patch('main.open_connection', new_callable=CoroMock)
    @patch('main.KafkaProducer', new_callable=Mock)
    def test_eof(self, mock_kafkaproducer, mock_openconnection):
        # mock setup
        self.mock_reader.readline.coro.side_effect = [b'{"pitr":"1584126630","type":"position"}', b""]
        mock_openconnection.coro.return_value = self.mock_reader, self.mock_writer

        # run test
        with self.assertRaises(EndTestNow), self.env:
            pitr = asyncio.run(main.main())

        # verify expected init cmds
        self.assertEqual(self.init_cmds, [b'live username testuser password testapikey keepalive 60\n', b'pitr 1584126630 username testuser password testapikey keepalive 60\n'])
        # verify expect output to kafka
        mock_kafkaproducer.return_value.send.assert_called_once_with('topic1', b'{"pitr":"1584126630","type":"position"}')

