import sys
import os
import asyncio
import warnings

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock, AsyncMock, ANY
import main


class EndTestNow(Exception):
    pass


class TestReconnect(unittest.TestCase):
    def setUp(self):
        self.init_cmd_limit = 2
        self.init_cmds = []
        self.env = patch.dict(
            "os.environ",
            {
                "COMPRESSION": "",
                "FH_USERNAME": "testuser",
                "FH_APIKEY": "testapikey",
                "KEEPALIVE": "60",
                "KEEPALIVE_STALE_PITRS": "5",
                "INIT_CMD_ARGS": "",
                "INIT_CMD_TIME": "live",
                "SERVER": "testserver",
                "PRINT_STATS_PERIOD": "0",
                "KAFKA_TOPIC_NAME": "topic1",
            },
        )
        self.mock_reader = Mock()
        self.mock_reader.readline = AsyncMock()
        self.mock_writer = Mock()
        self.mock_writer.drain = AsyncMock()
        self.mock_writer.write.side_effect = self.save_init_cmd_stop_test

    def tearDown(self):
        pass

    def save_init_cmd_stop_test(self, init_cmd):
        self.init_cmds.append(init_cmd)

        if len(self.init_cmds) >= self.init_cmd_limit:
            raise EndTestNow()

    def reconnect_after_error(
        self, test_reconnect_live, mock_kafkaproducer, mock_openconnection, error
    ):
        # mock setup
        if not isinstance(error, list):
            error = [error]
        if test_reconnect_live:
            self.mock_reader.readline.side_effect = error
        else:
            self.mock_reader.readline.side_effect = [
                b'{"pitr":"1584126630","type":"arrival","id":"KPVD-1588929046-hexid-ADF994"}',
            ] + error
        mock_openconnection.return_value = self.mock_reader, self.mock_writer

        # run test
        with self.assertRaises(EndTestNow), self.env:
            pitr = asyncio.run(main.main())

        if test_reconnect_live:
            # verify expected init cmds
            self.assertEqual(
                self.init_cmds,
                [
                    b"live username testuser password testapikey useragent firestarter keepalive 60\n",
                    b"live username testuser password testapikey useragent firestarter keepalive 60\n",
                ],
            )
            # verify expect output to kafka
            mock_kafkaproducer.return_value.produce.assert_not_called()
        else:
            # verify expected init cmds
            self.assertEqual(
                self.init_cmds,
                [
                    b"live username testuser password testapikey useragent firestarter keepalive 60\n",
                    b"pitr 1584126630 username testuser password testapikey useragent firestarter keepalive 60\n",
                ],
            )
            # verify expect output to kafka
            if len(error) == 1:
                mock_kafkaproducer.return_value.produce.assert_called_once_with(
                    "topic1",
                    key=b"KPVD-1588929046-hexid-ADF994",
                    value=b'{"pitr":"1584126630","type":"arrival","id":"KPVD-1588929046-hexid-ADF994"}',
                    callback=ANY,
                )
            else:
                self.assertEqual(mock_kafkaproducer.return_value.produce.call_count, len(error))

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_eof(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(False, mock_kafkaproducer, mock_openconnection, b"")

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_eof(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(True, mock_kafkaproducer, mock_openconnection, b"")

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_timeout(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            False, mock_kafkaproducer, mock_openconnection, asyncio.TimeoutError
        )

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_timeout(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            True, mock_kafkaproducer, mock_openconnection, asyncio.TimeoutError
        )

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_disconnect(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(False, mock_kafkaproducer, mock_openconnection, AttributeError)

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_disconnect(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(True, mock_kafkaproducer, mock_openconnection, AttributeError)

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_error_msg(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            False,
            mock_kafkaproducer,
            mock_openconnection,
            b'{"pitr":"1584126630","type":"error","error_msg":"test error"}',
        )

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_error_msg(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            True,
            mock_kafkaproducer,
            mock_openconnection,
            b'{"pitr":"1584126630","type":"error","error_msg":"test error"}',
        )

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_drift_exceeded(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            False,
            mock_kafkaproducer,
            mock_openconnection,
            [
                b'{"pitr":"1584126630","type":"keepalive"}',
                b'{"pitr":"1584126630","type":"keepalive"}',
                b'{"pitr":"1584126630","type":"keepalive"}',
                b'{"pitr":"1584126630","type":"keepalive"}',
                b'{"pitr":"1584126630","type":"keepalive"}',
                b'{"pitr":"1584126630","type":"keepalive"}',
            ]
        )

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_drift_reset(self, mock_kafkaproducer, mock_openconnection):
        # does not reconnect and is waiting for the next message
        with self.assertRaises(StopAsyncIteration), self.env:
            self.reconnect_after_error(
                False,
                mock_kafkaproducer,
                mock_openconnection,
                [
                    b'{"pitr":"1584126630","type":"keepalive"}',
                    b'{"pitr":"1584126630","type":"keepalive"}',
                    b'{"pitr":"1584126630","type":"keepalive"}',
                    b'{"pitr":"1584126630","type":"keepalive"}',
                    b'{"pitr":"1584126630","type":"keepalive"}',
                    b'{"pitr":"1584126631","type":"keepalive"}',
                ]
            )


# THIS TEST WILL ONLY RUN IN TRAVIS
@unittest.skipIf(not os.getenv("FH_APIKEY"), "No login credentials")
class TestCompression(unittest.TestCase):
    def setUp(self):
        self.emitted_msg = []
        self.save_main_open_connection = main.open_connection
        warnings.simplefilter("ignore", category=ResourceWarning)

    def tearDown(self):
        self.fh_writer.close()

    async def wrap_open_connection(self, *args, **kwargs):
        self.fh_reader, self.fh_writer = await self.save_main_open_connection(*args, **kwargs)
        return self.fh_reader, self.fh_writer

    def save_line_stop_test(self, topic, value=None, key=None, callback=None):
        self.emitted_msg.append(value)
        raise EndTestNow()

    def compression(self, mock_kafkaproducer, mock_openconnection, compression):
        mock_kafkaproducer.return_value.produce.side_effect = self.save_line_stop_test
        mock_openconnection.side_effect = self.wrap_open_connection
        # run test
        with self.assertRaises(EndTestNow):
            os.environ["COMPRESSION"] = compression
            pitr = asyncio.run(main.main())

        self.assertEqual(
            self.emitted_msg,
            [
                b'{"pitr":"1647160200","type":"flifo","ident":"RYR1005","dest":"LROP","actual_off":"1647159545","estimated_on":"1647170100","ete":"10555","facility_hash":"D5A46EA72EB25728","facility_name":"","id":"RYR1005-1646986800-schedule-0083","orig":"EGSS","reg":"EIEKI","scheduled_off":"1647159300","scheduled_departure_gate":"37","scheduled_out":"1647159300","status":"A"}\n'
            ],
        )

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_no_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "")

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_gzip_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "gzip")

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_compress_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "compress")

    @patch("main.open_connection", new_callable=AsyncMock)
    @patch("main.Producer", new_callable=Mock)
    def test_deflate_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "deflate")
