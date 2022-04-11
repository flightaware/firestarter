import sys
import os
import asyncio
import warnings

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock, ANY
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

    def reconnect_after_error(
        self, test_reconnect_live, mock_kafkaproducer, mock_openconnection, error
    ):
        # mock setup
        if not isinstance(error, list):
            error = [error]
        if test_reconnect_live:
            self.mock_reader.readline.coro.side_effect = [error]
        else:
            self.mock_reader.readline.coro.side_effect = [
                b'{"pitr":"1584126630","type":"arrival","id":"KPVD-1588929046-hexid-ADF994"}',
                error,
            ]
        mock_openconnection.coro.return_value = self.mock_reader, self.mock_writer

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

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_eof(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(False, mock_kafkaproducer, mock_openconnection, b"")

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_eof(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(True, mock_kafkaproducer, mock_openconnection, b"")

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_timeout(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            False, mock_kafkaproducer, mock_openconnection, asyncio.TimeoutError
        )

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_timeout(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            True, mock_kafkaproducer, mock_openconnection, asyncio.TimeoutError
        )

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_disconnect(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(False, mock_kafkaproducer, mock_openconnection, AttributeError)

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_disconnect(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(True, mock_kafkaproducer, mock_openconnection, AttributeError)

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_pitr_error_msg(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            False,
            mock_kafkaproducer,
            mock_openconnection,
            b'{"pitr":"1584126630","type":"error","error_msg":"test error"}',
        )

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_live_error_msg(self, mock_kafkaproducer, mock_openconnection):
        self.reconnect_after_error(
            True,
            mock_kafkaproducer,
            mock_openconnection,
            b'{"pitr":"1584126630","type":"error","error_msg":"test error"}',
        )

    @patch("main.open_connection", new_callable=CoroMock)
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

    @patch("main.open_connection", new_callable=CoroMock)
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
                b'{"pitr":"1584126630","type":"flightplan","ident":"DAL3120","aircrafttype":"B739","alt":"37000","dest":"KMSP","edt":"1584188640","eta":"1584193723","ete":"5083","facility_hash":"81E755935A704D47","facility_name":"","fdt":"1584187800","id":"DAL3120-1583991944-airline-0270","orig":"KDEN","reg":"N908DN","route":"KDEN.YOKES7.CHICI..HAAND..ONL..FSD..SSWAN.TORGY3.KMSP","speed":"418","status":"S","waypoints":[{"lat":39.86000,"lon":-104.67000},{"lat":39.87000,"lon":-104.63000},{"lat":39.89000,"lon":-104.52000},{"lat":39.91000,"lon":-104.43000},{"lat":39.97000,"lon":-104.38000},{"lat":39.99000,"lon":-104.38000},{"lat":40.06000,"lon":-104.38000},{"lat":40.14000,"lon":-104.46000},{"lat":40.18000,"lon":-104.49000},{"lat":40.19000,"lon":-104.51000},{"lat":40.30000,"lon":-104.50000},{"lat":40.34000,"lon":-104.46000},{"lat":40.42000,"lon":-104.38000},{"lat":40.44000,"lon":-104.36000},{"lat":40.58000,"lon":-104.24000},{"lat":40.58000,"lon":-104.23000},{"lat":40.59000,"lon":-104.22000},{"lat":40.60000,"lon":-104.20000},{"lat":40.66000,"lon":-104.14000},{"lat":40.72000,"lon":-104.06000},{"lat":40.77000,"lon":-103.99000},{"lat":40.87000,"lon":-103.86000},{"lat":41.13000,"lon":-103.53000},{"lat":41.25000,"lon":-103.37000},{"lat":41.40000,"lon":-103.17000},{"lat":41.57000,"lon":-102.26000},{"lat":41.65000,"lon":-101.96000},{"lat":41.72000,"lon":-101.69000},{"lat":42.33000,"lon":-99.29000},{"lat":42.47000,"lon":-98.69000},{"lat":43.37000,"lon":-97.24000},{"lat":43.65000,"lon":-96.78000},{"lat":44.16000,"lon":-95.95000},{"lat":44.23000,"lon":-95.76000},{"lat":44.32000,"lon":-95.53000},{"lat":44.46000,"lon":-95.18000},{"lat":44.48000,"lon":-95.08000},{"lat":44.50000,"lon":-95.02000},{"lat":44.55000,"lon":-94.79000},{"lat":44.55000,"lon":-94.77000},{"lat":44.58000,"lon":-94.66000},{"lat":44.64000,"lon":-94.38000},{"lat":44.67000,"lon":-94.24000},{"lat":44.68000,"lon":-94.23000},{"lat":44.70000,"lon":-94.12000},{"lat":44.73000,"lon":-94.00000},{"lat":44.75000,"lon":-93.89000},{"lat":44.79000,"lon":-93.73000},{"lat":44.83000,"lon":-93.52000},{"lat":44.85000,"lon":-93.43000},{"lat":44.78000,"lon":-93.26000},{"lat":44.75000,"lon":-93.19000},{"lat":44.68000,"lon":-93.04000},{"lat":44.72000,"lon":-93.07000},{"lat":44.79000,"lon":-93.13000},{"lat":44.86000,"lon":-93.20000},{"lat":44.88000,"lon":-93.22000}]}\n'
            ],
        )

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_no_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "")

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_gzip_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "gzip")

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_compress_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "compress")

    @patch("main.open_connection", new_callable=CoroMock)
    @patch("main.Producer", new_callable=Mock)
    def test_deflate_compression(self, mock_kafkaproducer, mock_openconnection):
        self.compression(mock_kafkaproducer, mock_openconnection, "deflate")
