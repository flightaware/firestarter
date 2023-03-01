import sys
import os
import shutil
import datetime
from sqlalchemy.sql import select

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock


class FakeMessage:
    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def error(self):
        return None


class TestInsertAndExpire(unittest.TestCase):
    # fmt: off
    _flight_msgs = [b'{"type": "keepalive", "serverTime": "1589808417", "pitr": "1589808413"}',
        b'{"pitr": "1589549426", "type": "flightplan", "ident": "CGEYQ", "aircrafttype": "C177", "alt": "6000", "dest": "CYQI", "edt": "1589549400", "eta": "1589552338", "facility_hash": "81E755935A704D47", "facility_name": "", "fdt": "1589548500", "id": "CGEYQ-1589545563-3-1-67", "orig": "CYZX", "predicted_off": "1589549400", "predicted_on": "1589552338", "reg": "CGEYQ", "route": "CYZX..YZX..MUXEL..OMTIV..CYQI", "speed": "112", "status": "F", "waypoints": [{"lat": 44.98, "lon": -64.92}, {"lat": 44.92, "lon": -65.1}, {"lat": 44.89, "lon": -65.18}, {"lat": 44.84, "lon": -65.28}, {"lat": 44.67, "lon": -65.62}, {"lat": 44.48, "lon": -65.88}, {"lat": 44.15, "lon": -65.98}, {"lat": 44.08, "lon": -66.01}, {"lat": 43.9, "lon": -66.06}, {"lat": 43.83, "lon": -66.09}], "ete": "2938"}',
        b'{"pitr": "1589549426", "type": "departure", "ident": "BLOCKED", "adt": "1589549420", "aircrafttype": "C172", "facility_hash": "F81DD9371A207384", "facility_name": "FlightAware ADS-B", "id": "BLOCKED-1589549420-adhoc-0", "orig": "KIWA", "reg": "BLOCKED", "synthetic": "1"}',
        b'{"pitr": "1589549426", "type": "arrival", "ident": "BMJ64", "dest": "KBRD", "facility_hash": "F3391B59517AE9FF", "facility_name": "FlightAware", "id": "BMJ64-1589543130-5-1-174", "orig": "KLJF", "reg": "N104BA", "synthetic": "1", "aat": "1589549221", "timeType": "estimated"}',
        b'{"pitr": "1589551286", "type": "cancellation", "ident": "CES2187", "aircrafttype": "A320", "dest": "ZSAM", "edt": "1589605500", "eta": "1589614800", "facility_hash": "F44B2C6C456D33FB", "facility_name": "Airline", "fdt": "1589605500", "id": "CES2187-1589431537-airline-0180", "orig": "ZLXY", "speed": "299", "status": "X", "trueCancel": "1", "ete": "9300"}',
        b'{"pitr": "1589549554", "type": "extendedFlightInfo", "ident": "SKW3284", "actual_departure_gate": "G11", "actual_departure_terminal": "3", "actual_out": "1589547780", "estimated_arrival_gate": "C3", "estimated_in": "1589553900", "estimated_out": "1589548200", "facility_hash": "F44B2C6C456D33FB", "facility_name": "Airline", "id": "SKW3284-1589345159-airline-0376", "scheduled_departure_terminal": "3", "scheduled_in": "1589554920", "scheduled_out": "1589548200"}',
        b'{"pitr": "1589551845", "type": "offblock", "ident": "UAL2465", "clock": "1589551845", "dest": "KDEN", "facility_hash": "23D67E4254EC60CD", "facility_name": "United Airlines", "id": "UAL2465-1589328337-fa-0001", "orig": "KORD"}',
        b'{"pitr": "1589551558", "type": "onblock", "ident": "CHH7691", "clock": "1589551547", "dest": "ZSOF", "facility_hash": "F44B2C6C456D33FB", "facility_name": "Airline", "id": "CHH7691-1589345160-airline-0022", "orig": "ZJSY"}',
        b'{"pitr": "1589551880", "type": "flifo", "ident": "UAL2465", "clock": "1589551845", "dest": "KHOU", "facility_hash": "23D67E4254EC60CD", "facility_name": "United Airlines", "id": "UAL2465-1589328337-fa-0001", "orig": "KORD"}',
    ]
    flight_msgs = [FakeMessage(msg) for msg in _flight_msgs]

    _position_msgs = [b'{"type": "keepalive", "serverTime": "1589808417", "pitr": "1589808413"}',
        b'{"pitr":"1591763773","type":"position","ident":"UEA2236","air_ground":"A","alt":"13800","altChange":" ","clock":"1591763767","dest":"ZUUU","facility_hash":"E4030EF8218DE21D","facility_name":"FlightAware ADS-B","id":"UEA2236-1591584600-schedule-0391","gs":"319","heading":"336","heading_magnetic":"336.1","hexid":"780BBB","lat":"29.84762","lon":"104.34529","mach":"0.484","orig":"ZGGG","pressure":"600","reg":"B9985","speed_ias":"250","speed_tas":"314","squawk":"4525","temperature":"4","temperature_quality":"1","updateType":"A"}',
        b'{"pitr":"1591763778","type":"position","ident":"CES5489","air_ground":"A","alt":"17700","altChange":" ","clock":"1591763763","dest":"ZPPP","facility_hash":"18249C69E5BC3F1B","facility_name":"FlightAware ADS-B","id":"CES5489-1591589400-schedule-0472","gs":"401","heading":"244","heading_magnetic":"250.0","hexid":"780D1B","lat":"31.43910","lon":"115.68351","mach":"0.648","orig":"ZSOF","pressure":"512","reg":"B1610","speed_ias":"312","speed_tas":"414","squawk":"5005","temperature":"-4","temperature_quality":"1","updateType":"A"}',
        b'{"pitr":"1591763786","type":"position","ident":"CAL782","air_ground":"A","alt":"34500","altChange":"C","clock":"1591763780","dest":"RCTP","facility_hash":"52446C6CE8E0B859","facility_name":"FlightAware ADS-B","id":"CAL782-1591588500-schedule-0187","gs":"494","heading":"36","heading_magnetic":"37.3","hexid":"899128","lat":"12.81155","lon":"108.19659","mach":"0.852","orig":"VVTS","pressure":"244","reg":"B18909","speed_ias":"297","speed_tas":"510","squawk":"5620","temperature":"-37","temperature_quality":"1","updateType":"A"}',
        b'{"pitr":"1591763795","type":"position","ident":"CWA926","air_ground":"A","alt":"23575","altChange":"C","clock":"1591763790","dest":"CYEG","facility_hash":"B566E7DB03CCFF2C","facility_name":"FlightAware ADS-B","id":"CWA926-1591760300-2-0-98","gs":"234","heading":"158","hexid":"C063CD","lat":"57.87987","lon":"-116.59763","orig":"CYOJ","reg":"CGLUQ","squawk":"5213","updateType":"A"}',
        b'{"pitr":"1591763814","type":"position","ident":"SOO7930","aircrafttype":"B77L","air_ground":"A","alt":"34000","altChange":" ","clock":"1591763809","dest":"RKSI","facility_hash":"C3DCCEF85E56D79D","facility_name":"FlightAware ADS-B","id":"SOO7930-1591724581-7-3-192","gs":"494","heading":"272","hexid":"A95CA4","lat":"64.62843","lon":"-143.16364","orig":"KJFK","reg":"N702GT","squawk":"3064","updateType":"A"}',
        b'{"pitr":"1591763825","type":"position","ident":"CES5489","air_ground":"A","alt":"38100","altChange":" ","clock":"1591763819","dest":"ZUUU","facility_hash":"E4030EF8218DE21D","facility_name":"FlightAware ADS-B","id":"CES5489-1591589400-schedule-0472","gs":"464","heading":"179","heading_magnetic":"184.9","hexid":"780D1B","lat":"29.50584","lon":"106.56178","mach":"0.804","orig":"ZSOF","pressure":"205","reg":"B1610","speed_ias":"255","speed_tas":"470","squawk":"4113","temperature":"-48","temperature_quality":"1","updateType":"A"}',
        b'{"pitr":"1591763834","type":"position","ident":"N517MT","aircrafttype":"EC35","air_ground":"A","alt":"1550","altChange":"D","clock":"1591763828","facility_hash":"A4C87376ACECA2CE","facility_name":"FlightAware ADS-B","id":"N517MT-1591762834-adhoc-0","gs":"119","heading":"13","hexid":"A67C95","lat":"33.58891","lon":"-97.21146","orig":"KDTO","reg":"N517MT","squawk":"1200","updateType":"A"}',
    ]
    position_msgs = [FakeMessage(msg) for msg in _position_msgs]

    expected_flights_results = [('CGEYQ-1589545563-3-1-67', 'CGEYQ', 'CGEYQ', None, None, 'CYZX', 'CYQI', 'C177', None, 112, None, 6000, None, 'CYZX..YZX..MUXEL..OMTIV..CYQI', 'F', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 30), datetime.datetime(2020, 5, 15, 14, 18, 58), None, datetime.datetime(2020, 5, 15, 13, 15), None, None, None, datetime.datetime(2020, 5, 15, 13, 30), datetime.datetime(2020, 5, 15, 14, 18, 58), None, None), ('BLOCKED-1589549420-adhoc-0', 'BLOCKED', 'BLOCKED', None, None, 'KIWA', None, 'C172', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 30, 20), None, None, None, None, None, None, None, None, None, None, None, None, None, None), ('BMJ64-1589543130-5-1-174', 'BMJ64', 'N104BA', None, None, 'KLJF', 'KBRD', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 27, 1), None, None, None, None, None, None, None, None, None, None, None, None, None), ('CES2187-1589431537-airline-0180', 'CES2187', None, None, None, 'ZLXY', 'ZSAM', 'A320', None, 299, None, None, True, None, 'X', None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 1, 26), None, None, None, None, None, datetime.datetime(2020, 5, 16, 5, 5), datetime.datetime(2020, 5, 16, 7, 40), None, datetime.datetime(2020, 5, 16, 5, 5), None, None, None, None, None, None, None), ('SKW3284-1589345159-airline-0376', 'SKW3284', None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'C3', 'G11', None, None, None, '3', '3', None, None, None, None, datetime.datetime(2020, 5, 15, 13, 3), None, None, None, datetime.datetime(2020, 5, 15, 13, 10), None, None, datetime.datetime(2020, 5, 15, 14, 45), None, datetime.datetime(2020, 5, 15, 13, 10), datetime.datetime(2020, 5, 15, 15, 2), None, None, None, None, None), ('UAL2465-1589328337-fa-0001', 'UAL2465', None, None, None, 'KORD', 'KDEN', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 10, 45), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None), ('CHH7691-1589345160-airline-0022', 'CHH7691', None, None, None, 'ZJSY', 'ZSOF', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 5, 47), None, None, None, None, None, None, None, None, None, None, None, None), ('UAL2465-1589328337-fa-0001', 'UAL2465', None, None, None, 'KORD', 'KDEN', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 10, 45), None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'KHOU')]
    expected_positions_results = [('UEA2236-1591584600-schedule-0391', datetime.datetime(2020, 6, 10, 4, 36, 7), '29.84762', '104.34529', 'E4030EF8218DE21D', 'FlightAware ADS-B', 'A', None, None, 13800, None, ' ', None, 'B9985', 'ZGGG', 'ZUUU', None, None, None, None, 319, '336', '336.1', None, '780BBB', '0.484', None, None, None, None, None, None, None, None, None, None, None, None, 600, None, 250, 314, 4525, 4, 1, None, None, None, None, None, None), ('CES5489-1591589400-schedule-0472', datetime.datetime(2020, 6, 10, 4, 36, 3), '31.43910', '115.68351', '18249C69E5BC3F1B', 'FlightAware ADS-B', 'A', None, None, 17700, None, ' ', None, 'B1610', 'ZSOF', 'ZPPP', None, None, None, None, 401, '244', '250.0', None, '780D1B', '0.648', None, None, None, None, None, None, None, None, None, None, None, None, 512, None, 312, 414, 5005, -4, 1, None, None, None, None, None, None), ('CAL782-1591588500-schedule-0187', datetime.datetime(2020, 6, 10, 4, 36, 20), '12.81155', '108.19659', '52446C6CE8E0B859', 'FlightAware ADS-B', 'A', None, None, 34500, None, 'C', None, 'B18909', 'VVTS', 'RCTP', None, None, None, None, 494, '36', '37.3', None, '899128', '0.852', None, None, None, None, None, None, None, None, None, None, None, None, 244, None, 297, 510, 5620, -37, 1, None, None, None, None, None, None), ('CWA926-1591760300-2-0-98', datetime.datetime(2020, 6, 10, 4, 36, 30), '57.87987', '-116.59763', 'B566E7DB03CCFF2C', 'FlightAware ADS-B', 'A', None, None, 23575, None, 'C', None, 'CGLUQ', 'CYOJ', 'CYEG', None, None, None, None, 234, '158', None, None, 'C063CD', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 5213, None, None, None, None, None, None, None, None), ('SOO7930-1591724581-7-3-192', datetime.datetime(2020, 6, 10, 4, 36, 49), '64.62843', '-143.16364', 'C3DCCEF85E56D79D', 'FlightAware ADS-B', 'A', None, 'B77L', 34000, None, ' ', None, 'N702GT', 'KJFK', 'RKSI', None, None, None, None, 494, '272', None, None, 'A95CA4', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 3064, None, None, None, None, None, None, None, None), ('CES5489-1591589400-schedule-0472', datetime.datetime(2020, 6, 10, 4, 36, 59), '29.50584', '106.56178', 'E4030EF8218DE21D', 'FlightAware ADS-B', 'A', None, None, 38100, None, ' ', None, 'B1610', 'ZSOF', 'ZUUU', None, None, None, None, 464, '179', '184.9', None, '780D1B', '0.804', None, None, None, None, None, None, None, None, None, None, None, None, 205, None, 255, 470, 4113, -48, 1, None, None, None, None, None, None), ('N517MT-1591762834-adhoc-0', datetime.datetime(2020, 6, 10, 4, 37, 8), '33.58891', '-97.21146', 'A4C87376ACECA2CE', 'FlightAware ADS-B', 'A', None, 'EC35', 1550, None, 'D', None, 'N517MT', 'KDTO', None, None, None, None, None, 119, '13', None, None, 'A67C95', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 1200, None, None, None, None, None, None, None, None)]
    # fmt: on

    def setUp(self):
        os.mkdir("db")

    def tearDown(self):
        shutil.rmtree("db")
        try:
            del sys.modules["main"]
        except KeyError:
            pass

    def no_timedelta(self, hours):
        return datetime.timedelta()

    def remove_changed_added_cols(self, table_entries):
        result_table_entries = []

        for entry in table_entries:
            entry.pop(1)
            entry.pop(1)
            result_table_entries.append(entry)
        return result_table_entries

    def run_and_check_table(self, end_empty, table, mock_kafkaconsumer):
        if table == "flights":
            mock_kafkaconsumer().poll.side_effect = self.flight_msgs
            expected_table_results = self.expected_flights_results
        elif table == "positions":
            mock_kafkaconsumer().poll.side_effect = self.position_msgs
            expected_table_results = self.expected_positions_results

        try:
            import main

            main.main()
        except StopIteration:
            pass

        with main.engine.begin() as conn:
            main.cache.flush(conn)
            rows_in_table = conn.execute(
                select([c for c in main.table.c if c.name != "changed" and c.name != "added"])
            )
            self.assertEqual(rows_in_table.fetchall(), expected_table_results)
        main._expire_old_from_table()
        expired_rows_in_table = main.engine.execute(
            select([c for c in main.table.c if c.name != "changed" and c.name != "added"])
        )

        if end_empty:
            self.assertEqual(expired_rows_in_table.fetchall(), [])
        else:
            self.assertEqual(expired_rows_in_table.fetchall(), expected_table_results)

    @patch.dict("os.environ", {"DB_URL": "sqlite:///db/flights.db", "TABLE": "flights"})
    @patch("main.timedelta")
    @patch("main.threading.Thread")
    @patch("main.Consumer")
    def test_insert_flights_then_expire_msgs(self, mock_kafkaconsumer, mock_thread, mock_timedelta):
        mock_timedelta.side_effect = self.no_timedelta
        self.run_and_check_table(1, "flights", mock_kafkaconsumer)

    @patch.dict("os.environ", {"DB_URL": "sqlite:///db/flights.db", "TABLE": "flights"})
    @patch("main.threading.Thread")
    @patch("main.Consumer")
    def test_insert_flights_no_expired_msgs(self, mock_kafkaconsumer, mock_thread):
        self.run_and_check_table(0, "flights", mock_kafkaconsumer)

    @patch.dict("os.environ", {"DB_URL": "sqlite:///db/positions.db", "TABLE": "positions"})
    @patch("main.timedelta")
    @patch("main.threading.Thread")
    @patch("main.Consumer")
    def test_insert_positions_then_expire_msgs(
        self, mock_kafkaconsumer, mock_thread, mock_timedelta
    ):
        mock_timedelta.side_effect = self.no_timedelta
        self.run_and_check_table(1, "positions", mock_kafkaconsumer)

    @patch.dict("os.environ", {"DB_URL": "sqlite:///db/positions.db", "TABLE": "positions"})
    @patch("main.threading.Thread")
    @patch("main.Consumer")
    def test_insert_positions_no_expired_msgs(self, mock_kafkaconsumer, mock_thread):
        self.run_and_check_table(0, "positions", mock_kafkaconsumer)
