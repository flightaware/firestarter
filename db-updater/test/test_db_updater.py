import sys
import os
import shutil
import datetime
from sqlalchemy.sql import select

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock

env = patch.dict("os.environ", {"DB_URL": "sqlite:///db/flights.db"})
with env:
    import main


class TestInsertAndExpire(unittest.TestCase):
    # fmt: off
    msgs = [type('', (), {"value": b'{"type": "keepalive", "serverTime": "1589808417", "pitr": "1589808413"}'}),
        type('', (), {"value": b'{"pitr": "1589549426", "type": "flightplan", "ident": "CGEYQ", "aircrafttype": "C177", "alt": "6000", "dest": "CYQI", "edt": "1589549400", "eta": "1589552338", "facility_hash": "81E755935A704D47", "facility_name": "", "fdt": "1589548500", "id": "CGEYQ-1589545563-3-1-67", "orig": "CYZX", "predicted_off": "1589549400", "predicted_on": "1589552338", "reg": "CGEYQ", "route": "CYZX..YZX..MUXEL..OMTIV..CYQI", "speed": "112", "status": "F", "waypoints": [{"lat": 44.98, "lon": -64.92}, {"lat": 44.92, "lon": -65.1}, {"lat": 44.89, "lon": -65.18}, {"lat": 44.84, "lon": -65.28}, {"lat": 44.67, "lon": -65.62}, {"lat": 44.48, "lon": -65.88}, {"lat": 44.15, "lon": -65.98}, {"lat": 44.08, "lon": -66.01}, {"lat": 43.9, "lon": -66.06}, {"lat": 43.83, "lon": -66.09}], "ete": "2938"}'}),
        type('', (), {"value": b'{"pitr": "1589549426", "type": "departure", "ident": "BLOCKED", "adt": "1589549420", "aircrafttype": "C172", "facility_hash": "F81DD9371A207384", "facility_name": "FlightAware ADS-B", "id": "BLOCKED-1589549420-adhoc-0", "orig": "KIWA", "reg": "BLOCKED", "synthetic": "1"}'}),
        type('', (), {"value": b'{"pitr": "1589549426", "type": "arrival", "ident": "BMJ64", "dest": "KBRD", "facility_hash": "F3391B59517AE9FF", "facility_name": "FlightAware", "id": "BMJ64-1589543130-5-1-174", "orig": "KLJF", "reg": "N104BA", "synthetic": "1", "aat": "1589549221", "timeType": "estimated"}'}),
        type('', (), {"value": b'{"pitr": "1589551286", "type": "cancellation", "ident": "CES2187", "aircrafttype": "A320", "dest": "ZSAM", "edt": "1589605500", "eta": "1589614800", "facility_hash": "F44B2C6C456D33FB", "facility_name": "Airline", "fdt": "1589605500", "id": "CES2187-1589431537-airline-0180", "orig": "ZLXY", "speed": "299", "status": "X", "trueCancel": "1", "ete": "9300"}'}),
        type('', (), {"value": b'{"pitr": "1589549554", "type": "extendedFlightInfo", "ident": "SKW3284", "actual_departure_gate": "G11", "actual_departure_terminal": "3", "actual_out": "1589547780", "estimated_arrival_gate": "C3", "estimated_in": "1589553900", "estimated_out": "1589548200", "facility_hash": "F44B2C6C456D33FB", "facility_name": "Airline", "id": "SKW3284-1589345159-airline-0376", "scheduled_departure_terminal": "3", "scheduled_in": "1589554920", "scheduled_out": "1589548200"}'}),
        type('', (), {"value": b'{"pitr": "1589551845", "type": "offblock", "ident": "UAL2465", "clock": "1589551845", "dest": "KDEN", "facility_hash": "23D67E4254EC60CD", "facility_name": "United Airlines", "id": "UAL2465-1589328337-fa-0001", "orig": "KORD"}'}),
        type('', (), {"value": b'{"pitr": "1589551558", "type": "onblock", "ident": "CHH7691", "clock": "1589551547", "dest": "ZSOF", "facility_hash": "F44B2C6C456D33FB", "facility_name": "Airline", "id": "CHH7691-1589345160-airline-0022", "orig": "ZJSY"}'}),
    ]

    expected_flight_table_results = [('CGEYQ-1589545563-3-1-67', 'CGEYQ', 'CGEYQ', None, None, 'CYZX', 'CYQI', 'C177', None, 112, 6000, None, 'CYZX..YZX..MUXEL..OMTIV..CYQI', 'F', None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 15), None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 30), datetime.datetime(2020, 5, 15, 14, 18, 58), None, None, None, None, datetime.datetime(2020, 5, 15, 13, 30), datetime.datetime(2020, 5, 15, 14, 18, 58), None), ('BLOCKED-1589549420-adhoc-0', 'BLOCKED', 'BLOCKED', None, None, 'KIWA', None, 'C172', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 30, 20), None, None, None, None, None, None, None, None, None, None, None, None), ('BMJ64-1589543130-5-1-174', 'BMJ64', 'N104BA', None, None, 'KLJF', 'KBRD', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 13, 27, 1), None, None, None, None, None, None, None, None, None, None, None), ('CES2187-1589431537-airline-0180', 'CES2187', None, None, None, 'ZLXY', 'ZSAM', 'A320', None, 299, None, True, None, 'X', None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 1, 26), datetime.datetime(2020, 5, 16, 5, 5), None, None, None, None, None, datetime.datetime(2020, 5, 16, 5, 5), datetime.datetime(2020, 5, 16, 7, 40), None, None, None, None, None, None, None), ('SKW3284-1589345159-airline-0376', 'SKW3284', None, None, None, None, None, None, None, None, None, None, None, None, None, 'C3', 'G11', None, None, None, '3', '3', None, None, None, datetime.datetime(2020, 5, 15, 13, 3), None, None, None, datetime.datetime(2020, 5, 15, 13, 10), None, None, datetime.datetime(2020, 5, 15, 14, 45), datetime.datetime(2020, 5, 15, 13, 10), datetime.datetime(2020, 5, 15, 15, 2), None, None, None, None), ('UAL2465-1589328337-fa-0001', 'UAL2465', None, None, None, 'KORD', 'KDEN', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 10, 45), None, None, None, None, None, None, None, None, None, None, None, None, None), ('CHH7691-1589345160-airline-0022', 'CHH7691', None, None, None, 'ZJSY', 'ZSOF', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, datetime.datetime(2020, 5, 15, 14, 5, 47), None, None, None, None, None, None, None, None, None, None)]
    # fmt: on

    def setUp(self):
        os.mkdir("db")

    def tearDown(self):
        shutil.rmtree("db")

    def no_timedelta(self, hours):
        return datetime.timedelta()

    def remove_changed_added_cols(self, table_entries):
        result_table_entries = []

        for entry in table_entries:
            entry.pop(1)
            entry.pop(1)
            result_table_entries.append(entry)
        return result_table_entries

    def run_and_check_flights_table(self, end_empty, mock_kafkaconsumer):
        mock_kafkaconsumer.return_value = self.msgs

        main.main()

        with main.engine.begin() as conn:
            main._flush_cache(conn)
            flights_in_table = conn.execute(
                select([c for c in main.flights.c if c.name != "changed" and c.name != "added"])
            )
            # out_rows = self.remove_changed_added_cols(flights_in_table.fetchall())
            self.assertEqual(flights_in_table.fetchall(), self.expected_flight_table_results)
        main._expire_old_flights()
        expired_flights_in_table = main.engine.execute(
            select([c for c in main.flights.c if c.name != "changed" and c.name != "added"])
        )

        if end_empty:
            self.assertEqual(expired_flights_in_table.fetchall(), [])
        else:
            self.assertEqual(
                expired_flights_in_table.fetchall(), self.expected_flight_table_results
            )

    @patch("main.timedelta")
    @patch("main.threading.Thread")
    @patch("main.KafkaConsumer")
    def test_insert_then_expire_msgs(self, mock_kafkaconsumer, mock_thread, mock_timedelta):
        mock_timedelta.side_effect = self.no_timedelta
        self.run_and_check_flights_table(1, mock_kafkaconsumer)

    @patch("main.threading.Thread")
    @patch("main.KafkaConsumer")
    def test_insert_no_expired_msgs(self, mock_kafkaconsumer, mock_thread):
        self.run_and_check_flights_table(0, mock_kafkaconsumer)
