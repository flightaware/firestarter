import sys
import os
import shutil
from datetime import timedelta
from sqlalchemy.sql import select
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock
env = patch.dict('os.environ', {'DB_URL':'sqlite:///db/flights.db'})
with env:
	import main

class TestInsertAndExpire(unittest.TestCase):
    def setUp(self):
        os.mkdir('db')

    def tearDown(self):
        shutil.rmtree('db')

    def no_timedelta(self, hours):
        return timedelta()

    @patch('main.timedelta')
    def test_insert_then_expire_msgs(self, mock_timedelta):
        mock_timedelta.side_effect = self.no_timedelta

        main.setup_sqlite()
        main.meta.create_all(main.engine)

        processor_functions = {
            "arrival": main.process_arrival_message,
            "departure": main.process_departure_message,
            "cancellation": main.process_cancellation_message,
            "offblock": main.process_offblock_message,
            "onblock": main.process_onblock_message,
            "extendedFlightInfo": main.process_extended_flight_info_message,
            "flightplan": main.process_flightplan_message,
            "keepalive": main.process_keepalive_message,
        }

        msgs = [{'type': 'keepalive', 'serverTime': '1589808417', 'pitr': '1589808413'},
            {'pitr': '1589549426', 'type': 'flightplan', 'ident': 'CGEYQ', 'aircrafttype': 'C177', 'alt': '6000', 'dest': 'CYQI', 'edt': '1589549400', 'eta': '1589552338', 'facility_hash': '81E755935A704D47', 'facility_name': '', 'fdt': '1589548500', 'id': 'CGEYQ-1589545563-3-1-67', 'orig': 'CYZX', 'predicted_off': '1589549400', 'predicted_on': '1589552338', 'reg': 'CGEYQ', 'route': 'CYZX..YZX..MUXEL..OMTIV..CYQI', 'speed': '112', 'status': 'F', 'waypoints': [{'lat': 44.98, 'lon': -64.92}, {'lat': 44.92, 'lon': -65.1}, {'lat': 44.89, 'lon': -65.18}, {'lat': 44.84, 'lon': -65.28}, {'lat': 44.67, 'lon': -65.62}, {'lat': 44.48, 'lon': -65.88}, {'lat': 44.15, 'lon': -65.98}, {'lat': 44.08, 'lon': -66.01}, {'lat': 43.9, 'lon': -66.06}, {'lat': 43.83, 'lon': -66.09}], 'ete': '2938'},
            {'pitr': '1589549426', 'type': 'departure', 'ident': 'BLOCKED', 'adt': '1589549420', 'aircrafttype': 'C172', 'facility_hash': 'F81DD9371A207384', 'facility_name': 'FlightAware ADS-B', 'id': 'BLOCKED-1589549420-adhoc-0', 'orig': 'KIWA', 'reg': 'BLOCKED', 'synthetic': '1'},
            {'pitr': '1589549426', 'type': 'arrival', 'ident': 'BMJ64', 'dest': 'KBRD', 'facility_hash': 'F3391B59517AE9FF', 'facility_name': 'FlightAware', 'id': 'BMJ64-1589543130-5-1-174', 'orig': 'KLJF', 'reg': 'N104BA', 'synthetic': '1', 'aat': '1589549221', 'timeType': 'estimated'},
            {'pitr': '1589551286', 'type': 'cancellation', 'ident': 'CES2187', 'aircrafttype': 'A320', 'dest': 'ZSAM', 'edt': '1589605500', 'eta': '1589614800', 'facility_hash': 'F44B2C6C456D33FB', 'facility_name': 'Airline', 'fdt': '1589605500', 'id': 'CES2187-1589431537-airline-0180', 'orig': 'ZLXY', 'speed': '299', 'status': 'X', 'trueCancel': '1', 'ete': '9300'},
            {'pitr': '1589549554', 'type': 'extendedFlightInfo', 'ident': 'SKW3284', 'actual_departure_gate': 'G11', 'actual_departure_terminal': '3', 'actual_out': '1589547780', 'estimated_arrival_gate': 'C3', 'estimated_in': '1589553900', 'estimated_out': '1589548200', 'facility_hash': 'F44B2C6C456D33FB', 'facility_name': 'Airline', 'id': 'SKW3284-1589345159-airline-0376', 'scheduled_departure_terminal': '3', 'scheduled_in': '1589554920', 'scheduled_out': '1589548200'},
            {'pitr': '1589551845', 'type': 'offblock', 'ident': 'UAL2465', 'clock': '1589551845', 'dest': 'KDEN', 'facility_hash': '23D67E4254EC60CD', 'facility_name': 'United Airlines', 'id': 'UAL2465-1589328337-fa-0001', 'orig': 'KORD'},
            {'pitr': '1589551558', 'type': 'onblock', 'ident': 'CHH7691', 'clock': '1589551547', 'dest': 'ZSOF', 'facility_hash': 'F44B2C6C456D33FB', 'facility_name': 'Airline', 'id': 'CHH7691-1589345160-airline-0022', 'orig': 'ZJSY'},
        ]

        for msg in msgs: 
            processor_functions.get(msg["type"], main.process_unknown_message)(msg)

        with main.engine.begin() as conn:
            main._flush_cache(conn)
            print("flushed!")
            flights_in_table = conn.execute(select([main.flights]))
            for flight in flights_in_table:
                print(flight)

        main._expire_old_flights()
        print("expired!")
        expired_flights_in_table = main.engine.execute(select([main.flights]))
        for expired_flight in expired_flights_in_table:
                print(expired_flight)
