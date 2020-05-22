import sys
import os
import shutil
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import unittest
from unittest.mock import patch, Mock
import datetime
env = patch.dict('os.environ', {'DB_URL':'sqlite:///test/db/flights.db'})
#with env, patch('flask.Flask') as mock_flask:
from flask import Flask
with env, patch.object(Flask, 'run') as mock_flask:
    import app

class TestQueries(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def new_datetime_now(self, tz):
        return datetime.datetime(2020, 5, 15, 13, 10)

    def new_datetime_fromtimestamp(self, time, tz):
        return datetime.datetime(2020, 5, 15, 13, 10)

    def test_get_flight(self):
        with app.app.app_context():
            result = app.get_flight("CGEYQ-1589545563-3-1-67")
            self.assertEqual(result, {'id': 'CGEYQ-1589545563-3-1-67', 'added': datetime.datetime(2020, 5, 19, 15, 23, 39), 'changed': datetime.datetime(2020, 5, 19, 15, 23, 39), 'flight_number': 'CGEYQ', 'registration': 'CGEYQ', 'atc_ident': None, 'hexid': None, 'origin': 'CYZX', 'destination': 'CYQI', 'aircraft_type': 'C177', 'filed_ground_speed': None, 'filed_speed': 112, 'filed_altitude': 6000, 'true_cancel': None, 'route': 'CYZX..YZX..MUXEL..OMTIV..CYQI', 'status': 'F', 'actual_arrival_gate': None, 'estimated_arrival_gate': None, 'actual_departure_gate': None, 'estimated_departure_gate': None, 'actual_arrival_terminal': None, 'scheduled_arrival_terminal': None, 'actual_departure_terminal': None, 'scheduled_departure_terminal': None, 'baggage_claim': None, 'cancelled': None, 'filed_off': datetime.datetime(2020, 5, 15, 13, 15), 'actual_out': None, 'actual_off': None, 'actual_on': None, 'actual_in': None, 'estimated_out': None, 'estimated_off': datetime.datetime(2020, 5, 15, 13, 30), 'estimated_on': datetime.datetime(2020, 5, 15, 14, 18, 58), 'estimated_in': None, 'scheduled_out': None, 'scheduled_in': None, 'predicted_out': None, 'predicted_off': datetime.datetime(2020, 5, 15, 13, 30), 'predicted_on': datetime.datetime(2020, 5, 15, 14, 18, 58), 'predicted_in': None})

    @patch('app.datetime')
    def test_get_busiest_airports(self, mock_datetime):
        mock_datetime.now.side_effect = self.new_datetime_now
        mock_datetime.fromtimestamp.side_effect = self.new_datetime_fromtimestamp
        with app.app.app_context(), app.app.test_request_context('test', data={'query': 0}):
            result = app.get_busiest_airports().get_json()
            self.assertEqual(result, ['KIWA', 'KORD'])

    @patch('app.datetime')
    def test_airport_arrivals(self, mock_datetime):
        mock_datetime.now.side_effect = self.new_datetime_now
        with app.app.app_context():
           result = app.airport_arrivals("KBRD").get_json()
           self.assertEqual(result, [{'actual_arrival_gate': None, 'actual_arrival_terminal': None, 'actual_departure_gate': None, 'actual_departure_terminal': None, 'actual_in': None, 'actual_off': None, 'actual_on': 'Fri, 15 May 2020 13:27:01 GMT', 'actual_out': None, 'added': 'Tue, 19 May 2020 15:23:39 GMT', 'aircraft_type': None, 'atc_ident': None, 'baggage_claim': None, 'cancelled': None, 'changed': 'Tue, 19 May 2020 15:23:39 GMT', 'destination': 'KBRD', 'estimated_arrival_gate': None, 'estimated_departure_gate': None, 'estimated_in': None, 'estimated_off': None, 'estimated_on': None, 'estimated_out': None, 'filed_altitude': None, 'filed_ground_speed': None, 'filed_off': None, 'filed_speed': None, 'flight_number': 'BMJ64', 'hexid': None, 'id': 'BMJ64-1589543130-5-1-174', 'origin': 'KLJF', 'predicted_in': None, 'predicted_off': None, 'predicted_on': None, 'predicted_out': None, 'registration': 'N104BA', 'route': None, 'scheduled_arrival_terminal': None, 'scheduled_departure_terminal': None, 'scheduled_in': None, 'scheduled_out': None, 'status': None, 'true_cancel': None}])

    @patch('app.datetime')
    def test_airport_departures(self, mock_datetime):
        mock_datetime.now.side_effect = self.new_datetime_now
        with app.app.app_context():
            result = app.airport_departures("KORD").get_json()
            self.assertEqual(result, [{'actual_arrival_gate': None, 'actual_arrival_terminal': None, 'actual_departure_gate': None, 'actual_departure_terminal': None, 'actual_in': None, 'actual_off': None, 'actual_on': None, 'actual_out': 'Fri, 15 May 2020 14:10:45 GMT', 'added': 'Tue, 19 May 2020 15:23:39 GMT', 'aircraft_type': None, 'atc_ident': None, 'baggage_claim': None, 'cancelled': None, 'changed': 'Tue, 19 May 2020 15:23:39 GMT', 'destination': 'KDEN', 'estimated_arrival_gate': None, 'estimated_departure_gate': None, 'estimated_in': None, 'estimated_off': None, 'estimated_on': None, 'estimated_out': None, 'filed_altitude': None, 'filed_ground_speed': None, 'filed_off': None, 'filed_speed': None, 'flight_number': 'UAL2465', 'hexid': None, 'id': 'UAL2465-1589328337-fa-0001', 'origin': 'KORD', 'predicted_in': None, 'predicted_off': None, 'predicted_on': None, 'predicted_out': None, 'registration': None, 'route': None, 'scheduled_arrival_terminal': None, 'scheduled_departure_terminal': None, 'scheduled_in': None, 'scheduled_out': None, 'status': None, 'true_cancel': None}])

    @patch('app.datetime')
    def test_airport_enroute(self, mock_datetime):
        mock_datetime.now.side_effect = self.new_datetime_now
        with app.app.app_context():
            result = app.airport_enroute("CYQI").get_json()
            self.assertEqual(result, [{'actual_arrival_gate': None, 'actual_arrival_terminal': None, 'actual_departure_gate': None, 'actual_departure_terminal': None, 'actual_in': None, 'actual_off': None, 'actual_on': None, 'actual_out': None, 'added': 'Tue, 19 May 2020 15:23:39 GMT', 'aircraft_type': 'C177', 'atc_ident': None, 'baggage_claim': None, 'cancelled': None, 'changed': 'Tue, 19 May 2020 15:23:39 GMT', 'destination': 'CYQI', 'estimated_arrival_gate': None, 'estimated_departure_gate': None, 'estimated_in': None, 'estimated_off': 'Fri, 15 May 2020 13:30:00 GMT', 'estimated_on': 'Fri, 15 May 2020 14:18:58 GMT', 'estimated_out': None, 'filed_altitude': 6000, 'filed_ground_speed': None, 'filed_off': 'Fri, 15 May 2020 13:15:00 GMT', 'filed_speed': 112, 'flight_number': 'CGEYQ', 'hexid': None, 'id': 'CGEYQ-1589545563-3-1-67', 'origin': 'CYZX', 'predicted_in': None, 'predicted_off': 'Fri, 15 May 2020 13:30:00 GMT', 'predicted_on': 'Fri, 15 May 2020 14:18:58 GMT', 'predicted_out': None, 'registration': 'CGEYQ', 'route': 'CYZX..YZX..MUXEL..OMTIV..CYQI', 'scheduled_arrival_terminal': None, 'scheduled_departure_terminal': None, 'scheduled_in': None, 'scheduled_out': None, 'status': 'F', 'true_cancel': None}])

    @patch('app.datetime')
    def test_airport_scheduled(self, mock_datetime):
        mock_datetime.now.side_effect = self.new_datetime_now
        with app.app.app_context():
            result = app.airport_scheduled("CYZX").get_json()
            self.assertEqual(result, [{'actual_arrival_gate': None, 'actual_arrival_terminal': None, 'actual_departure_gate': None, 'actual_departure_terminal': None, 'actual_in': None, 'actual_off': None, 'actual_on': None, 'actual_out': None, 'added': 'Tue, 19 May 2020 15:23:39 GMT', 'aircraft_type': 'C177', 'atc_ident': None, 'baggage_claim': None, 'cancelled': None, 'changed': 'Tue, 19 May 2020 15:23:39 GMT', 'destination': 'CYQI', 'estimated_arrival_gate': None, 'estimated_departure_gate': None, 'estimated_in': None, 'estimated_off': 'Fri, 15 May 2020 13:30:00 GMT', 'estimated_on': 'Fri, 15 May 2020 14:18:58 GMT', 'estimated_out': None, 'filed_altitude': 6000, 'filed_ground_speed': None, 'filed_off': 'Fri, 15 May 2020 13:15:00 GMT', 'filed_speed': 112, 'flight_number': 'CGEYQ', 'hexid': None, 'id': 'CGEYQ-1589545563-3-1-67', 'origin': 'CYZX', 'predicted_in': None, 'predicted_off': 'Fri, 15 May 2020 13:30:00 GMT', 'predicted_on': 'Fri, 15 May 2020 14:18:58 GMT', 'predicted_out': None, 'registration': 'CGEYQ', 'route': 'CYZX..YZX..MUXEL..OMTIV..CYQI', 'scheduled_arrival_terminal': None, 'scheduled_departure_terminal': None, 'scheduled_in': None, 'scheduled_out': None, 'status': 'F', 'true_cancel': None}])
