#!/bin/bash

sleep 5m
docker ps

flights_count=$(docker exec -it $(docker ps | grep firestarter_db-updater_1 | awk 'NF>1{print $NF}') sqlite3 /home/firestarter/app/db/flights.db 'SELECT COUNT(*) FROM flights')

positions_count=$(docker exec -it $(docker ps | grep firestarter_timescaledb_1 | awk 'NF>1{print $NF}') psql -qAt -U postgres -c 'SELECT COUNT(*) FROM positions')

echo "Flights Count: ${flights_count}"
echo "Positions Count: ${positions_count}"

if [[ $flights_count < 45000 ]]; then
	echo "Flight count lower than threshold 45000"
	exit 1
fi

if [[ $positions_count < 450000 ]]; then
	echo "Position count lower than threshold 450000"
	exit 1
fi
