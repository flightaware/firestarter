#!/bin/bash

sleep 5m
docker ps

flights_count=$(docker exec -it $(docker ps | grep firestarter_db-updater_1 | awk 'NF>1{print $NF}') sqlite3 /home/firestarter/app/db/flights.db 'SELECT COUNT(*) FROM flights')

positions_count=$(docker exec -it $(docker ps | grep firestarter_timescaledb_1 | awk 'NF>1{print $NF}') psql -qAt -U postgres -c 'SELECT COUNT(*) FROM positions')

echo "${flights_count}"

echo "${positions_count}"