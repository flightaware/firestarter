#!/bin/bash

sleep 5m
docker ps

flights_count=$(docker exec -it firestarter_db-updater_1 sqlite3 /home/firestarter/app/db/flights.db 'SELECT COUNT(*) FROM flights')

positions_count=$(docker exec -it firestarter_timescaledb_1 psql -qAt -U postgres -c 'SELECT COUNT(*) FROM positions')

echo "${flights_count}"

echo "${positions_count}"