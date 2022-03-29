#!/bin/bash

start=$(date +%s)

sleep 300

dbupdater_container=$(docker ps | grep firestarter_db-updater_1 | awk 'NF>1{print $NF}')
flights_count=$(docker exec -i ${dbupdater_container} sqlite3 /home/firestarter/app/db/flights.db 'SELECT COUNT(*) FROM flights')

timescaledb_container=$(docker ps | grep firestarter_timescaledb_1 | awk 'NF>1{print $NF}')
positions_count=$(docker exec -i ${timescaledb_container} psql -qAt -U postgres -c 'SELECT COUNT(*) FROM positions')

positions_time_processed=$(docker exec -i ${timescaledb_container} psql -qAt -U postgres -c 'SELECT MAX(extract(epoch from time)) - MIN(extract(epoch from time)) as time_diff FROM positions' | tr -d '\r')

end=$(date +%s)

echo "db-updater: ${dbupdater_container}"
echo "timescaledb: ${timescaledb_container}"
echo "---"

echo "Flights Count: ${flights_count}"
echo "Positions Count: ${positions_count}"
echo "Position catch-up rate: x$(($positions_time_processed / ($end - $start)))"

if [[ $flights_count < 45000 ]]; then
	echo "Flight count lower than threshold 45000"
	exit 1
fi

if [[ $positions_count < 200000 ]]; then
	echo "Position count lower than threshold 200000"
	exit 1
fi

exit 0