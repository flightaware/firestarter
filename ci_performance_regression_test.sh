#!/bin/bash

start=$(date +%s)

sleep 300

dbupdater_container=$(docker ps | grep firestarter_db-updater_1 | awk 'NF>1{print $NF}')
flights_count=$(docker exec -i ${dbupdater_container} sqlite3 /home/firestarter/app/db/flights.db 'SELECT COUNT(*) FROM flights')

timescaledb_container=$(docker ps | grep firestarter_timescaledb_1 | awk 'NF>1{print $NF}')
positions_count=$(docker exec -i ${timescaledb_container} psql -qAt -U postgres -c 'SELECT COUNT(*) FROM positions')

positions_time_processed=$(docker exec -i ${timescaledb_container} psql -qAt -U postgres -c 'SELECT MAX(extract(epoch from time)) - MIN(extract(epoch from time)) as time_diff FROM positions' | tr -d '\r')

end=$(date +%s)

echo "Flights Count: ${flights_count}"
echo "Positions Count: ${positions_count}"

if [[ $flights_count -lt 45000 ]]; then
	echo "Flight count lower than threshold 45000"
	docker-compose logs
	exit 1
fi

if [[ $positions_count -lt 200000 ]]; then
	echo "Position count lower than threshold 200000"
	docker-compose logs
	exit 1
fi

echo "Position catch-up rate: x$(($positions_time_processed / ($end - $start)))"

exit 0
