run:
	docker-compose build --parallel && docker-compose up

run-background:
	docker-compose build --parallel && docker-compose up -d

run-s3-exporter-workload:
	docker-compose pull kafka zookeeper db-updater
	docker-compose build connector s3-exporter
	docker-compose up -d kafka zookeeper
	sleep 5
	docker-compose --env-file ./.env -- up -d connector s3-exporter

docker-clean:
	docker-compose down --volumes
