run:
	docker-compose build --parallel && docker-compose up

run-background:
	docker-compose build --parallel && docker-compose up -d

run-s3-exporter-workload:
	docker-compose up -d kafka zookeeper
	sleep 5
	docker-compose up -d connector s3-exporter summary_connector db-summarizer s3-exporter-flights-summary

docker-clean:
	docker-compose down --volumes
