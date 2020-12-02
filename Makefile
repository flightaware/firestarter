run:
	docker-compose build --parallel && docker-compose up

run-background:
	docker-compose build --parallel && docker-compose up -d
