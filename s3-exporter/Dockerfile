FROM python:3.9-slim-bullseye

LABEL org.opencontainers.image.source=https://github.com/flightaware/firestarter

RUN apt-get update && apt-get install -y build-essential librdkafka-dev
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir app
WORKDIR /home/firestarter/app
COPY --chown=firestarter s3-exporter/requirements ./requirements
COPY --chown=firestarter s3-exporter/Makefile .

RUN make docker-setup
USER firestarter

ENV VIRTUAL_ENV=./venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=firestarter s3-exporter/main.py .

CMD ["python3", "main.py"]
