FROM python:3.8-slim-buster

LABEL org.opencontainers.image.source=https://github.com/flightaware/firestarter

RUN apt-get update && apt-get install -y libpq-dev gcc make sqlite3 postgresql-client
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir app
WORKDIR /home/firestarter/app
RUN mkdir db
COPY --chown=firestarter db-updater/requirements ./requirements
COPY --chown=firestarter db-updater/Makefile .

RUN make docker-setup
ENV VIRTUAL_ENV=./venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=firestarter db-updater/main.py .

CMD ["python3", "main.py"]
