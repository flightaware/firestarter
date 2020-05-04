FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y libpq-dev gcc make
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir db-updater
WORKDIR /home/firestarter/db-updater
RUN mkdir db
COPY --chown=firestarter db-updater/requirements ./requirements
COPY --chown=firestarter db-updater/main.py .
COPY --chown=firestarter db-updater/Makefile .

RUN make docker-setup

ENTRYPOINT ["venv/bin/python3"]
CMD ["main.py"]
