FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y make
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir connector
WORKDIR /home/firestarter/connector
COPY --chown=firestarter connector/requirements ./requirements
COPY --chown=firestarter connector/main.py .
COPY --chown=firestarter connector/Makefile .

RUN make docker-setup

ENTRYPOINT ["venv/bin/python3"]
CMD ["main.py"]

