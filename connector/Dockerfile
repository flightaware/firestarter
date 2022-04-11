FROM python:3.9-slim-bullseye

LABEL org.opencontainers.image.source=https://github.com/flightaware/firestarter

RUN apt-get update && apt-get install -y build-essential librdkafka-dev
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir app
WORKDIR /home/firestarter/app
COPY --chown=firestarter connector/requirements ./requirements
COPY --chown=firestarter connector/Makefile .

RUN make docker-setup
ENV VIRTUAL_ENV=./venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=firestarter connector/main.py .

CMD ["python3", "main.py"]

