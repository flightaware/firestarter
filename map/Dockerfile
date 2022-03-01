FROM python:3.8-slim-buster

LABEL org.opencontainers.image.source=https://github.com/flightaware/firestarter

RUN apt-get update && \
	apt-get install -y make
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir app
WORKDIR /home/firestarter/app

COPY --chown=firestarter map/requirements ./requirements
COPY --chown=firestarter map/Makefile .

ENV FLASK_APP=app.py
ENV FLASK_ENV=development

RUN make docker-setup
ENV VIRTUAL_ENV=./venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=firestarter map/frontend frontend
COPY --chown=firestarter map/app.py .

CMD ["python3", "app.py"]
