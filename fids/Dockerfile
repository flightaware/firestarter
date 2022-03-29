FROM python:3.9-slim-bullseye

LABEL org.opencontainers.image.source=https://github.com/flightaware/firestarter

RUN apt-get update && \
	apt-get install -y libpq-dev build-essential
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir app
WORKDIR /home/firestarter/app

RUN mkdir db
COPY --chown=firestarter fids/requirements ./requirements
COPY --chown=firestarter fids/Makefile .

ENV FLASK_APP=app.py
ENV FLASK_ENV=development

RUN make docker-setup
ENV VIRTUAL_ENV=./venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=firestarter fids/app.py .
COPY --chown=firestarter fids/trig.py .

CMD ["python3", "app.py"]
