FROM python:3.8-slim-buster

LABEL org.opencontainers.image.source https://github.com/maryryang2/firestarter

RUN apt-get update && \
	apt-get install -y libpq-dev gcc npm make && \
	npm install npm@latest -g
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter Makefile.inc .

RUN mkdir app
WORKDIR /home/firestarter/app

COPY --chown=firestarter fids/frontend/client frontend
RUN cd frontend && \
	npm install --no-optional && \
	npm cache clean --force && \
	npm run build && \
	rm -rf node_modules

RUN mkdir db
COPY --chown=firestarter fids/requirements ./requirements
COPY --chown=firestarter fids/Makefile .

ENV FLASK_APP=app.py
ENV FLASK_ENV=development

RUN make docker-setup
ENV VIRTUAL_ENV=./venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY --chown=firestarter fids/app.py .

CMD ["python3", "app.py"]
