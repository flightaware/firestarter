FROM python:3.8-slim-buster

RUN apt-get update && \
	apt-get install -y libpq-dev gcc npm && \
	npm install npm@latest -g
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter frontend/client frontend
RUN cd frontend && \
	npm install --no-optional && \
	npm cache clean --force && \
	npm run build && \
	rm -rf node_modules

ENV VIRTUAL_ENV=./venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN mkdir db

COPY --chown=firestarter requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=firestarter app.py .

ENV FLASK_APP=app.py
ENV FLASK_ENV=development
CMD ["python3", "app.py"]
