FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y libpq-dev gcc
RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

ENV VIRTUAL_ENV=./venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN mkdir db

COPY --chown=firestarter requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=firestarter main.py .

CMD ["python3", "main.py"]
