FROM python:3.8-slim-buster

RUN id -u firestarter || useradd -u 8081 firestarter -c "FIRESTARTER User" -m -s /bin/sh
USER firestarter
WORKDIR /home/firestarter

COPY --chown=firestarter main.py .

CMD ["python3", "main.py"]

