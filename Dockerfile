FROM apache/spark:4.0.0-python3

USER root

WORKDIR /app

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /app/requirements.txt

RUN python3 -m pip install \
    --no-cache-dir \
    --default-timeout=300 \
    -r /app/requirements.txt

USER spark

CMD ["tail", "-f", "/dev/null"]