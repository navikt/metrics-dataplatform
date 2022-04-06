FROM navikt/python:3.8

USER root

COPY . .
COPY cloud-sql-python-connector/ ./cloud-sql-python-connector

RUN pip install -r requirements.txt

RUN cd cloud-sql-python-connector && \
    pip install .

USER apprunner

CMD ["python", "pipeline/main.py"]