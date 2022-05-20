FROM navikt/python:3.8

USER root

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts scripts/

USER apprunner

CMD ["python", "etl/alt_source_stage.py"]