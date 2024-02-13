FROM python:3.11

RUN pip install pandas

WORKDIR /pipeline
COPY pipeline.py pipeline.py

ENTRYPOINT ["python", "pipeline.py"]
