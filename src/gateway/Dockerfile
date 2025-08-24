FROM python:3.11-slim

WORKDIR /app

COPY src/gateway .
COPY utility ./utility

RUN pip install fastapi[all]
RUN pip install uvicorn[standard]
RUN pip install minio
RUN pip install confluent-kafka
RUN pip install pydantic
RUN pip install duckdb>=1.2.2
RUN pip install pyarrow
RUN pip install numpy
RUN pip install deltalake


CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]