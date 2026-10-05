ARG AIRFLOW_VERSION=3.1.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

COPY requirements.txt /tmp/requirements.txt

RUN python -m venv /home/airflow/pipeline-venv \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
    && mkdir -p /home/airflow/.cache/huggingface