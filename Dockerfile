FROM apache/airflow:slim-3.1.1-python3.10

# --- Instalação do Chromium ---
USER root
RUN apt-get update \
    && apt-get install -y chromium chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# -----------------------------------

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
# DAGs embutidas na imagem: com KubernetesExecutor cada task é um pod novo, sem o bind mount do compose
COPY dags/ /opt/airflow/dags/
