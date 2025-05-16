FROM astrocrpublic.azurecr.io/runtime:3.0-2

COPY dbt-requirements.txt ./
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate