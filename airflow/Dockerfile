FROM apache/airflow:2.8.1
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
COPY key_admin_service_account.json /gcp_adc/key_admin_service_account.json
# COPY setup.py setup.py
ENV GOOGLE_APPLICATION_CREDENTIALS /gcp_adc/key_admin_service_account.json
# ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/plugin/my_modules"