# PROD — bake the released provider wheel into an image.
#
# PROD's job is the one thing no other environment does: exercise the *packaged
# artifact* rather than an editable source install. That is also what makes
# rollback meaningful — you rebuild with the previous wheel and redeploy.
#
# Build:
#   docker build --build-arg WHEEL=apache_airflow_providers_apache_kafka-1.15.1-py3-none-any.whl \
#                -t airflow-kafka:1.15.1 .

FROM apache/airflow:3.3.1

ARG WHEEL
COPY dist/${WHEEL} /tmp/${WHEEL}

USER airflow
RUN pip install --no-cache-dir "/tmp/${WHEEL}" && rm -f "/tmp/${WHEEL}"
