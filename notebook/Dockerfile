FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      tini ca-certificates curl libpq5 \
    && rm -rf /var/lib/apt/lists/*

ARG NB_USER=jupyter
ARG NB_UID=1000
ARG NB_GID=1000

RUN set -eux; \
    getent group "${NB_GID}" >/dev/null || groupadd --gid "${NB_GID}" "${NB_USER}"; \
    id -u "${NB_USER}" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash \
      --uid "${NB_UID}" --gid "${NB_GID}" "${NB_USER}"

RUN mkdir -p /home/${NB_USER}/work /home/${NB_USER}/.duckdb /opt/duckdb/extensions && \
    chown -R ${NB_UID}:${NB_GID} /home/${NB_USER} /opt/duckdb

ENV JUPYTER_WORKDIR=/home/${NB_USER}/work
WORKDIR /home/${NB_USER}/work

RUN pip install --no-cache-dir \
      "jupyterlab>=4,<5" \
      "notebook>=7,<8" \
      "duckdb==1.4.*" \
      "jupysql>=0.10" \
      "duckdb-engine>=0.10" \
      "boto3==1.41.0"

COPY init_extensions.py /usr/local/bin/ducklake-bootstrap.py
COPY docker-entrypoint.sh  /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER ${NB_UID}:${NB_GID}

ENV JUPYTER_TOKEN=duck \
    JUPYTER_PORT=8888 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8888

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["python", "-m", "jupyterlab", "--ServerApp.ip=0.0.0.0", "--ServerApp.port=8888", "--ServerApp.allow_origin=*"]
