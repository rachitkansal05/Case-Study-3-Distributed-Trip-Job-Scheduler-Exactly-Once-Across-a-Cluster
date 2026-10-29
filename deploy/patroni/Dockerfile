# Patroni on the official PostgreSQL 16 image (Debian bookworm).
# Patroni is installed into a virtualenv because bookworm's system Python is "externally managed".
FROM postgres:16

ARG PATRONI_VERSION=4.0.4

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-venv curl; \
    python3 -m venv /opt/patroni; \
    /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3,psycopg3]==${PATRONI_VERSION}"; \
    ln -s /opt/patroni/bin/patroni /usr/local/bin/patroni; \
    ln -s /opt/patroni/bin/patronictl /usr/local/bin/patronictl; \
    apt-get purge -y --auto-remove; \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/patroni-entrypoint.sh
COPY post-bootstrap.sh /usr/local/bin/patroni-post-bootstrap.sh
RUN chmod 0755 /usr/local/bin/patroni-entrypoint.sh /usr/local/bin/patroni-post-bootstrap.sh \
    && mkdir -p /var/lib/postgresql/data \
    && chown -R postgres:postgres /var/lib/postgresql

# Patroni refuses to run Postgres as root.
USER postgres
EXPOSE 5432 8008
ENTRYPOINT ["/usr/local/bin/patroni-entrypoint.sh"]
