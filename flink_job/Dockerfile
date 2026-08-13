FROM flink:1.20-scala_2.12-java17

# The Flink base image has no Python runtime; PyFlink needs one.
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# pemja, PyFlink's C bridge between the Python and Java runtimes, publishes no
# manylinux aarch64 wheel, so arm64 has to compile it. It needs Python headers
# and the JDK headers already present in this image. The toolchain is removed
# again in the same layer so it never reaches the final image.
ENV JAVA_HOME=/opt/java/openjdk

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /usr/local/bin/uv

# The connector version encodes its target Flink version: 3.4.0-1.20 is
# connector 3.4.0 built for Flink 1.20. They are not interchangeable.
#
# The "sql" artifact is the shaded uber-jar: it bundles kafka-clients and its
# transitive deps, so it works for the DataStream API too and removes a second
# version to keep in sync.
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
RUN wget -nv -O /opt/flink/lib/flink-sql-connector-kafka.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

# Installed into the system interpreter rather than a venv: Flink spawns
# `python` directly for the Python UDF workers, and that process must be able
# to import pyflink without a venv on its PATH.
#
# Exported from uv.lock rather than `uv sync`, so the image gets exactly the
# locked versions while keeping the dependency layer cacheable.
COPY pyproject.toml uv.lock /tmp/deps/
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3-dev build-essential \
    && cd /tmp/deps \
    && uv export --frozen --only-group flink --no-hashes -o requirements.txt \
    && uv pip install --system --break-system-packages -r requirements.txt \
    && apt-get purge -y --auto-remove python3-dev build-essential \
    && rm -rf /tmp/deps /var/lib/apt/lists/*

# Shipped as a package, not a loose script, so the job's relative imports work.
# Submit with: -pyfs /opt/flink/usrlib -pym flink_job.job
COPY flink_job /opt/flink/usrlib/flink_job
