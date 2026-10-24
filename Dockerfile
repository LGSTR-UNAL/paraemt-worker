FROM python:3.12-bookworm

WORKDIR /opt/paraemt-worker
RUN apt-get update && apt-get install -y --no-install-recommends git librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG PARAEMT_COMMIT=d79d735a4a587d56c5b88187d1a499195b6b2b84
RUN git clone https://github.com/NatLabRockies/ParaEMT_public.git /opt/paraemt/ParaEMT_public \
    && git -C /opt/paraemt/ParaEMT_public checkout ${PARAEMT_COMMIT}

COPY src/ ./src/
ENV PYTHONPATH=/opt/paraemt-worker/src \
    PARAEMT_HOME=/opt/paraemt/ParaEMT_public \
    PARAEMT_COMMIT=${PARAEMT_COMMIT} \
    NUMBA_CACHE_DIR=/tmp/numba_cache \
    NUMBA_NUM_THREADS=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1

RUN useradd --system --no-create-home app && chown -R app:app /opt/paraemt-worker /opt/paraemt
USER app
CMD ["python", "/opt/paraemt-worker/src/main.py"]
