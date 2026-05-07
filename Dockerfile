# syntax=docker/dockerfile:1

FROM node:20-bookworm-slim AS topn-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY tools/preprocessing/topN_cpp ./tools/preprocessing/topN_cpp
RUN make -C tools/preprocessing/topN_cpp


FROM node:20-bookworm-slim AS runtime

ENV NODE_ENV=production \
    PORT=3000 \
    PYTHON_COMMAND=/opt/venv/bin/python \
    DB_PATH=/app/data/network_viz.db \
    DATA_PATH=/app/data/networks \
    INDEXES_PATH=/app/data/indexes \
    NODE_ATTRIBUTES_PATH=/app/data/nodes_attr \
    SPECIES_PATH=/app/data/NCBI_txID/NCBI_txID.csv \
    TAXON_NAMES_PATH=/app/data/NCBI_txID/NCBI_txID.csv \
    TAXON_TREE_PATH=/app/data/NCBI_txID/commontree.txt \
    EXPORTS_PATH=/app/data/exports \
    TEMP_DATA_PATH=/app/data/tmp \
    SUBNETWORK_SCRIPT_PATH=/app/tools/runtime/extract_subnetwork.py \
    SUBNETWORK_JOB_TEMP_PATH=/app/data/tmp/subnetwork-jobs \
    LOG_FILE=/app/logs/server.log

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-pip \
        python3-venv \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm ci --omit=dev \
    && npm cache clean --force

COPY requirements.txt ./
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

COPY backend ./backend
COPY frontend ./frontend
COPY tools ./tools
COPY --from=topn-builder /build/tools/bin/topn ./tools/bin/topn

RUN mkdir -p \
        /app/data/networks \
        /app/data/indexes \
        /app/data/nodes_attr \
        /app/data/NCBI_txID \
        /app/data/exports \
        /app/data/tmp \
        /app/logs \
    && chown -R node:node /app /opt/venv

USER node

EXPOSE 3000
VOLUME ["/app/data"]

ENTRYPOINT ["tini", "--"]
CMD ["npm", "start"]
