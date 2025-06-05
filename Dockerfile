# Stage 1: Base - Install uv using the official astral.sh script
FROM python:3.12-slim AS uv-base
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"
RUN uv --version

# Stage 2: Builder - Create virtual environment and install dependencies
FROM uv-base AS builder
WORKDIR /app
ADD . /app
RUN uv --version
RUN uv sync

# Stage 3: Runner - Final stage with the application
FROM python:3.12-slim AS runner
WORKDIR /app
COPY --from=builder /app/.venv ./.venv
COPY --from=uv-base /root/.local/bin/uv /root/.local/bin/uv
ADD . /app
ENV PATH="/root/.local/bin:${PATH}"

EXPOSE 8018
CMD ["uv", "run", "python", "src/api.py"]

