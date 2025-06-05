# Stage 1: Base - Install uv using the official astral.sh script
# Using a recent Python slim image as a base
FROM python:3.12-slim AS uv-base

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# Verify uv installation by printing its version.
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
CMD ["uv", "run", "uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8018"]

