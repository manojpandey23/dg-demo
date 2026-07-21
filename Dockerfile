# ── Stage 1: Build ──
FROM python:3.12-slim AS builder

ARG INSTALL_EXTRAS=""

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
COPY framework/ framework/

RUN if [ -n "$INSTALL_EXTRAS" ]; then \
      uv sync --no-dev --frozen --extra "$INSTALL_EXTRAS"; \
    else \
      uv sync --no-dev --frozen; \
    fi

# ── Stage 2: Runtime ──
FROM python:3.12-slim AS runtime

RUN groupadd -r dagster && useradd -r -g dagster -d /opt/dagster dagster

WORKDIR /opt/dagster

COPY --from=builder /app/.venv /opt/dagster/.venv
COPY framework/ /opt/dagster/framework/
COPY pyproject.toml /opt/dagster/

ENV PATH="/opt/dagster/.venv/bin:$PATH"
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1

RUN mkdir -p $DAGSTER_HOME && chown -R dagster:dagster /opt/dagster

USER dagster

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import framework; print('ok')" || exit 1

EXPOSE 3000 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000"]
