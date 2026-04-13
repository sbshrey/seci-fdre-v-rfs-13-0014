FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SECI_FDRE_V_WORKSPACE=/workspace
ENV SECI_FDRE_V_SOURCE_CONFIG=/app/config/project.yaml

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY seci_fdre_v_model /app/seci_fdre_v_model
COPY config /app/config
COPY data /app/data
COPY main.py /app/main.py

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir .

EXPOSE 8000

CMD ["waitress-serve", "--host=0.0.0.0", "--port=8000", "--call", "seci_fdre_v_model.web.app:create_app"]
