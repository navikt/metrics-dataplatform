FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14-dev AS builder

ENV LANG=C.UTF-8
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/venv/bin:$PATH"

WORKDIR /app

RUN python -m venv --without-pip /app/venv
COPY requirements.txt requirements.txt
RUN pip --python /app/venv/bin/python install --no-cache-dir -r requirements.txt

FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.14 AS runner

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PATH="/app/venv/bin:$PATH"

COPY src src
COPY --from=builder /app/venv /app/venv

ENTRYPOINT ["python", "src/main.py"]
