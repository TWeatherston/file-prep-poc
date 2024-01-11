FROM python:3.11-slim as base

RUN pip install poetry==1.5.1

COPY ["pyproject.toml", "poetry.lock", "/"]

RUN poetry install

COPY /src /src
