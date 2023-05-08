##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
FROM --platform=$build_for python:3.8.16-slim-bullseye as base

# System setup
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple

# Set docker basics
WORKDIR /usr/app
VOLUME /usr/app
ENTRYPOINT ["dbt"]


##
# dbt-odps
##
FROM base as dbt-odps
RUN python -m pip install --no-cache-dir dbt-odps==0.0.1 -i https://pypi.tuna.tsinghua.edu.cn/simple
