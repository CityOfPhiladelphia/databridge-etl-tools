#FROM ubuntu:16.04
#FROM python:3.6.15-slim-bullseye
#FROM python:3.7.12-slim-buster
#FROM python:3.8.15-slim-buster
FROM python:3.9.23-slim-bullseye

# Add our worker users custom binaries to the path, some python packages are installed here.
ENV PATH="/home/worker/.local/bin:${PATH}"
#ENV PYTHONPATH="/home/worker/.local/lib/python3.7/site-packages:${PYTHONPATH}"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Solve annoying locale problems in docker
# C.UTF-8 should have better availablility then the default
# we like to use, "en_US.UTF-8"
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Oracle
ENV ORACLE_HOME=/usr/lib/oracle/18.5/client64
ENV LD_LIBRARY_PATH=$ORACLE_HOME/lib
ENV PATH=$ORACLE_HOME/bin:$PATH
# Sets our hostname so it's pingable, which will make oracle happy.
# We set our hostname in this file in the entrypoint.sh script
# Explanation: https://medium.com/@mitchplanck/aws-lambda-node-js-oracle-3b5806fbecd3
# More context on HOSTALIASES, which isn't well documented: https://bugzilla.redhat.com/show_bug.cgi?id=7385i2
# Nother reference: https://stackoverflow.com/a/40400117
# Importantly, it's order is reversed vs /etc/hosts (which we can't modify in docker)
#ENV HOSTALIASES=/tmp/HOSTALIASES
# EDIT with oracle instantclient 18.5 these seems ot no longer be needed.

RUN set -ex \
  && buildDeps=' \
  python3-dev \
  libkrb5-dev \
  libsasl2-dev \
  libssl-dev \
  libffi-dev \
  build-essential \
  libblas-dev \
  liblapack-dev \
  ' \
  && apt-get update -yqq \
  && apt-get install -yqq --no-install-recommends \
  $buildDeps \
  libpq-dev \
  netbase \
  apt-utils \
  unzip \
  curl \
  netcat \
  locales \
  git \
  alien \
  libgdal-dev \
  libgeos-dev \
  binutils \
  libproj-dev \
  gdal-bin \
  libspatialindex-dev \
  libaio1 \
  freetds-dev

# Locale stuff
RUN sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
  && locale-gen \
  && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
  && useradd -ms /bin/bash worker

# Cleanup
RUN apt-get remove --purge -yqq $buildDeps \
  && apt-get clean \
  && rm -rf \
  /var/lib/apt/lists/* \
  /tmp/* \
  /var/tmp/* \
  /usr/share/man \
  /usr/share/doc \
  /usr/share/doc-base

# instant basic-lite instant oracle client
#COPY oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm ./
COPY oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm ./
RUN alien -i oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm \
  && rm oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm 

# instant oracle-sdk
#RUN alien -i oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm \
#    && rm oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm 

USER worker
WORKDIR /home/worker/

# pip stuff
RUN pip3.9 install pip --upgrade \
  && pip3.9 install setuptools --upgrade \
  && pip3.9 install Cython==0.29.28 \
  awscli==1.22.70 \
  boto3==1.21.15 \
  click==8.0.4 \
  cryptography==36.0.1 \
  petl==1.7.8 \
  pyasn1==0.4.8 \
  pyodbc==4.0.32 \
  pytz==2021.3 \
  wheel

# FAST BUILD LINES
COPY docker-fast-requirements.txt /docker-fast-requirements.txt
RUN pip3 install -r /docker-fast-requirements.txt
########################

# Per WORKDIR above, these should be placed in /home/worker/
COPY --chown=worker:root scripts/entrypoint.sh ./entrypoint.sh
COPY --chown=worker:root tests/ ./tests/
#COPY --chown=worker:root setup.py ./setup.py
COPY --chown=worker:root pyproject.toml ./pyproject.toml
COPY --chown=worker:root databridge_etl_tools ./databridge_etl_tools

RUN chmod +x ./entrypoint.sh

# Python syntax check
RUN python3.9 -m compileall ./databridge_etl_tools

# Install databridge-etl-tools using pyproject.toml
RUN pip3.9 install .

# Quick hack to fix CSV dump issue from Oracle
RUN   sed -i "s|MAX_NUM_POINTS_IN_GEOM_FOR_CHAR_CONVERSION_IN_DB = 150|MAX_NUM_POINTS_IN_GEOM_FOR_CHAR_CONVERSION_IN_DB = 100|g" /home/worker/.local/lib/python3.9/site-packages/geopetl/oracle_sde.py

# Set aws access keys as an env var for use with boto3
# do this under the worker user.
# These are passed in via --build-arg at build time.
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY


ENTRYPOINT ["/home/worker/entrypoint.sh"]
