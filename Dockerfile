FROM python:3.12.11-slim-trixie

# Add our worker users custom binaries to the path, some python packages are installed here.
ENV PATH="/home/worker/.local/bin:${PATH}"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

RUN useradd -ms /bin/bash worker

RUN apt-get update && \
  apt-get install -yqq --no-install-recommends \
  apt-utils \
  build-essential \
  ca-certificates \
  curl \
  git \
  gnupg \
  lsb-release \
  wget \
  unzip

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

USER worker
WORKDIR /home/worker/

# Per WORKDIR above, these should be placed in /home/worker/
COPY --chown=worker:root scripts/entrypoint.sh ./entrypoint.sh
COPY --chown=worker:root tests/ ./tests/
COPY --chown=worker:root pyproject.toml ./pyproject.toml
COPY --chown=worker:root databridge_etl_tools ./databridge_etl_tools

RUN chmod +x ./entrypoint.sh

# pip stuff
RUN pip install pip --upgrade \
  && pip install setuptools --upgrade

# Python syntax check
RUN python -m compileall ./databridge_etl_tools

# Install databridge-etl-tools using pyproject.toml
RUN pip install .

# Set aws access keys as an env var for use with boto3
# do this under the worker user.
# These are passed in via --build-arg at build time.
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

ENTRYPOINT ["/home/worker/entrypoint.sh"]