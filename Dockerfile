FROM ubuntu:16.04

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

# Oracle
ENV ORACLE_HOME=/usr/lib/oracle/12.1/client64
ENV LD_LIBRARY_PATH=$ORACLE_HOME/lib
ENV PATH=$ORACLE_HOME/bin:$PATH
ENV HOSTALIASES=/tmp/HOSTALIASES

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
        libpq-dev \
    ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python3 \
        python3-pip \
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
        freetds-dev \
        ssh-client \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && ssh-keyscan -H github.com >> ~/.ssh/known_hosts \
    && useradd -ms /bin/bash worker \ 
    && python3 -m pip install -U pip \
    && pip3 install -U setuptools \
    && pip3 install Cython \
    && pip3 install git+https://github.com/CityOfPhiladelphia/databridge-etl-tools#egg=databridge_etl_tools \
    && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# instant basic-lite instant oracle client
RUN set -ex \
    && aws s3api get-object \
           --bucket citygeo-oracle-instant-client \
           --key oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
                 oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
RUN set -ex \
    && aws s3api get-object \
           --bucket citygeo-oracle-instant-client \ 
           --key oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
                 oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

COPY scripts/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER worker
ENTRYPOINT ["/entrypoint.sh"]
#CMD ["/bin/bash"] 