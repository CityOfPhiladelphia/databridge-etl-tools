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
    ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        libpq-dev \
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
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash worker \ 
    && pip3 install -U setuptools \
    && pip3 install Cython \
       awscli==1.16.140 \
       boto3==1.7.84 \
       carto==1.4.0 \
       click==7.0 \
       cryptography==2.6.1 \
       cx-Oracle==7.0.0 \
       -e git+https://github.com/CityOfPhiladelphia/geopetl.git@57222e39902c43f4121cdb5b4b6058bf048d84d7#egg=geopetl \
       petl==1.2.0 \
       psycopg2==2.8.1 \
       pyasn1==0.4.5 \
       pyodbc==4.0.26 \
       pytz==2015.7 \
    && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm ./

# instant basic-lite instant oracle client
RUN alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
RUN alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

COPY scripts/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

# Cache bust
ENV updated-adds-on 5-1-2019_5
COPY databridge_etl_tools /databridge_etl_tools
COPY setup.py /setup.py
RUN pip3 install -e .

USER worker
ENTRYPOINT ["/entrypoint.sh"]
#CMD ["/bin/bash"] 

