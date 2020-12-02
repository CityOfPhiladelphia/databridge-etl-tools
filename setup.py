#!/usr/bin/env python

from distutils.core import setup

setup(
    name='databridge_etl_tools',
    version='0.1.0',
    packages=['databridge_etl_tools',],
    install_requires=[
        'boto3==1.9.137',
        'botocore==1.12.137',
        'certifi==2019.3.9',
        'chardet==3.0.4',
        'click==7.0',
        'docutils==0.14',
        'future==0.17.1',
        'idna==2.8',
        'jmespath==0.9.4',
        'petl==1.6.8',
        'pyrestcli==0.6.8',
        'python-dateutil==2.8.0',
        'requests==2.21.0',
        's3transfer==0.2.0',
        'six==1.12.0',
        'urllib3==1.24.2'
    ],
    extras_require={
        'carto': ['carto==1.4.0'],
        'oracle': ['cx_Oracle==7.1.3'],
        'postgres': ['psycopg2-binary==2.8.2'],
        'dev': [
            'moto==1.3.8',
            'pytest==4.4.1',
            'requests-mock==1.6.0'
        ]
    },
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/geopetl/tarball/57222e39902c43f4121cdb5b4b6058bf048d84d7'
    ],
    entry_points={
        'console_scripts': [
            'databridge_etl_tools=databridge_etl_tools:main',
        ],
    },
)