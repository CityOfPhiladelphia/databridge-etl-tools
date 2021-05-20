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
        'chardet==4.0.0',
        'click==7.0',
        'docutils==0.14',
        'future==0.18.2',
        'idna==2.10',
        'jmespath==0.9.4',
        'petl==1.2.0',
        'pyrestcli==0.6.11',
        'python-dateutil==2.8.1',
        'requests==2.25.0',
        's3transfer==0.2.0',
        'six==1.16.0',
        'urllib3==1.26.4'
    ],
    extras_require={
        'carto': ['carto'],
        'oracle': ['cx_Oracle'],
        'postgres': ['psycopg2-binary'],
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