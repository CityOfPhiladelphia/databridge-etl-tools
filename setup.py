#!/usr/bin/env python

from distutils.core import setup

setup(
    name='databridge_etl_tools',
    version='0.1.0',
    packages=['databridge_etl_tools',],
    install_requires=[
        'boto3',
        'botocore',
        'certifi',
        'chardet',
        'click',
        'docutils',
        'future',
        'idna',
        'jmespath',
        'petl',
        'pyrestcli',
        'python-dateutil',
        'requests',
        's3transfer',
        'six',
        'urllib3'
    ],
    extras_require={
        'carto': ['carto'],
        'oracle': ['cx_Oracle'],
        'postgres': ['psycopg2-binary'],
        'dev': [
            'moto',
            'pytest',
            'requests-mock'
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