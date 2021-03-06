#!/usr/bin/env bash

set -e

cd $TRAVIS_BUILD_DIR

mkdir dist

ZIP_FILE_PATH=$TRAVIS_BUILD_DIR/dist/databridge-etl-tools-$ENVIRONMENT.zip

echo "Zip up site-packages, excluding non-compiled psycopg2 files to avoid confusing python imports"
cd /home/travis/virtualenv/python3.5.6/lib/python3.5/site-packages/
zip -rq $ZIP_FILE_PATH . -x psycopg2/**\* psycopg2_binary-2.8.2.dist-info/**\

cd $TRAVIS_BUILD_DIR/
echo "Rename compiled psycopg2 file so python recognizes it"
mv psycopg2-3.6 psycopg2

echo "Zip together previous zip file and psycopg2 compiled files"
zip -urq $ZIP_FILE_PATH psycopg2

echo "Zip together previous zip file and lambda function"
cd $TRAVIS_BUILD_DIR/lambda
zip -gq $ZIP_FILE_PATH index.py