#!/bin/env bash
HOWO_TABLE_NAME=$1
HOWO_VIEW_NAME="howo_dont_touch"
HOWO_LOCATION=$2

HOWO_INITIALIZE_VIEW="DROP VIEW IF EXISTS \"${HOWO_VIEW_NAME}\";"

HOWO_SQL_CREATE_BASE="CREATE VIEW IF NOT EXISTS \"${HOWO_TABLE_NAME}\" (rowkey varchar primary key, \"info\".\"FirstName\" varchar, \"info\".\"LastName\" varchar, \"info\".\"Location\" varchar, \"info\".\"Count\" varchar);"

HOWO_SQL_CREATE_VIEW="CREATE VIEW \"${HOWO_VIEW_NAME}\" (rowkey varchar primary key, \"info\".\"FirstName\" varchar, \"info\".\"LastName\" varchar, \"info\".\"Location\" varchar, \"info\".\"Count\" varchar) AS SELECT * FROM \"${HOWO_TABLE_NAME}\";"

HOWO_SQL_QUERY="SELECT COUNT(*) FROM \"${HOWO_VIEW_NAME}\" where \"info\".\"Location\"='${HOWO_LOCATION}';"

phoenix-sqlline localhost <<< $HOWO_INITIALIZE_VIEW

phoenix-sqlline localhost <<< $HOWO_SQL_CREATE_BASE

phoenix-sqlline localhost <<< $HOWO_SQL_CREATE_VIEW

phoenix-sqlline localhost <<< $HOWO_SQL_QUERY

