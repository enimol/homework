#!/bin/env bash
HOWO_LAST_NAME=$1
HOWO_LOCATION=$2
HOWO_VIEW_NAME="howo_dont_touch"

HOWO_SQL_QUERY="SELECT COUNT(*) FROM \"${HOWO_VIEW_NAME}\" WHERE \"info\".\"LastName\"='${HOWO_LAST_NAME}' AND  \"info\".\"Location\"='${HOWO_LOCATION}';"

phoenix-sqlline localhost <<< $HOWO_SQL_QUERY

