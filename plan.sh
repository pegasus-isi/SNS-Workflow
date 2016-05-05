#!/bin/bash

SITE=nersc
OUTPUT_SITE=nersc

if [ $# -ne 1 ]; then
    echo "Usage: $0 WORKFLOW_DIR"
    exit 1
fi

WORKFLOW_DIR=$1

if [ -d "$WORKFLOW_DIR" ]; then
    WORKFLOW_DIR=$(cd $WORKFLOW_DIR && pwd)
else
    echo "No such directory: $WORKFLOW_DIR"
    exit 1
fi

DIR=$(cd $(dirname $0) && pwd)
INPUT_DIR=$DIR/inputs
SUBMIT_DIR=$WORKFLOW_DIR/submit
DAX=$WORKFLOW_DIR/dax.xml
TC=$DIR/tc.txt
RC=$WORKFLOW_DIR/rc.txt
SC=$DIR/sites.xml
PP=$DIR/pegasus.properties

echo "Planning workflow..."
pegasus-plan \
    -Dpegasus.metrics.app=sns \
    -Dpegasus.catalog.site.file=$SC \
    -Dpegasus.catalog.replica=File \
    -Dpegasus.catalog.replica.file=$RC \
    -Dpegasus.catalog.transformation.file=$TC \
    --conf $PP \
    --dax $DAX \
    --dir $SUBMIT_DIR \
    --input-dir $INPUT_DIR \
    --sites $SITE \
    --output-site $OUTPUT_SITE \
    --cleanup leaf

