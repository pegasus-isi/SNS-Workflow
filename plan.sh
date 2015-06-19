#!/bin/bash

SITE=hopper
OUTPUT_SITE=local

if [ $# -lt 2 ]; then
    echo "Usage: $0 WORKFLOW_DIR CONFIG_FILE <--synthetic|--hydrogen>"
    exit 1
fi

WORKFLOW_DIR=$1
CONFIG_FILE=$2

if [ ! -e "$CONFIG_FILE" ]; then
    echo "No such file: $CONFIG_FILE"
    exit 1
fi

SYNTHETIC=false
if [ "$3" == "--synthetic" ]; then

    SYNTHETIC=true

    echo "python daxgen.py --synthetic $CONFIG_FILE $WORKFLOW_DIR"
    python daxgen.py --synthetic $CONFIG_FILE $WORKFLOW_DIR
else
    if [ "$3" == "--hydrogen" ]; then

        echo "python daxgenQ.py $CONFIG_FILE $WORKFLOW_DIR"
        python daxgenQ.py $CONFIG_FILE $WORKFLOW_DIR

    else

        echo "python daxgen.py $CONFIG_FILE $WORKFLOW_DIR"
        python daxgen.py $CONFIG_FILE $WORKFLOW_DIR

    fi
fi

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

if [ $SYNTHETIC = true ]; then
    TC=$DIR/tc-fake.txt
fi

echo "We will us '$TC' as transformation catalog"

RC=$WORKFLOW_DIR/rc.txt
SC=$DIR/sites.xml
PP=$DIR/pegasus.properties

echo "Planning workflow..."
pegasus-plan \
    -Dpegasus.catalog.replica=File \
    -Dpegasus.catalog.replica.file=$RC \
    -Dpegasus.catalog.transformation=Text \
    -Dpegasus.catalog.transformation.file=$TC \
    --conf $PP \
    --dax $DAX \
    --dir $SUBMIT_DIR \
    --input-dir $INPUT_DIR \
    --sites $SITE \
    --output-site $OUTPUT_SITE \
    --cleanup leaf \
    --submit
