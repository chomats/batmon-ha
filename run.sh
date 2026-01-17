#!/usr/bin/with-contenv bashio

if [ -z ${OTHER_ARGS+x} ]; then
 OTHER_ARGS=''
fi
echo "OTHER_ARGS= $OTHER_ARGS"
echo "Python jkbms exporter main.py..."
python3 main.py --master --skip-discovery --console "$OTHER_ARGS"
