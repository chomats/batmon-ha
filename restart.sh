#/bin/bash
subPid=$(ps -ef | grep 'python3 main.py --master --skip-discovery' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
echo "Restart $subPid"
kill -2 "$subPid"