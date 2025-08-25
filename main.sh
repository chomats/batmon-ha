#/bin/bash

trap restart 3
trap call_exit 9

restart()
{
  subPid=$(ps -ef | grep 'python3 main.py --master --skip-discovery' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
  echo "Restart $subPid"
  kill -2 "$subPid"
}

call_exit()
{
  subPid=$(ps -ef | grep 'python3 main.py --master --skip-discovery' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
  echo "Exist $subPid"
  kill -2 "$subPid"
  sleep 1
  kill -9 "$subPid"
  exit 
}


while true
do
  /home/sch/venv/bin/python3 main.py --master --skip-discovery &
  batJkbmsPid=$!
  echo "current pid: $batJkbmsPid"
  wait
  sleep 1
done
echo exit