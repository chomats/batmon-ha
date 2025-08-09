#/bin/bash

trap restart 3
trap call_exit 9

restart()
{
  echo "Restart"
  kill -2 "$batJkbmsPid"
}

call_exit()
{
  echo "Restart"
  kill -2 "$batJkbmsPid"
  sleep 1
  kill -9 "$batJkbmsPid"
  exit 
}


while true
do
  /home/sch/venv/bin/python3 main.py&
  batJkbmsPid=$!
  wait
  sleep 1
done
echo exit