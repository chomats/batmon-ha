pid_main_sh=$(ps -ef | grep '/bin/sh ./main.sh' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
echo "stop $pid_main_sh"
kill -9 $pid_main_sh

subPid=$(ps -ef | grep 'python3 main.py --master --skip-discovery' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
echo "Stop $subPid"
kill -2 "$subPid"