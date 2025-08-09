#/bin/bash
pid_main_sh=$(ps -ef | grep '/bin/sh ./main.sh' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
echo "restart $pid_main_sh"
kill -3 $pid_main_sh