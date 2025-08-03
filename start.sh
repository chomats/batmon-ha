nohup ./main.sh <&- &>./jkbms.log &
pid_main_sh=$(ps -ef | grep '/bin/sh ./main.sh' | grep -v grep | tr -s ' ' | cut -d ' ' -f2)
echo "start $pid_main_sh"