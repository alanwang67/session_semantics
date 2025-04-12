#!/bin/bash

create_session() {
    tmux new-session -d -s ${1} -c ${2}
}

# Attach to tmux session
attach_session() {
    tmux attach-session -t $1
}

# Create new tmux window, set starting directory
new_window() {
    tmux new-window -t ${1}:${2} -c ${3}
}

# Create new tmux window split horizontally, set starting directory
new_window_horiz_split() {
    tmux new-window -t ${1}:${2} -c ${3}
    tmux split-window -h -t ${1}:${2}
}

# Name tmux window
name_window() {
    tmux rename-window -t ${1}:${2} ${3}
}

# Run tmux command
run_command() {
    tmux send-keys -t ${1}:${2} "${3}" C-m
}

# Run tmux command in left pane
run_command_left() {
    tmux send-keys -t ${1}:${2}.0 "${3}" C-m
}

# Run tmux command in right pane
run_command_right() {
    tmux send-keys -t ${1}:${2}.1 "${3}" C-m
}

if [[ $* == *-help* ]]; then 
    echo Provide: initialThreads numberOfOperations 
else 
    ct=0
    tmux kill-session -t experiment 
    
    cd ~/session_semantics
    go build main.go 

    cd ~/session_semantics/generateGraphs/

    SES="experiment"               
    DIR="~/session_semantics/"   

    create_session $SES $DIR       
    new_window $SES 1 $DIR
    new_window $SES 2 $DIR

    # Builtin flags in the above commands for the following actions
    # don't seem to work when run multiple times inside a bash script,
    # seemingly due to a race condition. Give them some time to finish.

    sleep 1

    name_window $SES 0 server0 
    run_command $SES 0 "ssh srg02"

    name_window $SES 1 server1
    run_command $SES 1 "ssh srg03"

    name_window $SES 2 server2
    run_command $SES 2 "ssh srg04"

    run_command $SES 0 "cd ~/session_semantics"
    run_command $SES 1 "cd ~/session_semantics"
    run_command $SES 2 "cd ~/session_semantics"

    sleep 1

    declare -a arr=("GossipRandom" "PinnedRoundRobin" "PrimaryBackupRandom" "PrimaryBackUpRoundRobin")
    declare -a workload=(5 50 95)
    for name in "${arr[@]}"
    do
        cd ~/session_semantics/generateGraphs/
        mkdir $name
        for w in "${workload[@]}"
        do
            cd ~/session_semantics/generateGraphs/$name/
            mkdir workload_$w
            for session in {0..5} 
            do  
                cd ~/session_semantics/generateGraphs/$name/workload_$w
                mkdir $session    
                # for run in {1..3}
                # do
                    # cd ~/session_semantics/generateGraphs/$name/workload_$w/$session 
                    # mkdir run_$run 
                for i in {1..7}
                do
                    run_command $SES 0 "./main $ct server 0 500"

                    run_command $SES 1 "./main $ct server 1 500"

                    run_command $SES 2 "./main $ct server 2 500"

                    sleep 10

                    if [ "$name" = "GossipRandom" ]; then
                        cd ~/session_semantics; ./main $ct client config_files/$name.json $(( 1 + (($i - 1) * 1) )) 30 $session $w > ./generateGraphs/$name/workload_$w/$session/$i
                    fi
                    
                    if [ "$name" = "PrimaryBackupRandom" ]; then
                        cd ~/session_semantics; ./main $ct client config_files/$name.json $(( 1 + (($i - 1) * 1) )) 30 $session $w > ./generateGraphs/$name/workload_$w/$session/$i
                    fi
                    if [ "$name" = "PinnedRoundRobin" ]; then
                        cd ~/session_semantics; ./main $ct client config_files/$name.json $(( 2 + (($i - 1) * 2) )) 30 $session $w > ./generateGraphs/$name/workload_$w/$session/$i
                    fi
                    if [ "$name" = "PrimaryBackUpRoundRobin" ]; then
                        cd ~/session_semantics; ./main $ct client config_files/$name.json $(( 2 + (($i - 1) * 2) )) 30 $session $w > ./generateGraphs/$name/workload_$w/$session/$i
                    fi
                    ct=$(($ct + 1))

                    tmux send-keys -t server0 C-c
                    tmux send-keys -t server1 C-c
                    tmux send-keys -t server2 C-c

                    # cd ~/session_semantics/generateGraphs/; python3 plot.py 5 ./generateGraphs/$name/workload_$w/$session/

                    # done
                done 
            done
        done 
    done
fi
