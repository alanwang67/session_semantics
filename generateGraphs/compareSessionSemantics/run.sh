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

    cd ~/session_semantics/generateGraphs/compareSessionSemantics


    SES="experiment"               
    DIR="~/session_semantics/"   

    create_session $SES $DIR       
    new_window $SES 1 $DIR
    new_window $SES 2 $DIR
    new_window $SES 3 $DIR

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

    for session in {0..5} 
    do
        cd ~/session_semantics/generateGraphs/compareSessionSemantics
        mkdir $session    
        for i in {1..5}
        do
            run_command $SES 0 "./main $ct server 0 500"

            run_command $SES 1 "./main $ct server 1 500"

            run_command $SES 2 "./main $ct server 2 500"

            sleep 20

            cd ~/session_semantics; go run main.go $ct client $(( $1 * $i * 2 )) $2 $session true [] [] > ./generateGraphs/compareSessionSemantics/$session/$i

            ct=$(($ct + 1))
            echo 'finished'

            tmux send-keys -t server0 C-c
            tmux send-keys -t server1 C-c
            tmux send-keys -t server2 C-c

        done
    done
fi
