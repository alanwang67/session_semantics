#!/bin/bash

# put session info, pinned servers, server gossip interval, threads, operations 
# gossiip vs primary backup config
# allow for fine grained info such as reading from one server and writing from one server in client file 

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


SES="0"               # session name
DIR="/home/alanwang/session_semantics/"   # base project directory

create_session $SES $DIR       # create detached session
new_window $SES 1 $DIR
new_window $SES 2 $DIR
new_window $SES 3 $DIR
# new_window_horiz_split $SES 2 ${DIR}/src

# Builtin flags in the above commands for the following actions
# don't seem to work when run multiple times inside a bash script,
# seemingly due to a race condition. Give them some time to finish.

sleep 0.1

# detatch-client
# ssh srg02 -t "go run main.go server 1 $2"

go build main.go

name_window $SES 0 server0 
run_command $SES 0 "ssh srg02"

name_window $SES 1 server1
run_command $SES 1 "ssh srg03"

name_window $SES 2 server2
run_command $SES 2 "ssh srg04"

name_window $SES 3 client
run_command $SES 3 "ssh srg05"

sleep 1

# run_command $SES 0 "cd session_semantics"
# run_command $SES 0 "go run main.go 0 server 0 500"

# run_command $SES 1 "cd session_semantics"
# run_command $SES 1 "go run main.go 0 server 1 500"

# run_command $SES 2 "cd session_semantics"
# run_command $SES 2 "go run main.go 0 server 2 500"

# run_command $SES 3 "cd session_semantics"
# run_command $SES 3 "go run main.go 0 client config_files/PinnedRoundRobin.json 32 30 5 95"

attach_session $SES