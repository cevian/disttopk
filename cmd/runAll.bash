#!/bin/bash
./cmd -suite UCB -modServers 10 -keyclient 0 2>&1 |tee UCB/run.`date +%s`.result
./cmd -suite UCB -modServers 50 -keyclient 0 2>&1 |tee UCB/run.`date +%s`.result
./cmd -suite UCB -modServers 100 -keyclient 0 2>&1 UCB/run.`date +%s`.result
./cmd -suite WC -keyclient 1 2>&1| tee WC/run.`date +%s`.result
