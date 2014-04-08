#!/bin/bash
./cmd -suite UCB -modServers 10 -keyclient=0 2>&1 |tee UCB/run.1.`date +%s`.result
./cmd -suite UCB -modServers 25 -keyclient=0 2>&1 |tee UCB/run.11.`date +%s`.result
./cmd -suite UCB -modServers 50 -keyclient=0 2>&1 |tee UCB/run.2.`date +%s`.result
./cmd -suite UCB -modServers 75 -keyclient=0 2>&1 |tee UCB/run.22.`date +%s`.result
./cmd -suite UCB -modServers 100 -keyclient=0 2>&1 |tee UCB/run.3.`date +%s`.result
./cmd -suite UCB -modServers 10 -keyclient=1 2>&1 |tee UCB/run.1.1.`date +%s`.result
./cmd -suite UCB -modServers 25 -keyclient=1 2>&1 |tee UCB/run.1.11.`date +%s`.result
./cmd -suite UCB -modServers 50 -keyclient=1 2>&1 |tee UCB/run.1.2.`date +%s`.result
./cmd -suite UCB -modServers 75 -keyclient=1 2>&1 |tee UCB/run.1.22.`date +%s`.result
./cmd -suite UCB -modServers 100 -keyclient=1 2>&1 |tee UCB/run.1.3.`date +%s`.result
./cmd -suite WC -keyclient=1 2>&1| tee WC/run.1.`date +%s`.result
./cmd -suite WC -keyclient=0 2>&1| tee WC/run.0.`date +%s`.result
