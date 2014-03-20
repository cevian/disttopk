#!/bin/bash
./cmd -suite UCB -modServers 10 -keyclient 0 > UCB/run.`date +%s`.result
./cmd -suite UCB -modServers 50 -keyclient 0 > UCB/run.`date +%s`.result
./cmd -suite UCB -modServers 100 -keyclient 0 > UCB/run.`date +%s`.result
./cmd -suite WC -keyclient 1 > WC/run.`date +%s`.result
