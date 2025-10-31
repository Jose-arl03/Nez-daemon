#!/bin/bash

# Set the environment variables the watcher needs
export WATCH_DIRECTORY="/home/alo03/Estadia/Nez-daemon/services/deployer/app/results"
export MICTLANX_URI="http://mictlanx-router-0@localhost:60666"

# Run the watcher script using the python from the virtual environment
/home/alo03/Estadia/Nez-daemon/venv/bin/python3 /home/alo03/Estadia/Nez-daemon/services/watcher/watcher.py
