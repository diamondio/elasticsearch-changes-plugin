#!/bin/bash

./uninstall.sh
./install.sh

launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist

tail -n 50 /usr/local/var/log/elasticsearch.log
