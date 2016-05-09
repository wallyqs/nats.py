#!/bin/bash

set -e

# check to see if gnatsd folder is empty
if [ ! "$(ls -A $HOME/gnatsd)" ]; then
    (
	mkdir -p $HOME/gnatsd;
	cd $HOME/gnatsd
	wget https://github.com/nats-io/gnatsd/releases/download/v0.8.0.beta2/gnatsd-v0.8.0.beta2-linux-amd64.zip -O gnatsd.zip;
	unzip gnatsd.zip;
    )
else
  echo 'Using cached directory.';
fi
