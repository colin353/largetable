#!/bin/bash
#
#   install_protobuf.sh
#
#   This script installs protobuf if it isn't already
#   installed, by downloading it from Google.

set -e
cd

if [ ! -f ~/cache/protoc ]; then
  wget https://github.com/google/protobuf/releases/download/v3.0.0/protoc-3.0.0-linux-x86_64.zip
  unzip protoc-3.0.0-linux-x86_64.zip
  mv bin/protoc ~/cache/protoc
fi

sudo mv ~/cache/protoc /usr/local/bin/protoc
