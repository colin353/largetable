#!/bin/bash
#
#   install_kcov.sh
#
#   This script installs protobuf if it isn't already
#   installed, by downloading it from Google.

set -e
cd

sudo apt-get install binutils-dev libcurl4-openssl-dev zlib1g-dev libdw-dev libiberty-dev

if ! hash kcov 2>/dev/null; then
  git clone https://github.com/SimonKagstrom/kcov.git
  mkdir kcov/build
  cd kcov/build; cmake ../; make; sudo make install;
  cp /usr/local/bin/kcov ~/.cargo/bin/
fi
