#!/bin/bash
#
#   install_kcov.sh
#
#   This script installs kcov, which is the rust
#   coverage measurement tool.

set -e
cd

sudo apt-get install binutils-dev libcurl4-openssl-dev zlib1g-dev libdw-dev libiberty-dev

if ! hash kcov 2>/dev/null; then
  git clone https://github.com/SimonKagstrom/kcov.git
  mkdir kcov/build
  cd kcov/build; cmake ../; make; sudo make install;
  cp /usr/local/bin/kcov ~/.cargo/bin/
fi
