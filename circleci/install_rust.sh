#!/bin/bash
#
#   install_rust.sh
#
#   This script installs the latest stable rust using rustup,
#   unless it is already installed in ~/.cargo

set -e
cd

if [ ! -f ~/.cargo/bin/rustc ]; then
  curl https://sh.rustup.rs -sSf | sh /dev/stdin -y
fi
