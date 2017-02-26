#!/bin/bash
#
#   install_binutils.sh
#
#   This script installs a new version of binutils. It turns out
#   that binutils-2.27 is needed in order to build a statically
#   linked rust binary of the application.

set -e
cd ~/cache

if [ ! -f ~/cache/binutils-2.27.90 ]; then
  wget ftp://sourceware.org/pub/binutils/snapshots/binutils-2.27.90.tar.bz2
  tar xjvf binutils-2.27.90.tar.bz2
  cd binutils-2.27.90
  ./configure
  make
fi
cd ~/cache/binutils-2.27.90
sudo make install
