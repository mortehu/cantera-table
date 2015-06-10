#!/bin/sh

# Any dependencies not in the Debian base install that are not contained in
# this Git repository should be listed in this install script.

apt-get -y install \
  automake \
  bison \
  flex \
  libtool \
  libhwloc-dev \
  libjsoncpp-dev \
  libyaml-cpp-dev \
  python-dev \
  swig
