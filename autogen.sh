#!/bin/sh

echo >&2 "Preparing the build system... please wait."

autoreconf -i -f -v || exit 1

echo >&2 "The build system is now prepared.  To build here, run:"
echo >&2
echo >&2 "  ./configure"
echo >&2 "  make"
