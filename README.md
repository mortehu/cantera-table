Cantera Table
=============

To install the dependencies required to build this software, run:

    $ ./install-debian-deps.sh

To generate the "configure" script, and related files, run:

    $ ./autogen.sh

To generate the Makefile, run:

    $ ./configure

To compile the software, run:

    $ make

# Terms of Redistribution

The package located under `third_party/capnproto` is licensed under the [MIT
license](http://opensource.org/licenses/MIT).

The package located under `third_party/gtest` is licensed under the [BSD
3-Clause License](http://opensource.org/licenses/BSD-3-Clause).

All other files are distributed under the terms of the GNU General Public
License as published by the Free Software Foundation; either [version 2 of the
License](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html), or (at
your option) any later version.

# Components

  * [Cantera Table](storage/ca-table/README)

  * [Cap'n Proto](third_party/capnproto/README.md)

  * [GoogleTest](third_party/gtest)

# Build System

This software distribution is created from the internal software repository
used at [e.ventures](http://www.eventures.vc/).  To maintain minimal
differences between the free and proprietary repositories, the peculiar build
system has been kept in place.
