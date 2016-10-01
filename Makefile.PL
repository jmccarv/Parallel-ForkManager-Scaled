#!/usr/bin/env perl
# IMPORTANT: if you delete this file your app will not work as
# expected.  You have been warned.
use inc::Module::Install;

name 'Parallel-ForkManager-Scaled';
all_from 'lib/Parallel/ForkManager/Scaled.pm';

requires 'Moo';
requires 'Parallel::ForkManager';
test_requires 'Test::More' => '0.88';

# install_script glob('script/*.pl');
#auto_install;
WriteAll;
