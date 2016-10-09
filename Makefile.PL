#!/usr/bin/env perl

use inc::Module::Install;

name     'Parallel-ForkManager-Scaled';
all_from 'lib/Parallel/ForkManager/Scaled.pm';

requires 'List::Util';
requires 'Moo';
requires 'Parallel::ForkManager';
requires 'Unix::Statgrab';
requires 'namespace::clean';

repository 'https://bitbucket.org/jmccarv/perl-parallel-forkmanager-scaled';

test_requires 'Test::More';

no_index directory => 'examples';
no_index directory => '.hg';

# install_script glob('script/*.pl');
auto_install;
WriteAll;
