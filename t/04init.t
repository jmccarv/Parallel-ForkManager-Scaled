#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;

use Parallel::ForkManager::Scaled;

plan tests => 9;

my $pm = Parallel::ForkManager::Scaled->new;
ok(defined $pm, 'constructor');

ok($pm->initial_procs > 0, 'initial procs');
ok($pm->max_procs == $pm->initial_procs, 'max procs');

ok($pm->hard_min_procs > 0, 'hard minimum procs');
ok($pm->hard_max_procs >= $pm->hard_min_procs, 'hard maximum procs');
ok($pm->soft_min_procs >= $pm->hard_min_procs, 'soft minimum procs');
ok($pm->soft_max_procs <= $pm->hard_max_procs, 'soft maximum procs 1');
ok($pm->soft_max_procs >= $pm->soft_min_procs, 'soft maximum procs 2');

ok($pm->update_frequency > 0, 'update frequency');
