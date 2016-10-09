package Parallel::ForkManager::Scaled;
use Moo;
use namespace::clean;
use Unix::Statgrab;
use List::Util qw( min max );

our $VERSION = '0.06';

extends 'Parallel::ForkManager';

has hard_min_procs   => ( is => 'rw', lazy => 1, builder => 1 );
has hard_max_procs   => ( is => 'rw', lazy => 1, builder => 1 );
has soft_min_procs   => ( is => 'rw', lazy => 1, builder => 1, trigger => 1 );
has soft_max_procs   => ( is => 'rw', lazy => 1, builder => 1, trigger => 1 );
has initial_procs    => ( is => 'lazy' );
has update_frequency => ( is => 'rw', default => 1 );
has idle_target      => ( is => 'rw', default => 0 );
has idle_threshold   => ( is => 'rw', default => 1 );
has run_on_update    => ( is => 'rw' );

has _stats_pct   => ( is => 'rwp',  handles => [ qw( idle ) ] );
has _host_info   => ( is => 'lazy', handles => [ qw( ncpus ) ] );
has _last_stats  => ( is => 'rw',  default => sub{ get_cpu_stats } );
has last_update  => ( is => 'rwp', default => sub{ time } );

#
# Once Parallel::ForkManager has converted to Moo (in development)
# this will no longer be necessary. Probably. :)
#
sub FOREIGNBUILDARGS {
    my ($class, @args) = @_;
    my @ret;

    my $args = @args > 1 ? {@args} : $args[0];

    push @ret, 1; # will get changed later in BUILD()
    push @ret, $args->{tempdir} if defined $args->{tempdir};

    @ret;
}

sub BUILD {
    my $self = shift;
    $self->set_max_procs($self->initial_procs);
    $self->update_stats_pct;
};

sub _build_hard_min_procs { 1 }
sub _build_hard_max_procs { (shift->ncpus // 1) * 2 }
sub _build_soft_min_procs { shift->hard_min_procs };
sub _build_soft_max_procs { shift->hard_max_procs };
sub _build__host_info     { get_host_info }

sub _build_initial_procs { 
    my $self = shift;
    $self->hard_min_procs+int(($self->hard_max_procs-$self->hard_min_procs)/2);
}

sub _trigger_soft_min_procs {
    my ($self, $newval) = @_;

    $self->soft_min_procs($self->hard_min_procs)
        if $newval < $self->hard_min_procs;
}

sub _trigger_soft_max_procs {
    my ($self, $newval) = @_;

    $self->soft_max_procs($self->hard_max_procs)
        if $newval > $self->hard_max_procs;
}

sub update_stats_pct {
    my $self = shift;

    my $stats = get_cpu_stats;
    $self->_set__stats_pct($stats->get_cpu_stats_diff($self->_last_stats)->get_cpu_percents);

    $self->_last_stats($stats);
    $self->_set_last_update(time);
}

#
# (Possibly) adjust our max_procs before the call to start(). 
#
before start => sub {
    my $self = shift;
 
    return unless time - $self->last_update >= $self->update_frequency;

    $self->update_stats_pct;

    my $new_procs;
    my $min_ok = max(0, $self->idle_target - $self->idle_threshold);
    my $max_ok = min(100, $self->idle_target + $self->idle_threshold);

    if ($self->idle >= $max_ok && $self->running_procs >= $self->max_procs) {
        $new_procs = $self->adjust_up;

    } elsif ($self->idle <= $min_ok) {
        $new_procs = $self->adjust_down;
    }

    if ($self->run_on_update && ref($self->run_on_update) eq 'CODE') {
        my $p = $self->run_on_update->($self, $new_procs);
        $new_procs = $p if defined $p;
    }

    if ($new_procs) {
        $new_procs = min($self->soft_max_procs, 
                     max($self->soft_min_procs, $new_procs));

        $self->set_max_procs($new_procs);
    }
};

sub stats {
    my $self = shift;
    my $new_procs = shift // $self->max_procs;

    sprintf(
        "%5.1f id %3d run %3d omax %3d nmax %3d smin %3d smax %3d hmin %3d hmax",
        $self->idle,
        scalar($self->running_procs), 
        $self->max_procs,
        $new_procs // -1,
        $self->soft_min_procs,
        $self->soft_max_procs,
        $self->hard_min_procs,
        $self->hard_max_procs
    );
}

sub dump_stats {
    my $self = shift;
    print STDERR $self->stats(@_)."\n";
    shift;
}

#
# Increase soft_max_procs to a maximum of hard_max_procs
#
# We'll use the system's idle percentage to tell us how much
# to increase by, the more idle the system is, the more we'll
# allow soft_max_procs to grow. Hopefully this will allow us
# to quickly adjust to the system without over-loading it if
# it's already close to our target idle state
#
sub adjust_soft_max {
    my $self = shift;
    $self->soft_max_procs(
        min($self->hard_max_procs,
            $self->soft_max_procs
            + max(1, int(
                ($self->hard_max_procs - $self->max_procs) 
                * ($self->idle - $self->idle_target) 
                / 100
            ))
        )
    );
}

#
# Decrease soft_min_procs, the system is too busy
#
sub adjust_soft_min {
    my $self = shift;
    $self->soft_min_procs(
        max($self->hard_min_procs,
            $self->hard_min_procs 
            + max(0, int(
                ($self->max_procs - $self->hard_min_procs)
                * ($self->idle_target - $self->idle)
                / 100
            ))
        )
    );
}

sub adjust_up {
    my $self = shift;
    my $cur = $self->max_procs;

    my $max = $cur >= $self->soft_max_procs
        ? $self->adjust_soft_max
        : $self->soft_max_procs;

    $self->soft_min_procs($cur);
    $cur + max(1,int(($max - $cur)/2));
}

sub adjust_down {
    my $self = shift;
    my $cur = $self->max_procs;

    my $min = $cur <= $self->soft_min_procs 
        ? $self->adjust_soft_min
        : $self->soft_min_procs;

    # Shouldn't happen, but test for it anyway
    return undef unless $cur > $min;

    $self->soft_max_procs($cur);
    $min + int(($cur - $min)/2);
}

1;

__END__

=pod

=head1 NAME

Parallel::ForkManager::Scaled - Run processes in parallel based on CPU usage

=head1 VERSION

Version 0.06

=head1 SYNOPSIS

    use Parallel::ForkManager::Scaled;

    # my $pm = Parallel::ForkManager::Scaled->new( attrib => value, ... );
    my $pm = Parallel::ForkManager::Scaled->new;

    # Used just like Parallel::ForkManager, so I'll paraphrase its documentation

    for my $data (@all_data) {
        # $pid is set to the child process' PID
        my $pid = $pm->start and next;

        # In the child process now
        # do some work ..

        # Exit the child
        $pm->finish; 
    }

=head1 DESCRIPTION

This module inherits from Parallel::ForkManager and adds the ability
to automatically manage the number of processes running based on how
busy the system is by watching the CPU idle time. Each time a child is
about to be start()ed a new value for B<max_procs> may be calculated
(if enough time has passed since the last calculation). If a new value
is calculated, the number of processes to run will be adjusted by
calling B<set_max_procs> with the new value.

Without specifying any attributes to the constructor, some defaults will
be set for you (see Attributes below)

=head2 Attributes

Attributes are just methods that may be passed to the constructor (C<new()>) and 
most may be changed during the life of the returned object. They take
as a parameter a new value to set for the attribute and return the current
value (or new value if one was passed).

=over

=item B<hard_min_procs>

The number of running processes will never be adjusted lower than this value.

default: 1

=item B<hard_max_procs>

The number of running processes will never be adjusted higher than this value.

default: The detected number of CPUs * 2

=item B<soft_min_procs>

=item B<soft_max_procs>

This is initially set to B<hard_min_procs> and B<hard_max_procs> respectively
and is adjusted over time. These are used when calculating adjustments as the 
minimum and maximum number of processes respectively. 

Over time B<soft_min_procs> and B<soft_max_procs> should approach the same value
for a consistent workload and a machine not otherwise busy.

Depending on the needs of the system, these values may also diverge if
necessary to try to reach B<idle_target>.

You may adjust these values if you wish by passing a value to the method
but you probably shouldn't. :)

=item B<initial_procs> (read-only)

The number of processes to start running before attempting any adjustments,
B<max_procs> will be set to this value upon initialization.

default: half way between B<hard_min_procs> and B<hard_max_procs>

=item B<update_frequency>

The minimum amount of time, in seconds, that must elapse between checks
of the system CPU's idle % and updates to the number of running processes.

Set this to 0 to cause a check before each call to C<start()>.

Before each call to C<start()> the time is compared with the last time a 
check/update was performed. If this much time has passed, a new check will be
made of how busy the CPU is and the number of processes may be adjusted.

default: 1

=item B<idle_target>

Percentage of CPU idle time to try to maintain by adjusting the number of running
processes between B<hard_min_procs> and B<hard_max_procs>

default: 0  # try to keep the CPU 100% busy (0% idle)

=item B<idle_threshold>

Only make adjustments if the current CPU idle % is this distance away from B<idle_target>.
In other words, only adjust if C<abs(B<cur_idle> - B<idle_target>) E<gt> B<idle_threshold>>.
This may be a fractional value (floating point).

You may notce that the default B<idle_target> of 0 and B<idle_threshold> of 1
would seem to indicate that the processes would never be adjusted as idle can
never be less than 0%. At the limits, the threshold is adjusted so that we
will still attempt adjustments, something like this:

    min_ok = max(0,   idle_target - idle_threshold)
    max_ok = min(100, idle_target - idle_threshold)

    adjust if idle >= max_ok
    adjust if idle <= min_ok

default: 1

=item B<run_on_update>

This is a callback function that is run immediately after (possibly) 
calculating an adjustment, but before setting our B<max_procs> to the 
new value. This allows you to override the default behavior of this 
module for your own nefarious purposes.

B<run_on_update> expects a coderef which will be called with two
parameters:

=over

=item * 

The object being adjusted.

=item *

The newly calculated value for B<max_procs> or undef if there was no adjustment to be made.

=back

The callback must return either a new value for B<max_procs> or undef. If the
returned value is undef, no change will be made to B<max_procs>. Otherwise
if a value is returned it will be used to set B<max_procs>.

Be aware that your returned value will be constrained by 
B<soft_min_procs> and B<soft_max_procs>.

=item B<tempdir>

This is passed to the Parallel::ForkManager constructor to set
tempdir. Where Parallel::ForkManager is constructed thusly:

  my $pm = Parallel::ForkManager->new($procs, $tempdir);

The equivalent for this module would be:

  my $pm = Parallel::ForkManager::Scaled->new(initial_procs => $procs, tempdir => $tempdir);

=back

=head2 Methods

All methods inherited from L<Parallel::ForkManager> plus the following:

=over

=item B<last_update>

Returns the last C<time()> a check/update was performed.

=item B<idle>

Returns the system's idle percentage as of B<last_update>.

=item B<ncpus>

The number of CPUs detected on the system, this is just
a wrapper to the cpus function from L<Unix::Statgrab>.

=item B<stats>

Returns a formatted string with information about the
current status. Takes a single parameter, the new
value for B<max_procs> to be set. If no parameter is passed,
the vlaue B<max_procs> will be used.

=back

=head3 Methods you probably don't need to use

These are not meant for general consumption but are available anyway.
Probably best to avoid them :)

=over

=item B<update_stats_pct>

This method will force an update of the B<idle> statistic.

=item B<dump_stats>

Print the string returned by B<stats> to STDERR. This may be used in the
B<run_on_update> callback to see diagnostics as processes are run:

C<$pm-E<gt>run_on_update(\&Parallel::ForkManager::Scaled::dump_stats)>

=back

=head1 EXAMPLES

These examples are also provided in the examples/ directory of 
this distribution.

=head2 Maximize CPU usage

see: examples/prun.pl

Run shell commands that are passed into the program and try to
keep the CPU busy, i.e. 0% idle

    use Parallel::ForkManager::Scaled;

    my $pm = Parallel::ForkManager::Scaled->new(
        run_on_update => \&Parallel::ForkManager::Scaled::dump_stats
    );
    
    # just to be sure we can saturate the CPU
    $pm->hard_max_procs($pm->ncpus * 4);

    $pm->set_waitpid_blocking_sleep(0);

    while (<>) {
        chomp;
        $pm->start and next;

        # In the child now, run the shell process
        system $_;
        $pm->finish;
    }

=head2 Dummy Load

see: examples/dummy_load.pl

This example provides a way to test the capabilities of this module.
Try changing the idle_target and other settings to see the effect.

    use Parallel::ForkManager::Scaled;

    my $pm = Parallel::ForkManager::Scaled->new(
        run_on_update => \&Parallel::ForkManager::Scaled::dump_stats,
        idle_target => 50,
    );

    $pm->set_waitpid_blocking_sleep(0);

    for my $i (0..1000) {
        $pm->start and next;

        my $start = time;
        srand($$);
        my $lifespan = 5+int(rand(10));

        # Keep the CPU busy until it's time to exit
        while (time - $start < $lifespan) { 
            my $a = time; 
            my $b = $a^time/3;
        }

        $pm->finish;
    }

=head1 NOTES

Currently this module only works on systems where Unix::Statgrab is available,
which is probably any system where the libstatgrab library can compile.

=head1 AUTHOR

Jason McCarver <slam@parasite.cc>

=head1 SEE ALSO

=over

=item L<Parallel::ForkManager>

=item L<Unix::Statgrab>

=back

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2016 by Jason McCarver

This is free software; you can redistribute it and/or modify it under the
same terms as the Perl 5 programming language system itself.

=cut
