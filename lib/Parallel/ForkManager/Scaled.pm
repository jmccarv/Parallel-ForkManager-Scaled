package Parallel::ForkManager::Scaled;
use Moo;
use namespace::clean;
use Unix::Statgrab;
use List::Util qw( min max );

extends 'Parallel::ForkManager';

has hard_min_procs   => ( is => 'rw', lazy => 1, builder => 1 );
has hard_max_procs   => ( is => 'rw', lazy => 1, builder => 1 );
has initial_procs    => ( is => 'lazy' );
has update_frequency => ( is => 'rw', default => 5 );
has idle_threshold   => ( is => 'rw', default => 1 );
has busy_threshold   => ( is => 'rw', default => 0 );
has run_on_update    => ( is => 'rw' );

has soft_min_procs => ( is => 'rwp', lazy => 1, builder => 1 );
has soft_max_procs => ( is => 'rwp', lazy => 1, builder => 1 );

has stats_pct    => ( is => 'rwp', handles => [qw( idle )] );
has last_update  => ( is => 'rwp', default => sub{ time } );

has _last_stats  => ( is => 'rw', default => sub{ get_cpu_stats } );
has _host_info   => ( is => 'lazy' );

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

sub _build_hard_min_procs { shift->_host_info->ncpus // 1 }
sub _build_hard_max_procs { (shift->_host_info->ncpus // 1) * 2 }
sub _build_soft_min_procs { shift->hard_min_procs };
sub _build_soft_max_procs { shift->hard_max_procs };
sub _build__host_info     { get_host_info }

sub _build_initial_procs { 
    my $self = shift;
    $self->hard_min_procs+int(($self->hard_max_procs-$self->hard_min_procs)/2);
}

sub update_stats_pct {
    my $self = shift;

    my $stats = get_cpu_stats;
    $self->_set_stats_pct($stats->get_cpu_stats_diff($self->_last_stats)->get_cpu_percents);

    $self->_last_stats($stats);
    $self->_set_last_update(time);
}

before start => sub {
    my $self = shift;
 
    return unless time - $self->last_update >= $self->update_frequency;

    $self->update_stats_pct;

    my $new_procs;
    if ($self->idle > $self->idle_threshold && $self->running_procs >= $self->max_procs) {
        # idle hands spend time at the genitals
        $new_procs = $self->adjust_up;

    } elsif ($self->idle <= $self->busy_threshold && $self->running_procs <= $self->max_procs) {
        # too busy, back off
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

sub dump_stats {
    my $self = shift;
    my $new_procs = shift;

    return unless defined $new_procs;

    printf(
        "%5.1f id %3d run %3d omax %3d nmax %3d smin %3d smax %3d hmin %3d hmax\n",
        $self->idle,
        scalar($self->running_procs), 
        $self->max_procs,
        $new_procs // -1,
        $self->soft_min_procs,
        $self->soft_max_procs,
        $self->hard_min_procs,
        $self->hard_max_procs
    );

    undef;
}

sub adjust_soft_max {
    my $self = shift;
    $self->_set_soft_max_procs(
        min($self->hard_max_procs,
            $self->soft_max_procs + 
            max(1,int(($self->hard_max_procs - $self->soft_max_procs) * $self->idle / 100))
        )
    );
}

sub adjust_soft_min {
    my $self = shift;
    $self->_set_soft_min_procs(
        $self->hard_min_procs+
        int(($self->max_procs - $self->hard_min_procs)/2)
    );
}

sub adjust_up {
    my $self = shift;
    my $cur = $self->max_procs;

    my $max = $cur >= $self->soft_max_procs
        ? $self->adjust_soft_max
        : $self->soft_max_procs;

    $self->_set_soft_min_procs($cur);
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

    $self->_set_soft_max_procs($cur);
    $min + int(($cur - $min)/2);
}

1;
