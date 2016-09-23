#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;
use HTTP::Request::Common;

use Catalyst::Test 'App::DynIP';

my $req = request(GET '/client/update', 'x-auth-token' => 'secret_token');
ok( $req->is_success, 'Request should succeed' );

done_testing();
