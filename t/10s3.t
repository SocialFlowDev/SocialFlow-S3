#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;

my $sfs3 = SocialFlow::S3->new(
   s3 => MockS3->new,
   bucket => "bucket-name/with-prefix",
);

ok( defined $sfs3, '$sfs3 defined' );

my %http_config;
my %s3_config;

is_deeply( \%http_config,
           { max_connections_per_host => 0 },
           'NaHTTP config' );

is_deeply( \%s3_config,
           { bucket        => "bucket-name",
             prefix        => "with-prefix",
             stall_timeout => 30,
             timeout       => 10, },
           'NaWS:S3 config' );

my $get_object_f = Future->new;

# internal API but an easy way to test this bit
my $f = $sfs3->get_meta( "some-filepath", "the-key" );

ok( defined $f, '->get_meta yields future' );
is_deeply( \my %get_object_args,
           { key     => "meta/some-filepath/the-key",
             timeout => 10 },
           '->get_object arguments' );

$get_object_f->done( "value", [], {} ); # fake response and meta as we don't need them

is_deeply( [ $f->get ],
           [ "value", [], {} ],
           '->get_object->get result' );

done_testing;

package MockS3;
use base qw( IO::Async::Notifier );

sub _init
{
   my $self = shift;
   $self->{http} = MockHttp->new;
   return $self->SUPER::_init( @_ );
}

sub configure
{
   my $self = shift;
   my %args = @_;

   exists $args{$_} and $s3_config{$_} = delete $args{$_}
      for qw( timeout stall_timeout bucket prefix );

   $self->SUPER::configure( %args );
}

sub get_object
{
   my $self = shift;
   %get_object_args = @_;

   return $get_object_f;
}

package MockHttp;

sub new
{
   my $class = shift;
   my $self = bless {}, $class;
   return $self;
}

sub configure
{
   shift;
   %http_config = @_;
}
