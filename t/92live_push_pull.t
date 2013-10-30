#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;

use File::Basename qw( dirname );
use File::Temp qw( tempdir );
use IO::Async::Loop;
use YAML qw( LoadFile );

-r "t/.sfs3.conf" or plan skip_all => "No t/.sfs3.conf";

my $CONFIG = LoadFile( "t/.sfs3.conf" );

my $loop = IO::Async::Loop->new;

my $sfs3 = SocialFlow::S3->new(
   quiet => 1,
   ( map { $_ => $CONFIG->{$_} } qw( access_key secret_key bucket ) ),
);
$loop->add( $sfs3 );

my @PATHS = qw( A/1 A/2 A/3 B/4 B/5 C/6 );

# Cheating
my $s3 = $sfs3->{s3};

# Clean up the store first just in case
Future->needs_all( map {
   my $key = "data/test-tree/$_";
   $s3->head_object( key => $key )
      ->and_then( sub { $s3->delete_object( key => $key ) } )
      ->or_else( sub { Future->new->done } )
} @PATHS )->get;

# push
{
   my $dir = tempdir( CLEANUP => 1 );

   foreach my $name ( @PATHS ) {
      my $thisdir = "$dir/" . dirname( $name );
      -d $thisdir or mkdir $thisdir or die "Cannot mkdir - $!";
      open my $fh, ">", "$dir/$name" or die "Cannot write $dir/$name - $!";

      $fh->print( "A temporary file to unit-test SocialFlow::S3 created by $0\n" );
      $fh->close;
   }

   $sfs3->cmd_push( $dir, "test-tree" );

   pass( "->cmd_push OK" );

   # check they all got there
   foreach my $name ( @PATHS ) {
      my ( $resp ) = $s3->head_object( key => "data/test-tree/$name" )->get;
      is( $resp->code, 200, "\$s3->get_object on data for $name" );
   }

   # A second cmd_push should not fail

   $sfs3->cmd_push( $dir, "test-tree", skip_logic => "all" );

   pass( "->cmd_push OK a second time with skip_all => 'all'" );

   # A third cmd_push should not fail

   $sfs3->cmd_push( $dir, "test-tree" );

   pass( "->cmd_push OK a third time with skip_logic => 'stat'" );
}

# pull
{
   my $dir = tempdir( CLEANUP => 1 );

   $sfs3->cmd_pull( "test-tree", $dir );

   pass( "->cmd_pull OK" );

   foreach my $name ( @PATHS ) {
      ok( -s "$dir/$name", "$dir/$name pulled again" );
   }
}

done_testing;
