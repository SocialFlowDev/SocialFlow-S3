#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;

use File::Temp qw( tempfile );
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

# Cheating
my $s3 = $sfs3->{s3};

# put
{
   my ( $fh, $filename ) = tempfile();
   $fh->print( <<"EOF" );
A temporary file to unit-test SocialFlow::S3 created by $0
EOF
   $fh->close;

   $sfs3->cmd_put( $filename, "test-key" );

   pass( "->cmd_put OK" );

   # check it got put there
   my ( $value, $resp ) = $s3->get_object( key => "data/test-key" )->get;
   is( $resp->code, 200, '$s3->get_object on data' );
   like( $value, qr/^A temporary file to unit-test SocialFlow::S3 /, 'content of put file' );

   ( $resp ) = $s3->head_object( key => "meta/test-key/md5sum" )->get;
   is( $resp->code, 200, '$s3->head_object on md5sum metadata' );
}

# get
{
   my ( $fh, $filename ) = tempfile();

   $sfs3->cmd_get( "test-key", $filename );

   pass( "->cmd_get OK" );

   my $value = do { local $/; <$fh> };
   like( $value, qr/^A temporary file to unit-test SocialFlow::S3 /, 'received file content from get' );
}

done_testing;
