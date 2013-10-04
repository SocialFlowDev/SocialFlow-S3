#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::MockS3;
use HTTP::Response;

use IO::Async::Loop;

my $sfs3 = SocialFlow::S3->new(
   s3     => my $s3 = t::MockS3->new,
   bucket => "bucket-name/with-prefix",
   quiet  => 1,
);
IO::Async::Loop->new->add( $sfs3 );

my %put_meta;
my $put_content = "";
my $put_md5sum;

$s3->EXPECT_put_object(
   key => "data/key-1"
)->RETURN_WITH( sub {
   my %args = @_;
   my $gen_parts = $args{gen_parts};
   %put_meta = %{ $args{meta} };

   while( my @part = $gen_parts->() ) {
      # $part[0] should be a Future
      $put_content .= $part[0]->get;
   }

   # MD5sum and length in bytes
   return Future->new->done( "157e3a08ddc87ae336292e4a363b715d", 21 );
});

$s3->EXPECT_put_object(
   key => "meta/key-1/md5sum"
)->RETURN_WITH( sub {
   my %args = @_;
   $put_md5sum = $args{value};

   return Future->new->done( "ETAG", 32 );
});

{
   # Can't just pass an in-memory filehandle as IO::Async won't like it
   pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
   $wr->print( "A new value for key-1" );
   $wr->close;

   $sfs3->cmd_uncat( "key-1", stdin => $rd );

   # Avoid race condition in timing
   like( delete $put_meta{Mtime}, qr/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/,
         'PUT metadata Mtime' );

   is( $put_content, "A new value for key-1", 'PUT content' );

   is( $put_md5sum, "157e3a08ddc87ae336292e4a363b715d\n", 'PUT meta md5' );
}

ok( $s3->NO_MORE_EXPECTATIONS, 'All expected methods called' );

done_testing;
