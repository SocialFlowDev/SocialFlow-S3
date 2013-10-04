#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::MockS3;
use HTTP::Response;

my $sfs3 = SocialFlow::S3->new(
   s3     => my $s3 = t::MockS3->new,
   bucket => "bucket-name/with-prefix",
   quiet  => 1,
);

my $content = "The value of key-1";

$s3->EXPECT_get_object(
   key => "meta/key-1/md5sum"
)->RETURN_F(
   "e28cbeebcc243df62a59d90ddfe4b3e8" # md5sum of $content
);

$s3->EXPECT_get_object(
   key => "data/key-1"
)->RETURN_WITH( sub {
   my %args = @_;
   my $on_chunk = $args{on_chunk};
   my $header = HTTP::Response->new( 200, "OK",
      [
      ] );
   $on_chunk->( $header, $content );
   $on_chunk->( $header, undef );
   return Future->new->done( $content, $header, {} );
});

{
   open my $outh, ">", \(my $output = "");

   $sfs3->cmd_cat( "key-1", stdout => $outh );

   is( $output, "The value of key-1", 'output from cmd_cat' );
}

ok( $s3->NO_MORE_EXPECTATIONS, 'All expected methods called' );

done_testing;
