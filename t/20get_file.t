#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::Mocking;
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
   my $content = "";
   my $f = $sfs3->_get_file_to_code( "key-1", sub { $content .= $_[1] if defined $_[1] } );

   no_more_expectations_ok;

   $f->get;

   is( $content, "The value of key-1", 'content of file' );
}

done_testing;
