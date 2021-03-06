#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::Mocking;
use t::MockS3;
use Digest::MD5 qw( md5_hex );
use HTTP::Response;

use IO::Async::Loop;

my $sfs3 = SocialFlow::S3->new(
   s3     => my $s3 = t::MockS3->new,
   bucket => "bucket-name/with-prefix",
   quiet  => 1,
);
( my $loop = IO::Async::Loop->new )->add( $sfs3 );

t::Mocking->mock_methods_into( "SocialFlow::S3", qw(
   fopen_write futime
));

my $content = "The value of key-1";

# get key-1 local-file
{
   my $fh;
   my $written;
   $sfs3->EXPECT_fopen_write(
      path => "local-file",
   )->RETURN_WITH( sub {
      open $fh, ">", \$written;
      return $fh;
   });

   $s3->EXPECT_get_object(
      key => "meta/key-1/md5sum"
   )->RETURN_F(
      md5_hex( $content )
   );

   $s3->EXPECT_head_then_get_object(
      key => "data/key-1"
   )->RETURN_WITH( sub {
      my %args = @_;
      my $on_chunk = $args{on_chunk};
      my $header = HTTP::Response->new( 200, "OK",
         [
         ] );
      $on_chunk->( $header, $content );
      $on_chunk->( $header, undef );
      my $meta = { Mtime => "2013-10-04T17:40:59Z" };
      return $loop->new_future->done_later(
         $loop->new_future->done_later( $content, $header, $meta ), $header, $meta,
      );
   });

   my $mtime;
   $sfs3->EXPECT_futime(
      path => "local-file"
   )->RETURN_WITH( sub {
      my %args = @_;
      $mtime = $args{mtime};
   });

   $sfs3->cmd_get( "key-1", "local-file" );

   no_more_expectations_ok;

   is( $written, $content, 'content of file from ->get_file' );
   is( $mtime, 1380908459, 'utime() mtime of written file' );
}

done_testing;
