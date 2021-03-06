#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Refcount;

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

$s3->EXPECT_get_object(
   key     => "meta/key-1/md5sum",
   timeout => 10,
)->RETURN_F(
   md5_hex( $content )
)->PERSIST;

$s3->EXPECT_head_then_get_object(
   key           => "data/key-1",
   stall_timeout => 30,
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
})->PERSIST;

# ->_get_file_to_code
{
   my $got_content = "";
   my $f = $sfs3->_get_file_to_code(
      "key-1", my $code = sub { $got_content .= $_[1] if defined $_[1] }
   );

   is_refcount( $code, 2, 'Fetch sub {} has refcount 2 before get' );

   $f->get;

   no_more_expectations_ok;

   is( $got_content, $content, 'content of file' );
   is_oneref( $code, '_get_file_to_code has not leaked a ref to fetch sub {}' );
}

# ->get_file
{
   my $fh;
   my $written;
   $sfs3->EXPECT_fopen_write(
      path => "local-file",
   )->RETURN_WITH( sub {
      open $fh, ">", \$written;
      return $fh;
   });

   my $mtime;
   $sfs3->EXPECT_futime(
      path => "local-file"
   )->RETURN_WITH( sub {
      my %args = @_;
      $mtime = $args{mtime};
   });

   my $f = $sfs3->get_file( "key-1", "local-file" );

   $f->get;

   no_more_expectations_ok;

   is( $written, $content, 'content of file from ->get_file' );
   is( $mtime, 1380908459, 'utime() mtime of written file' );
}

$s3->EXPECT_get_object(
   key     => "meta/key-2/md5sum",
   timeout => 10,
)->RETURN_F(
   md5_hex( $content )
)->PERSIST;

# ->_get_file_to_code fails after MD5sum mismatch
{
   # First result corrupts the content
   $s3->EXPECT_head_then_get_object(
      key           => "data/key-2",
      stall_timeout => 30,
   )->RETURN_WITH( sub {
      my %args = @_;
      my $on_chunk = $args{on_chunk};
      my $header = HTTP::Response->new( 200, "OK",
         [
         ] );
      $on_chunk->( $header, uc $content );
      $on_chunk->( $header, undef );
      my $meta = { Mtime => "2013-10-04T17:40:59Z" };
      return $loop->new_future->done_later(
         $loop->new_future->done_later( uc $content, $header, $meta ), $header, $meta,
      );
   });

   my $got_content = "";
   my $f = $sfs3->_get_file_to_code(
      "key-2", sub { $got_content .= $_[1] if defined $_[1] }
   );

   no_more_expectations_ok;

   my $failure = $f->failure;
   ok( $failure, '_get_file_to_code fails after MD5sum mismatch' );
}

# ->get_file retries after MD5sum mismatch
{
   # First result corrupts the content
   $s3->EXPECT_head_then_get_object(
      key           => "data/key-2",
      stall_timeout => 30,
   )->RETURN_WITH( sub {
      my %args = @_;
      my $on_chunk = $args{on_chunk};
      my $header = HTTP::Response->new( 200, "OK",
         [
         ] );
      $on_chunk->( $header, uc $content );
      $on_chunk->( $header, undef );
      my $meta = { Mtime => "2013-10-04T17:40:59Z" };
      return $loop->new_future->done_later(
         $loop->new_future->done_later( uc $content, $header, $meta ), $header, $meta,
      );
   });

   # Second result is correct
   $s3->EXPECT_head_then_get_object(
      key           => "data/key-2",
      stall_timeout => 30,
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

   my $fh;
   my $written;
   $sfs3->EXPECT_fopen_write(
      path => "local-file",
   )->RETURN_WITH( sub {
      open $fh, ">", \$written;
      return $fh;
   });

   my $mtime;
   $sfs3->EXPECT_futime(
      path => "local-file"
   )->RETURN_WITH( sub {
      my %args = @_;
      $mtime = $args{mtime};
   });

   my $f = $sfs3->get_file( "key-2", "local-file" );

   # This test involves timeouts
   $f->get;

   no_more_expectations_ok;

   is( $written, $content, 'content of file from ->get_file' );
   is( $mtime, 1380908459, 'utime() mtime of written file' );
}

done_testing;
