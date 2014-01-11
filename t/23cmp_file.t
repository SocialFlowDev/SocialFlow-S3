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
   fopen_read fstat_type_size_mtime
));

my $s3content    = "The value of key-1";
my $localcontent = "The value of key-1";
my $s3mtime      = "2013-10-04T14:26:04Z";

$s3->EXPECT_get_object(
   key     => "meta/key-1/md5sum",
   timeout => 10,
)->RETURN_F(
   md5_hex( $s3content )
)->PERSIST;

$s3->EXPECT_head_then_get_object(
   key           => "data/key-1",
   stall_timeout => 30,
)->RETURN_WITH( sub {
   my %args = @_;
   my $on_chunk = $args{on_chunk};
   my $header = HTTP::Response->new( 200, "OK",
      [
         "Content-Length" => length( $s3content ),
      ] );
   $on_chunk->( $header, $s3content );
   $on_chunk->( $header, undef );
   my $meta = { Mtime => $s3mtime };
   return $loop->new_future->done_later(
      $loop->new_future->done_later( $s3content, $header, $meta ), $header, $meta,
   );
})->PERSIST;

$sfs3->EXPECT_fopen_read(
   path => "local-file"
)->RETURN_WITH( sub {
   # Can't just pass an in-memory filehandle as IO::Async won't like it
   pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
   $wr->print( $localcontent );
   $wr->close;

   return $rd;
})->PERSIST;

$sfs3->EXPECT_fstat_type_size_mtime(
)->RETURN_WITH( sub {
   "f", # type
   length( $localcontent ), # length
   1380896764, # mtime
})->PERSIST;

# cmp_file OK
{
   my $f = $sfs3->cmp_file( "local-file", "key-1" );

   my $result = $f->get;

   no_more_expectations_ok;

   is( $result, undef, 'cmp_file indicated no difference' );
}

# cmp_file size
{
   $localcontent .= "more content";

   my $f = $sfs3->cmp_file( "local-file", "key-1" );

   my $result = $f->get;

   no_more_expectations_ok;

   is( $result, "size", 'cmp_file indicated size difference' );
}

# cmp_file mtime
{
   $localcontent = $s3content;
   $s3mtime = "2014-01-11T03:22:43Z";

   my $f = $sfs3->cmp_file( "local-file", "key-1" );

   my $result = $f->get;

   no_more_expectations_ok;

   is( $result, "mtime", 'cmp_file indicated mtime difference' );
}

# cmp_file bytes
{
   $localcontent = uc $s3content;
   $s3mtime = "2013-10-04T14:26:04Z";

   my $f = $sfs3->cmp_file( "local-file", "key-1" );

   my $result = $f->get;

   no_more_expectations_ok;

   is( $result, "bytes", 'cmp_file indicated bytes difference' );
}

done_testing;
