#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::Mocking;
use Digest::MD5 qw( md5_hex );
use HTTP::Response;

use IO::Async::Loop;

my $sfs3 = SocialFlow::S3->new(
   # No mocked S3 because we need a real one
   access_key => "ACCESS KEY GOES HERE",
   secret_key => "AND THE SECRET KEY GOES IN HERE AS WELL.",
   bucket     => "bucket-name/prefix",
   get_retries => 0,
);
$sfs3->{s3}->configure( max_retries => 1 );
( my $loop = IO::Async::Loop->new )->add( $sfs3 );

# TODO: I really should write a proper mock HTTP agent sometime...
t::Mocking->mock_methods_into( "Net::Async::HTTP", qw(
   do_request
));
my $http = $sfs3->{s3}->{http} or
   die "TODO - can't find NaWS:S3's NaHTTP object to mock methods into it";

my $content = "Here is the full content of the S3 key";

$http->EXPECT_do_request(
   request => MATCHES( sub {
      my ( $req ) = @_;
      $req->uri->path eq "/prefix/meta/key/md5sum"
   }),
   stall_timeout => 30,
   timeout       => 10,
)->RETURN_WITH( sub {
   my %args = @_;
   my $on_header = $args{on_header};
   my $on_body = $on_header->( my $resp = HTTP::Response->new( 200, "OK", [] ) );
   $on_body->( md5_hex( $content ) );
   $on_body->();
   return Future->new->done( $resp );
})->PERSIST;

# resume after stall timeout
{
   my $stall_after = 10;

   # First stall
   $http->EXPECT_do_request(
      request => MATCHES( sub {
         my ( $req ) = @_;
         $req->uri->path eq "/prefix/data/key"
      }),
      stall_timeout => 30,
      timeout       => undef,
   )->RETURN_WITH( sub {
      my %args = @_;
      my $on_header = $args{on_header};
      my $on_body = $on_header->( HTTP::Response->new( 200, "OK", [
         'ETag' => "the etag",
      ] ) );
      $on_body->( substr( $content, 0, $stall_after ) );
      return Future->new->fail( "Stall timeout", stall_timeout => );
   });

   # Second fetch OK
   $http->EXPECT_do_request(
      request => MATCHES( sub {
         my ( $req ) = @_;
         $req->uri->path eq "/prefix/data/key" or return 0;

         is( $req->header( "Range" ), "bytes=$stall_after-", 'Resume request Range header' );
         is( $req->header( "If-Match" ), "the etag", 'Resume request If-Match header' );

         return 1;
      }),
      stall_timeout => 30,
      timeout       => undef,
   )->RETURN_WITH( sub {
      my %args = @_;
      my $on_header = $args{on_header};
      my $on_body = $on_header->( my $resp = HTTP::Response->new( 200, "OK", [
         'ETag' => "the etag",
      ] ) );
      $on_body->( substr( $content, $stall_after ) );
      return Future->new->done( $resp );
   });

   my $got_content = "";
   my $f = $sfs3->_get_file_to_code(
      "key", sub { $got_content .= $_[1] if defined $_[1] }
   );

   $f->get;

   no_more_expectations_ok;
}

done_testing;
