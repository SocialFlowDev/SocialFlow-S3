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

my %S3CONTENT = (
   "tree/A/1" => "one",
   "tree/A/2" => "two",
   "tree/B/3" => "three",
);
my %S3CONTENT_FOR_HASH = %S3CONTENT;
my $MTIME = "2013-10-07T23:24:25Z";

$s3->EXPECT_list_bucket(
   prefix => MATCHES(qr(^data/tree)),
)->RETURN_WITH( sub {
   my %args = @_;
   my $prefix = $args{prefix};
   my @keys;
   foreach ( keys %S3CONTENT ) {
      my $key = "data/$_";
      push @keys, {
         key  => $key,
         size => length $S3CONTENT{$_},
      } if $key =~ m/^\Q$prefix/;
   }
   return $loop->new_future->done_later( \@keys, [] );
})->PERSIST;

foreach my $k ( keys %S3CONTENT ) {
   my $content = $S3CONTENT{$k};

   $s3->EXPECT_get_object( key => "meta/$k/md5sum" )->RETURN_WITH( sub {
      return Future->new->done( md5_hex( $S3CONTENT_FOR_HASH{$k} ) )
   })->PERSIST;

   $s3->EXPECT_head_object( key => "data/$k" )->RETURN_WITH( sub {
      my %args = @_;
      my $header = HTTP::Response->new( 200, "OK",
         [
            "Content-Length" => length( $content ),
         ] );
      return $loop->new_future->done_later( $header, { Mtime => $MTIME } );
   })->PERSIST;

   $s3->EXPECT_head_then_get_object( key => "data/$k" )->RETURN_WITH( sub {
      my %args = @_;
      my $on_chunk = $args{on_chunk};
      my $header = HTTP::Response->new( 200, "OK",
         [
            "Content-Length" => length( $content ),
         ] );
      $on_chunk->( $header, $content );
      $on_chunk->( $header, undef );
      my $meta = { Mtime => $MTIME };
      return $loop->new_future->done_later(
         $loop->new_future->done_later( $content, $header, $meta ), $header, $meta,
      );
   })->PERSIST;
}

# md5check - OK
{
   my $ret = $sfs3->cmd_md5check( "tree" );

   no_more_expectations_ok;

   is( $ret, 0, 'returned no differences' );
}

# md5check - fails
{
   local $S3CONTENT_FOR_HASH{"tree/A/1"} = "HELLO";

   my $ret = $sfs3->cmd_md5check( "tree" );

   no_more_expectations_ok;

   is( $ret, 1, 'returned differences' );
}

done_testing;
