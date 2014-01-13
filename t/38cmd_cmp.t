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
   fopen_read freaddir fstat_type_size_mtime
));

my %S3CONTENT = (
   "tree/A/1" => "one",
   "tree/A/2" => "two",
   "tree/B/3" => "three",
);
my %LOCALCONTENT = (
   "tree/A/1" => "one",
   "tree/A/2" => "two",
   "tree/B/3" => "three",
);
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

   $s3->EXPECT_get_object( key => "meta/$k/md5sum" )->RETURN_F(
      md5_hex( $content )
   )->PERSIST;

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

$sfs3->EXPECT_freaddir(
   path => MATCHES(qr(^tree)),
)->RETURN_WITH( sub {
   my %args = @_;
   my $path = $args{path};
   my %ents;
   foreach my $f ( keys %LOCALCONTENT ) {
      $ents{$1}++ if $f =~ m(^\Q$path\E/([^/]+));
   }
   return keys %ents;
})->PERSIST;

my %FH_2_FILE;
foreach my $k ( keys %LOCALCONTENT ) {
   $sfs3->EXPECT_fopen_read( path => $k )->RETURN_WITH( sub {
      # Can't just pass an in-memory filehandle as IO::Async won't like it
      pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
      $wr->print( $LOCALCONTENT{$k} );
      $wr->close;

      $FH_2_FILE{$rd} = $k;
      return $rd;
   })->PERSIST;
}

$sfs3->EXPECT_fstat_type_size_mtime()->RETURN_WITH( sub {
   my %args = @_;
   my $path = $args{path} // $FH_2_FILE{$args{fh}};
   if( exists $LOCALCONTENT{$path} ) {
      return "f", length $LOCALCONTENT{$path}, 1381188265;
   }
   elsif( grep { $_ =~ m(^\Q$path\E/) } keys %LOCALCONTENT ) {
      return "d", 0, 1381188265;
   }
   elsif( defined $path and $path eq "local/key" ) {
      return "f", 5, 1383154075;
   }
})->PERSIST;

# cmp - OK
{
   my $ret = $sfs3->cmd_cmp( "tree", "tree" );

   no_more_expectations_ok;

   is( $ret, 0, 'returned no differences' );
}

# content differs
{
   local $LOCALCONTENT{"tree/A/1"} = "NEW";

   my $ret = $sfs3->cmd_cmp( "tree", "tree" );

   no_more_expectations_ok;

   is( $ret, 1, 'returned differences in files' );
}

# filesets differ
{
   local $LOCALCONTENT{"tree/A/4"} = "another";

   my $ret = $sfs3->cmd_cmp( "tree", "tree" );

   no_more_expectations_ok;

   is( $ret, 2, 'returned differences in trees' );
}

done_testing;
