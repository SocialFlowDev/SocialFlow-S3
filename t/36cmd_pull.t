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
   fopen_write fstat_type_size_mtime futime
));

my %CONTENT = (
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
   foreach ( keys %CONTENT ) {
      my $key = "data/$_";
      push @keys, {
         key  => $key,
         size => length $CONTENT{$_},
      } if $key =~ m/^\Q$prefix/;
   }
   return $loop->new_future->done_later( \@keys, [] );
})->PERSIST;

foreach my $k ( keys %CONTENT ) {
   my $content = $CONTENT{$k};

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

# pull --all
{
   my %written_content;

   foreach my $f (qw( A/1 A/2 B/3 )) {
      $sfs3->EXPECT_fopen_write( path => "tree/$f" )->RETURN_WITH( sub {
         open my $fh, ">", \$written_content{"tree/$f"};
         return $fh;
      });

      $sfs3->EXPECT_futime( path => "tree/$f",
         atime => 1381188265,
         mtime => 1381188265,
      )->RETURN();
   }

   $sfs3->cmd_pull( "tree", "tree", skip_logic => "all" );

   no_more_expectations_ok;

   is_deeply( \%written_content,
              \%CONTENT,
              'written content of files' );
}

# pull [default == stat]
{
   # claim two files are up to date, one not
   $sfs3->EXPECT_fstat_type_size_mtime( path => "tree/A/1" )
      ->RETURN( "f", 3, 1381188265 );
   $sfs3->EXPECT_fstat_type_size_mtime( path => "tree/A/2" )
      ->RETURN( "f", 3, 1381000000 );
   $sfs3->EXPECT_fstat_type_size_mtime( path => "tree/B/3" )
      ->RETURN( "f", 5, 1381188265 );

   my %written_content;

   foreach my $f (qw( A/2 )) {
      $sfs3->EXPECT_fopen_write( path => "tree/$f" )->RETURN_WITH( sub {
         open my $fh, ">", \$written_content{"tree/$f"};
         return $fh;
      });

      $sfs3->EXPECT_futime( path => "tree/$f",
         atime => 1381188265,
         mtime => 1381188265,
      )->RETURN();
   }

   $sfs3->cmd_pull( "tree", "tree", skip_logic => "stat" );

   no_more_expectations_ok;
}

# S3 path canonicalisation
{
   foreach my $path ( "root", "/root", "root/", "/root/" ) {
      $s3->EXPECT_list_bucket(
         prefix => "data/root"
      )->RETURN_F(
         [ { key => "data/root/key", size => 5 } ], [],
      );

      $s3->EXPECT_head_then_get_object(
         key => "data/root/key"
      )->RETURN_WITH( sub {
         my %args = @_;
         my $on_chunk = $args{on_chunk};
         my $header = HTTP::Response->new( 200, "OK", [] );
         $on_chunk->( $header, "Hello" );
         $on_chunk->( $header, undef );
         return Future->new->done(
            Future->new->done( "Hello", $header, {} ), $header, {},
         );
      });

      $s3->EXPECT_get_object(
         key => "meta/root/key/md5sum"
      )->RETURN_F(
         md5_hex( "Hello" )
      )->PERSIST;

      $sfs3->EXPECT_fopen_write(
         path => "local/key",
      )->RETURN_WITH( sub {
         open my $fh, ">", \my $tmp;
         return $fh;
      });

      $sfs3->cmd_pull( $path, "local", skip_logic => "all" );

      no_more_expectations_ok;
   }
}

done_testing;
