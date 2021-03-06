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

my %CONTENT = (
   "tree/A/1" => "one",
   "tree/A/2" => "two",
   "tree/B/3" => "three",
);
my $MTIME = "2013-10-07T23:24:25Z";

$sfs3->EXPECT_freaddir(
   path => MATCHES(qr(^tree)),
)->RETURN_WITH( sub {
   my %args = @_;
   my $path = $args{path};
   my %ents;
   foreach my $f ( keys %CONTENT ) {
      $ents{$1}++ if $f =~ m(^\Q$path\E/([^/]+));
   }
   return keys %ents;
})->PERSIST;

my %FH_2_FILE;
foreach my $k ( keys %CONTENT ) {
   $sfs3->EXPECT_fopen_read( path => $k )->RETURN_WITH( sub {
      # Can't just pass an in-memory filehandle as IO::Async won't like it
      pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
      $wr->print( $CONTENT{$k} );
      $wr->close;

      $FH_2_FILE{$rd} = $k;
      return $rd;
   })->PERSIST;
}

$sfs3->EXPECT_fstat_type_size_mtime()->RETURN_WITH( sub {
   my %args = @_;
   my $path = $args{path} // $FH_2_FILE{$args{fh}};
   if( exists $CONTENT{$path} ) {
      return "f", length $CONTENT{$path}, 1381188265;
   }
   elsif( grep { $_ =~ m(^\Q$path\E/) } keys %CONTENT ) {
      return "d", 0, 1381188265;
   }
   elsif( defined $path and $path eq "local/key" ) {
      return "f", 5, 1383154075;
   }
})->PERSIST;

# push --all
{
   $s3->EXPECT_put_object(
      key => "data/$_"
   )->RETURN_WITH( sub {
      my %args = @_;
      my $gen_parts = $args{gen_parts};

      my $content = "";
      while( my @part = $gen_parts->() ) {
         # $part[0] should be a Future
         $content .= $part[0]->get;
      }

      return $loop->new_future->done_later( md5_hex( $content ), length $content );
   }) for keys %CONTENT;

   $s3->EXPECT_put_object(
      key => "meta/$_/md5sum"
   )->RETURN_F(
      "ETAG", 32
   ) for keys %CONTENT;

   $sfs3->cmd_push( "tree", "tree", skip_logic => "all" );

   no_more_expectations_ok;
}

# pull [default == stat]
{
   # claim two files are up to date, one not
   foreach my $k ( keys %CONTENT ) {
      my $content = $CONTENT{$k};
      my $mtime = $k eq "tree/A/2" ? "2013-09-01T12:34:56Z" : $MTIME;

      $s3->EXPECT_get_object( key => "meta/$k/md5sum" )->RETURN_F(
         md5_hex( $content )
      );

      $s3->EXPECT_head_object( key => "data/$k" )->RETURN_WITH( sub {
         my %args = @_;
         my $header = HTTP::Response->new( 200, "OK",
            [
               "Content-Length" => length( $content ),
            ] );
         return $loop->new_future->done_later( $header, { Mtime => $mtime } );
      });
   }

   $s3->EXPECT_put_object( key => "data/tree/A/2" )->RETURN_WITH( sub {
      my %args = @_;
      my $gen_parts = $args{gen_parts};

      my $content = "";
      while( my @part = $gen_parts->() ) {
         # $part[0] should be a Future
         $content .= $part[0]->get;
      }

      $loop->new_future->done_later( md5_hex( $content ), length $content );
   });

   $s3->EXPECT_put_object(
      key => "meta/tree/A/2/md5sum"
   )->RETURN_F(
      "ETAG", 32
   );

   $sfs3->cmd_push( "tree", "tree", skip_logic => "stat" );

   no_more_expectations_ok;
}

# S3 path canonicalisation
{
   foreach my $path ( "root", "/root", "root/", "/root/" ) {
      $sfs3->EXPECT_freaddir(
         path => "local"
      )->RETURN(qw( key ));

      $sfs3->EXPECT_fopen_read(
         path => "local/key",
      )->RETURN_WITH( sub {
         pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
         $wr->print( "Hello" );
         $wr->close;

         $FH_2_FILE{$rd} = "local/key";
         return $rd;
      });

      $s3->EXPECT_put_object( key => "data/root/key" )->RETURN_WITH( sub {
         my %args = @_;
         my $gen_parts = $args{gen_parts};

         my $content = "";
         while( my @part = $gen_parts->() ) {
            # $part[0] should be a Future
            $content .= $part[0]->get;
         }

         return Future->new->done( md5_hex( $content ), length $content );
      });

      $s3->EXPECT_put_object( key => "meta/root/key/md5sum" )->RETURN_F(
         "ETAG", 32
      );

      $sfs3->cmd_push( "local", $path, skip_logic => "all" );

      no_more_expectations_ok;
   }
}

done_testing;
