#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::Mocking;
use t::MockS3;
use File::Temp qw( tempfile );
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

my $content = "A new value for key-1";

my %put_meta;
my $put_content = "";
my $put_md5sum;

$s3->EXPECT_put_object(
   key => "data/key-1"
)->RETURN_WITH( sub {
   my %args = @_;
   my $gen_parts = $args{gen_parts};
   %put_meta = %{ $args{meta} };

   my $pos = 0;
   while( my @part = $gen_parts->() ) {
      # $part[0] should be a Future or CODE
      if( ref $part[0] eq "CODE" ) {
         $put_content .= $part[0]->( $pos, $part[1] );
         $pos = length $put_content;
      }
      else {
         $put_content .= $part[0]->get;
      }
   }

   # MD5sum and length in bytes
   return $loop->new_future->done_later( md5_hex( $put_content ), 21 );
})->PERSIST;

$s3->EXPECT_put_object(
   key => "meta/key-1/md5sum"
)->RETURN_WITH( sub {
   my %args = @_;
   $put_md5sum = $args{value};

   return $loop->new_future->done_later( "ETAG", 32 );
})->PERSIST;

# ->_put_file_from_fh (regular)
{
   ( %put_meta, $put_content, $put_md5sum ) = ();

   # Can't just pass an in-memory filehandle as IO::Async won't like it
   my $fh = tempfile();
   $fh->print( $content );
   $fh->autoflush(1);
   $fh->seek( 0, 0 );

   # 2013-10-04 14:26:04 UTC
   my $f = $sfs3->_put_file_from_fh( $fh, "key-1", mtime => 1380896764 );

   no_more_expectations_ok;

   $f->get;

   is( $put_meta{Mtime}, "2013-10-04T14:26:04Z", 'PUT metadata Mtime' );
   is( $put_content, "A new value for key-1", 'PUT content' );

   is( $put_md5sum, "157e3a08ddc87ae336292e4a363b715d\n", 'PUT meta md5' );
}

# ->_put_file_from_fh (pipe)
{
   ( %put_meta, $put_content, $put_md5sum ) = ();

   # Can't just pass an in-memory filehandle as IO::Async won't like it
   pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
   $wr->print( $content );
   $wr->close;

   # 2013-10-04 14:26:04 UTC
   my $f = $sfs3->_put_file_from_fh( $rd, "key-1", mtime => 1380896764 );

   no_more_expectations_ok;

   $f->get;

   is( $put_meta{Mtime}, "2013-10-04T14:26:04Z", 'PUT metadata Mtime' );
   is( $put_content, "A new value for key-1", 'PUT content' );

   is( $put_md5sum, "157e3a08ddc87ae336292e4a363b715d\n", 'PUT meta md5' );
}

# ->_put_file_from_fh to create a new file
{
   ( %put_meta, $put_content, $put_md5sum ) = ();

   $s3->EXPECT_put_object(
      key => "data/key-new",
   )->RETURN_WITH( sub {
      my %args = @_;
      my $gen_parts = $args{gen_parts};
      %put_meta = %{ $args{meta} };

      while( my @part = $gen_parts->() ) {
         # $part[0] should be a Future
         $put_content .= $part[0]->get;
      }

      # MD5sum and length in bytes
      return $loop->new_future->done_later( md5_hex( $put_content ), 21 );
   });

   $s3->EXPECT_put_object(
      key => "meta/key-new/md5sum",
   )->RETURN_WITH( sub {
      my %args = @_;
      $put_md5sum = $args{value};

      return $loop->new_future->done_later( "ETAG", 32 );
   });

   # Can't just pass an in-memory filehandle as IO::Async won't like it
   pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
   $wr->print( "New content" );
   $wr->close;

   # 2013-10-25 18:47:09 UTC
   my $f = $sfs3->_put_file_from_fh( $rd, "key-new", mtime => 1382726829 );

   $f->get;

   no_more_expectations_ok;

   is( $put_content, "New content", 'PUT content for new key' );
}

# ->_put_file_from_fh with small enough part size to do multi-part
{
   # CHEATING
   local $sfs3->{part_size} = 16;

   my @put_parts;
   $s3->EXPECT_put_object(
      key => "data/key-split",
   )->RETURN_WITH( sub {
      my %args = @_;
      my $gen_parts = $args{gen_parts};

      while( my @part = $gen_parts->() ) {
         # $part[0] should be a Future
         push @put_parts, scalar $part[0]->get;
      }

      my $content = join "", @put_parts;
      return $loop->new_future->done_later( md5_hex( $content ), length $content );
   });

   $s3->EXPECT_put_object(
      key => "meta/key-split/md5sum",
   )->RETURN_WITH( sub {
      return $loop->new_future->done_later( "ETAG", 32 );
   });

   # Can't just pass an in-memory filehandle as IO::Async won't like it
   pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
   $wr->print( "X" x 20 );
   $wr->close;

   my $f = $sfs3->_put_file_from_fh( $rd, "key-split", mtime => 1382730146 );

   $f->get;

   no_more_expectations_ok;

   is_deeply( \@put_parts,
              [ "X" x 16, "X" x 4 ],
              '@put_parts from PUT content with multipart split' );
}

# ->put_file
{
   ( %put_meta, $put_content, $put_md5sum ) = ();

   $sfs3->EXPECT_fopen_read(
      path => "local-file"
   )->RETURN_WITH( sub {
      # Can't just pass an in-memory filehandle as IO::Async won't like it
      pipe( my ( $rd, $wr ) ) or die "Cannot pipe() - $!";
      $wr->print( "A new value for key-1" );
      $wr->close;

      return $rd;
   });

   $sfs3->EXPECT_fstat_type_size_mtime(
   )->RETURN(
      "f", # type
      21, # length
      1380896764, # mtime
   );

   my $f = $sfs3->put_file( "local-file", "key-1" );

   no_more_expectations_ok;

   $f->get;

   is( $put_meta{Mtime}, "2013-10-04T14:26:04Z", 'PUT metadata Mtime' );
   is( $put_content, "A new value for key-1", 'PUT content' );

   is( $put_md5sum, "157e3a08ddc87ae336292e4a363b715d\n", 'PUT meta md5' );
}

done_testing;
