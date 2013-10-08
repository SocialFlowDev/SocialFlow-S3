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

t::Mocking->mock_methods_into( "SocialFlow::S3", qw(
   fopen_write futime
));

my %CONTENT = (
                 # value    md5sum
   "tree/A/1" => [ "one",   "f97c5d29941bfb1b2fdab0874906ab82" ],
   "tree/A/2" => [ "two",   "b8a9f715dbb64fd5c56e7783c6820a61" ],
   "tree/B/3" => [ "three", "35d6d33467aae9a2e3dccb4b6b027878" ],
);
my $MTIME = "2013-10-07T23:24:25Z";

$s3->EXPECT_list_bucket()->RETURN_WITH( sub {
   my %args = @_;
   my $prefix = $args{prefix};
   my @keys;
   foreach ( keys %CONTENT ) {
      my $key = "data/$_";
      push @keys, {
         key  => $key,
         size => length $CONTENT{$_}[0],
      } if $key =~ m/^\Q$prefix/;
   }
   return Future->new->done( \@keys, [] );
});

foreach my $k ( keys %CONTENT ) {
   my $content = $CONTENT{$k}[0];

   $s3->EXPECT_get_object( key => "meta/$k/md5sum" )->RETURN_F(
      $CONTENT{$k}[1]
   );

   $s3->EXPECT_get_object( key => "data/$k" )->RETURN_WITH( sub {
      my %args = @_;
      my $on_chunk = $args{on_chunk};
      my $header = HTTP::Response->new( 200, "OK",
         [
         ] );
      $on_chunk->( $header, $content );
      $on_chunk->( $header, undef );
      return Future->new->done( $content, $header, { Mtime => $MTIME } );
   });
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
}

done_testing;
