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

# rm key-3
{
   $s3->EXPECT_list_bucket(
      delimiter => "/",
      prefix    => "meta/key-3/"
   )->RETURN_F(
      [
         { key => "meta/key-3/md5sum" },
      ],
      []
   );

   $s3->EXPECT_delete_object(
      key => "data/key-3"
   )->RETURN_F();

   $s3->EXPECT_delete_object(
      key => "meta/key-3/md5sum"
   )->RETURN_F();

   $sfs3->cmd_rm( "key-3" );

   no_more_expectations_ok;
}

done_testing;
