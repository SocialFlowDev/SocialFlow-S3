#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use SocialFlow::S3;
use t::Mocking;
use t::MockS3;
use HTTP::Response;

my $sfs3 = SocialFlow::S3->new(
   s3 => my $s3 = t::MockS3->new,
   bucket => "bucket-name/with-prefix",
);

$s3->EXPECT_list_bucket(
   delimiter => "/",
   prefix => "data/",
)->RETURN_WITH( sub {
   # ->cmd_list destroys the list of prefixes
   return Future->new->done(
      [
         { key => "data/key-1" },
         { key => "data/key-2" },
         { key => "data/key-3" },
      ],
      [qw( data/prefix-1 data/prefix-2 )]
   );
})->PERSIST;

# ls
{
   open my $outh, ">", \(my $output = "");

   $sfs3->cmd_ls( "", stdout => $outh );

   $output =~ s/ +$//mg;
   is( $output, <<'EOF',
prefix-1                               DIR
prefix-2                               DIR
key-1
key-2
key-3
EOF
   'output from cmd_ls short no-recurse' );

   no_more_expectations_ok;
}

# ls -l
{
   $s3->EXPECT_head_object( key => "data/key-1" )
      ->RETURN_F( HTTP::Response->new( 200, "OK",
         [
            "Content-Length" => 123,
         ] ), { Mtime => "2013-10-03T23:24:10Z" } );

   $s3->EXPECT_head_object( key => "data/key-2" )
      ->RETURN_F( HTTP::Response->new( 200, "OK",
         [
            "Content-Length" => 135,
         ] ), { Mtime => "2013-10-03T23:24:12Z" } );

   $s3->EXPECT_head_object( key => "data/key-3" )
      ->RETURN_F( HTTP::Response->new( 200, "OK",
         [
            "Content-Length" => 147,
         ] ), { Mtime => "2013-10-03T23:24:14Z" } );

   open my $outh, ">", \(my $output = "");

   $sfs3->cmd_ls( "", long => 1, stdout => $outh );

   $output =~ s/ +$//mg;
   is( $output, <<'EOF',
prefix-1                               DIR
prefix-2                               DIR
key-1                                              123 2013-10-04 00:24:10
key-2                                              135 2013-10-04 00:24:12
key-3                                              147 2013-10-04 00:24:14
EOF
   'output from cmd_ls long no-recurse' );

   no_more_expectations_ok;
}

# ls -r
{
   $s3->EXPECT_list_bucket(
      delimiter => "",
      prefix => "data/",
   )->RETURN_F(
      [
         { key => "data/key-1" },
         { key => "data/key-2" },
         { key => "data/key-3" },
         { key => "data/prefix-1/subkey-A" },
         { key => "data/prefix-2/subkey-B" },
      ],
      []
   );

   open my $outh, ">", \(my $output = "");

   $sfs3->cmd_ls( "", recurse => 1, stdout => $outh );

   $output =~ s/ +$//mg;
   is( $output, <<'EOF',
key-1
key-2
key-3
prefix-1/subkey-A
prefix-2/subkey-B
EOF
   'output from cmd_ls short recurse' );

   no_more_expectations_ok;
}

# ls prefix-1
{
   $s3->EXPECT_list_bucket(
      delimiter => "/",
      prefix => "data/prefix-1/",
   )->RETURN_F(
      [
         { key => "data/prefix-1/subkey-A" },
      ],
      []
   );

   open my $outh, ">", \(my $output = "");

   $sfs3->cmd_ls( "prefix-1/", stdout => $outh );

   $output =~ s/ +$//mg;
   is( $output, <<'EOF',
prefix-1/subkey-A
EOF
   'output from cmd_ls in subdir' );

   no_more_expectations_ok;
}

done_testing;
