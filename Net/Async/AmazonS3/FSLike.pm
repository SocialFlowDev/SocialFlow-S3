package Net::Async::AmazonS3::FSLike;

use strict;
use warnings;
use base qw( Net::Async::AmazonS3 );

sub list_dir
{
   my $self = shift;
   my %args = @_;

   my $path = $args{path} // "";
   $path =~ s{/$}{};

   $path .= "/" if length $path;

   $self->list_bucket(
      %args,
      prefix => $path,
   );
}

0x55AA;
