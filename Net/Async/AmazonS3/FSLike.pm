package Net::Async::AmazonS3::FSLike;

use strict;
use warnings;
use base qw( Net::Async::AmazonS3 );

sub list_dir
{
   my $self = shift;
   my %args = @_;

   my $path = delete $args{path} // "";
   $path =~ s{/$}{};

   $path .= "/" if length $path;

   $self->list_bucket(
      %args,
      delimiter => "/",
      prefix    => $path,
   );
}

sub get_file
{
   my $self = shift;
   my %args = @_;

   $self->get_object(
      key => delete $args{path},
      %args,
   );
}

0x55AA;
