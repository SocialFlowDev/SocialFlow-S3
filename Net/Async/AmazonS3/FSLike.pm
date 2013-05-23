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

   my $path_re = length $path ? qr/^\Q$path\E(?:$|\/)/ : qr/./;

   $self->list_bucket(
      %args,
      marker       => $path,
      while_marker => $path_re,
   );
}

0x55AA;
