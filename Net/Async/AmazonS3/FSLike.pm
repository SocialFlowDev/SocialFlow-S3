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

   my $path = delete $args{path};
   $path =~ s{//+}{/};

   $self->get_object(
      key => $path,
      %args,
   );
}

sub put_file
{
   my $self = shift;
   my %args = @_;

   my $path = delete $args{path};
   $path =~ s{//+}{/};

   $self->put_object(
      key => $path,
      value => delete $args{content},
      gen_value => delete $args{gen_content},
      value_length => delete $args{content_length},
      %args,
   );
}

0x55AA;
