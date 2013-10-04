package t::MockS3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

use t::Mocking;
t::Mocking->mock_methods(qw(
   list_bucket head_object get_object put_object delete_object
));

sub _init
{
   my $self = shift;
   $self->{http} = bless {}, "t::MockS3::Http";
   return $self->SUPER::_init( @_ );
}

sub configure
{
   my $self = shift;
   my %args = @_;
   delete $args{$_} for qw( timeout stall_timeout bucket prefix );
   $self->SUPER::configure( %args );
}

package t::MockS3::Http;
sub configure {}

1;
