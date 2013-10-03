package t::MockS3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

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

my @expectations; # [] = [$method, \%args, return]

foreach my $method (qw( list_bucket head_object )) {
   my $EXPECT_method = "EXPECT_$method";

   no strict 'refs';
   *$EXPECT_method = sub {
      shift;
      my %args = @_;
      push @expectations, my $e =
         bless [ $method => \%args, undef ], "t::MockS3::Expectation";
      return $e;
   };

   *$method = sub {
      my $self = shift;
      my %args = @_;

      EXPECT: foreach my $e ( @expectations ) {
         $e->[0] eq $method or next EXPECT;
         my $args = $e->[1];
         $args->{$_} eq $args{$_} or next EXPECT for keys %$args;

         return $e->[2]->();
      }

      die "Unexpected ->$method(" . join( ", ", map { "$_ => '$args{$_}'" } sort keys %args ) . ")";
   };
}

package t::MockS3::Http;
sub configure {}

package t::MockS3::Expectation;
sub RETURN
{
   my $e = shift;
   my @ret = @_;
   $e->[2] = sub { return wantarray ? @ret : $ret[0]; };
}

sub RETURN_F
{
   my $e = shift;
   $e->RETURN( Future->new->done( @_ ) );
}

1;
