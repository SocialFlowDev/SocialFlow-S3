package t::Mocking;

use strict;
use warnings;
use Exporter 'import';

our @EXPORT = qw(
   no_more_expectations_ok
);

# A micro expectation-based mocking system

my @expectations; # [] = [$package, $method, \%args, $flags, return]

use constant FLAG_PERSIST => 0x01;

sub mock_methods
{
   my $class = shift;
   my $pkg = caller;
   $class->mock_methods_into( $pkg, @_ );
}

sub mock_methods_into
{
   my $class = shift;
   my ( $pkg, @methods ) = @_;

   foreach my $method ( @methods ) {
      my $EXPECT_method = "EXPECT_$method";

      no strict 'refs';
      no warnings 'redefine';

      *{"${pkg}::$EXPECT_method"} = sub {
         shift;
         my %args = @_;
         push @expectations, my $e =
            bless [ $pkg, $method => \%args, 0, undef ], "t::Mocking::Expectation";
         return $e;
      };

      *{"${pkg}::$method"} = sub {
         my $self = shift;
         my %args = @_;

         EXPECT: foreach my $e ( @expectations ) {
            $e->[0] eq $pkg or next EXPECT;
            $e->[1] eq $method or next EXPECT;
            my $args = $e->[2];
            $args->{$_} eq $args{$_} or next EXPECT for keys %$args;

            @expectations = grep { $_ != $e } @expectations unless $e->[3] & FLAG_PERSIST;

            delete @args{keys %$args};

            return $e->[4]->( %args );
         }

         die "Unexpected ->$method(" . join( ", ", map { "$_ => '$args{$_}'" } sort keys %args ) . ")";
      };
   }
}

sub no_more_expectations_ok
{
   Test::More::ok( !( grep { !($_->[3] & FLAG_PERSIST) } @expectations ),
      'All expected methods were called' );
}

# TODO: This method is present in latest IO::Async source (0.61+bzr1246) but
# that's not on CPAN.
if( !defined &IO::Async::Future::done_later ) {
   no strict 'refs';
   *{"IO::Async::Future::done_later"} = sub {
      my $self = shift;
      my @result = @_;

      $self->loop->later( sub { $self->done( @result ) });

      return $self;
   };
}

package t::Mocking::Expectation;

sub RETURN_WITH
{
   my $e = shift;
   ( $e->[4] ) = @_;
   return $e;
}

sub RETURN
{
   my $e = shift;
   my @ret = @_;
   $e->RETURN_WITH( sub { return wantarray ? @ret : $ret[0]; } );
}

sub RETURN_F
{
   my $e = shift;
   $e->RETURN( Future->new->done( @_ ) );
}

sub PERSIST
{
   my $e = shift;
   $e->[3] |= t::Mocking::FLAG_PERSIST;
   return $e;
}

1;
