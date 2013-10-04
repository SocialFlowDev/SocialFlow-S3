package t::Mocking;

use strict;
use warnings;
use Exporter 'import';

our @EXPORT = qw(
   no_more_expectations_ok
);

# A micro expectation-based mocking system

my @expectations; # [] = [$package, $method, \%args, return]

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
      *{"${pkg}::$EXPECT_method"} = sub {
         shift;
         my %args = @_;
         push @expectations, my $e =
            bless [ $pkg, $method => \%args, undef ], "t::Mocking::Expectation";
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

            @expectations = grep { $_ != $e } @expectations;

            delete @args{keys %$args};

            return $e->[3]->( %args );
         }

         die "Unexpected ->$method(" . join( ", ", map { "$_ => '$args{$_}'" } sort keys %args ) . ")";
      };
   }
}

sub no_more_expectations_ok
{
   Test::More::ok( !@expectations, 'All expected methods were called' );
}

package t::Mocking::Expectation;

sub RETURN_WITH
{
   my $e = shift;
   ( $e->[3] ) = @_;
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

1;
