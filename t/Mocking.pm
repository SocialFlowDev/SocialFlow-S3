package t::Mocking;

use strict;
use warnings;
use Exporter 'import';

our @EXPORT = qw(
   no_more_expectations_ok
   MATCHES
);

use Carp;

# A micro expectation-based mocking system

my @expectations; # [] = [$package, $method, \%args, $flags, return]
my @unexpected;

use constant FLAG_PERSIST => 0x01;

sub MATCHES
{
   my ( $predicate ) = @_;
   return bless \$predicate, "t::Mocking::Matcher";
}

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
            foreach ( keys %$args ) {
               exists $args{$_} or next EXPECT;

               my $matcher = $args->{$_};
               my $val     = $args{$_};
               if( !defined $matcher ) {
                  !defined $val or next EXPECT;
               }
               elsif( ref $matcher eq "t::Mocking::Matcher" ) {
                  $matcher->matches( $val ) or next EXPECT;
               }
               else {
                  defined $val and $matcher eq $val or next EXPECT
               }
            }

            @expectations = grep { $_ != $e } @expectations unless $e->[3] & FLAG_PERSIST;

            return $e->[4]->( %args );
         }

         push @unexpected, " ->$method(" . join( ", ", map { "$_ => ".to_qq($args{$_}) } sort keys %args ) . ")\n";
         croak "Unexpected ->$method";
      };
   }
}

sub to_qq
{
   my ( $v ) = @_;
   return "undef" if !defined $v;
   return $v if ref $v;
   return $v if $v =~ m/^-?\d+$/;
   return "'$v'";
}

sub no_more_expectations_ok
{
   Test::More::ok( !( grep { !($_->[3] & FLAG_PERSIST) } @expectations ),
      'All expected methods were called' );

   if( @unexpected ) {
      diag( "Unexpected methods: " );
      diag( "  $_" ) for @unexpected;
      undef @unexpected;
   }
}

require IO::Async::Future;
if( !defined &IO::Async::Future::done_later ) {
   # This method is present in IO::Async 0.61 but not earlier. Don't want to
   # pull in a later version just for unit testing, so we'll add it in here
   no strict 'refs';
   *{"IO::Async::Future::done_later"} = sub {
      my $self = shift;
      my @result = @_;

      $self->loop->later( sub { $self->done( @result ) });

      return $self;
   };

   *{"IO::Async::Future::fail_later"} = sub {
      my $self = shift;
      my @result = @_;

      $self->loop->later( sub { $self->fail( @result ) });

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

package t::Mocking::Matcher;

sub matches
{
   my $self = shift;
   my ( $val ) = @_;
   my $predicate = $$self;
   my $t = ref $predicate;

   if( $t eq "Regexp" ) {
      return $val =~ $predicate;
   }
   elsif( $t eq "CODE" ) {
      return $predicate->( $val );
   }
   else {
      die "TODO: unknown predicate type $t\n";
   }
}

1;
