package SocialFlow::S3::GpgAgentStream;
# TODO: This ought to become something like Net::Async::GgpAgent;

use strict;
use warnings;
use base qw( IO::Async::Stream );

my @commands = qw(
   get_passphrase
);

sub configure
{
   my $self = shift;
   my %params = @_;

   foreach ( @commands ) {
      my $key = "on_$_";
      $self->{$key} = delete $params{$key} if exists $params{$key};
   };

   return $self->SUPER::configure( %params );
}

sub on_read
{
   my $self = shift;
   my ( $buffref, $eof ) = @_;

   return 0 unless $$buffref =~ s/^(.*?)\n//;
   my ( $cmd, @args ) = split m/\s+/, $1;

   # @args are URL encoded
   s{(\+)|%([0-9A-F]{2})}{$1 ? " " : chr hex $2}eg for @args;

   # Just ignore gpg's attempts to set some options we don't care about
   if( $cmd eq "OPTION" ) {
      $self->write( "OK\n" );
   }
   elsif( $cmd eq "GET_PASSPHRASE" ) {
      $self->maybe_invoke_event( on_get_passphrase => @args )
         or $self->write( "ERR Cannot on_get_passphrase\n" );
   }
   elsif( $cmd eq "BYE" ) {
      $self->close_when_empty;
   }
   else {
      print STDERR "Command $cmd from gpg on agent socket\n";
   }
}

0x55AA;
