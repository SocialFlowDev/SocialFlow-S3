package Net::Async::AmazonS3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

use Net::Amazon::S3;
use Net::Amazon::S3::Request::GetObject;
use Net::Amazon::S3::Request::ListBucket;
use XML::LibXML;
use XML::LibXML::XPathContext;

my $libxml = XML::LibXML->new;

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $args->{http} ||= do {
      require Net::Async::HTTP;
      my $http = Net::Async::HTTP->new;
      $self->add_child( $http );
      $http;
   };

   return $self->SUPER::_init( @_ );
}

sub configure
{
   my $self = shift;
   my %args = @_;

   foreach (qw( http access_key secret_key )) {
      defined $args{$_} and $self->{$_} = delete $args{$_};
   }

   $self->{s3} = Net::Amazon::S3->new({
      aws_access_key_id     => $self->{access_key},
      aws_secret_access_key => $self->{secret_key},
   });

   $self->SUPER::configure( %args );
}

sub _do_request_xpc
{
   my $self = shift;
   my ( $request ) = @_;

   $self->{http}->do_request( request => $request )->then( sub {
      my $resp = shift;
      if( $resp->code !~ m/^2/ ) {
         return Future->new->fail( $resp->code, $resp->message ) # todo
      }

      my $xpc = XML::LibXML::XPathContext->new( $libxml->parse_string( $resp->content ) );
      $xpc->registerNs( s3 => "http://s3.amazonaws.com/doc/2006-03-01/" );

      return Future->new->done( $xpc );
   });
}

sub list_bucket
{
   my $self = shift;
   my %args = @_;

   my $req = Net::Amazon::S3::Request::ListBucket->new(
      %args,
      s3       => $self->{s3},
      max_keys => 100, # TODO
   )->http_request;

   $self->_do_request_xpc( $req )->then( sub {
      my $xpc = shift;

      my @files;
      foreach my $node ( $xpc->findnodes( ".//s3:Contents" ) ) {
         my $name = $xpc->findvalue( ".//s3:Key", $node );

         push @files, {
            name => $xpc->findvalue( ".//s3:Key", $node ),
            type => "F",
            size => $xpc->findvalue( ".//s3:Size", $node ),
         };
      }

      my @dirs;
      foreach my $node ( $xpc->findnodes( ".//s3:CommonPrefixes" ) ) {
         my $name = $xpc->findvalue( ".//s3:Prefix", $node );

         push @dirs, {
            name => $name,
            type => "D",
         };
      }

      return Future->new->done( @files, @dirs );
   });
}

sub get_object
{
   my $self = shift;
   my %args = @_;

   my $request = Net::Amazon::S3::Request::GetObject->new({
      %args,
      s3 => $self->{s3},
      method => "GET",
   })->http_request;

   $self->{http}->do_request( request => $request )->then( sub {
      my $resp = shift;
      if( $resp->code !~ m/^2/ ) {
         return Future->new->fail( $resp->code, $resp->message ) # todo
      }

      return Future->new->done( $resp->content );
   } );
}

0x55AA;
