package Net::Async::AmazonS3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

use Carp;

use XML::LibXML;
use XML::LibXML::XPathContext;
use Digest::MD5;
use Digest::HMAC_SHA1;
use URI::Escape qw( uri_escape_utf8 );
use HTTP::Date qw( time2str );
use MIME::Base64 qw( encode_base64 );

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

   $self->SUPER::configure( %args );
}

sub _make_request
{
   my $self = shift;
   my %args = @_;

   my $method = $args{method};

   my @params;
   foreach my $key ( keys %{ $args{query_params} } ) {
      next unless defined( my $value = $args{query_params}->{$key} );
      $key =~ s/_/-/g;
      push @params, $key . "=" . uri_escape_utf8( $value, "^A-Za-z0-9_-" );
   }

   my $bucket = $args{bucket};
   my $path   = $args{path};

   my $uri;
   # TODO: https?
   if( 1 ) { # TODO: sanity-check bucket
      $uri = "http://$bucket.s3.amazonaws.com/$path";
   }
   else {
      $uri = "http://s3.amazonaws.com/$bucket/$path";
   }
   $uri .= "?" . join( "&", @params ) if @params;

   my $s3 = $self->{s3};

   my @headers = (
      Date => time2str( time ),
   );

   my $request = HTTP::Request->new( $method, $uri, \@headers, $args{content} );

   $self->_gen_auth_header( $request, $bucket, $path );

   return $request;
}

sub _gen_auth_header
{
   my $self = shift;
   my ( $request, $bucket, $path ) = @_;

   # See also
   #   http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader

   my $canon_resource = "/$bucket/$path";

   my $buffer = join( "\n",
      $request->method,
      $request->header( "Content-MD5" ) // "",
      $request->header( "Content-Type" ) // "",
      $request->header( "Date" ) // "",
      # No AMZ headers
      $canon_resource );

   my $s3 = $self->{s3};

   my $hmac = Digest::HMAC_SHA1->new( $self->{secret_key} );
   $hmac->add( $buffer );

   my $access_key = $self->{access_key};
   my $authkey = encode_base64( $hmac->digest );
   # Trim the trailing \n
   $authkey =~ s/\n$//;

   $request->header( Authorization => "AWS $access_key:$authkey" );
}

# Turn non-2xx results into errors
sub _do_request
{
   my $self = shift;
   my ( $request, %args ) = @_;

   $self->{http}->do_request( request => $request, %args )->and_then( sub {
      my $f = shift;
      my $resp = $f->get;

      my $code = $resp->code;
      if( $code !~ m/^2/ ) {
         my $message = $resp->message;
         $message =~ s/\r$//; # HTTP::Response leaves the \r on this

         return Future->new->die(
            "$code $message on " . $request->method . " ". $request->uri->path,
            $request,
            $resp,
         );
      }

      return $f;
   });
}

# Convert response into an XML XPathContext tree
sub _do_request_xpc
{
   my $self = shift;
   my ( $request ) = @_;

   $self->_do_request( $request )->then( sub {
      my $resp = shift;

      my $xpc = XML::LibXML::XPathContext->new( $libxml->parse_string( $resp->content ) );
      $xpc->registerNs( s3 => "http://s3.amazonaws.com/doc/2006-03-01/" );

      return Future->new->done( $xpc );
   });
}

sub list_bucket
{
   my $self = shift;
   my %args = @_;

   my $req = $self->_make_request(
      method       => "GET",
      bucket       => $args{bucket},
      path         => "",
      query_params => {
         prefix       => $args{prefix},
         delimiter    => $args{delimiter},
         max_keys     => 100, # TODO
      },
   );

   $self->_do_request_xpc( $req )->then( sub {
      my $xpc = shift;

      my @files;
      foreach my $node ( $xpc->findnodes( ".//s3:Contents" ) ) {
         my $name = $xpc->findvalue( ".//s3:Key", $node );

         push @files, {
            name          => $xpc->findvalue( ".//s3:Key", $node ),
            type          => "F",
            size          => $xpc->findvalue( ".//s3:Size", $node ),
            last_modified => $xpc->findvalue( ".//s3:LastModified", $node ),
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

   my $on_chunk = delete $args{on_chunk};

   my $request = $self->_make_request(
      method => "GET",
      bucket => $args{bucket},
      path   => $args{key},
   );

   my $get_f;
   if( $on_chunk ) {
      $get_f = $self->_do_request( $request,
         on_header => sub {
            my ( $header ) = @_;
            my $code = $header->code;

            return sub {
               return $on_chunk->( $header, @_ ) if @_ and $code == 200;
               return $header; # with no body content
            },
         }
      );
   }
   else {
      $get_f = $self->_do_request( $request );
   }

   return $get_f->then( sub {
      my $resp = shift;
      return Future->new->done( $resp->content );
   } );
}

sub put_object
{
   my $self = shift;
   my %args = @_;

   my $content_length = delete $args{value_length} // length $args{value};
   defined $content_length or croak "Require value_length or value";

   my $gen_value = delete $args{gen_value};
   my $value     = delete $args{value};

   my $request = $self->_make_request(
      method  => "PUT",
      bucket  => $args{bucket},
      path    => $args{key},
      content => "", # Doesn't matter, it'll be ignored
   );

   $request->content_length( $content_length );
   $request->content( "" );

   my $md5ctx = Digest::MD5->new;

   $self->_do_request( $request,
      request_body => sub {
         return undef if !$gen_value and !length $value;
         my $chunk = $gen_value ? $gen_value->() : substr( $value, 0, 64*1024, "" );
         return undef if !defined $chunk;

         $md5ctx->add( $chunk );
         return $chunk;
      },
   )->then( sub {
      my $resp = shift;

      my $etag = $resp->header( "ETag" );
      # Amazon S3 currently documents that the returned ETag header will be
      # the MD5 hash of the content, surrounded in quote marks. We'd better
      # hope this continues to be true... :/
      my ( $got_md5 ) = $etag =~ m/^"([0-9a-f]{32})"$/ or
         return Future->new->die( "Returned ETag ($etag) does not look like an MD5 sum", $resp );

      my $expect_md5 = $md5ctx->hexdigest;

      if( $got_md5 ne $expect_md5 ) {
         return Future->new->die( "Returned MD5 hash ($got_md5) did not match expected ($expect_md5)", $resp );
      }

      return Future->new->done( $got_md5 );
   });
}

0x55AA;
