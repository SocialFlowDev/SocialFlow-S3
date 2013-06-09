package Socialflow::S3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

use IO::Async::Timer::Periodic;
use Net::Async::Webservice::S3 0.03;

use Fcntl qw( SEEK_SET );
use List::Util qw( max );
use POSIX qw( ceil );
use Time::HiRes qw( time );

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $args->{s3} ||= Net::Async::Webservice::S3->new(
      access_key => delete $args->{access_key},
      secret_key => delete $args->{secret_key},
      list_max_keys => 1000,
   );

   $self->SUPER::_init( $args );
}

sub configure
{
   my $self = shift;
   my %args = @_;

   if( my $s3 = delete $args{s3} ) {
      $self->remove_child( delete $self->{s3} ) if $self->{s3};
      $self->add_child( $self->{s3} = $s3 );
   }

   if( my $bucket = delete $args{bucket} ) {
      ( $bucket, my $prefix ) = split m{/}, $bucket, 2;
      $self->{s3}->configure(
         bucket => $bucket,
         prefix => $prefix,
      );
   }

   $self->SUPER::configure( %args );
}

sub _split_pattern
{
   my $self = shift;
   my ( $pattern, $keep_basename ) = @_;

   my @parts = split m{/}, $pattern;
   my @prefix;
   push @prefix, shift @parts while @parts and $parts[0] !~ m/[?*]/;

   die "TODO: Directory globs not yet suported" if @parts > 1;

   @parts = ( pop @prefix ) if $keep_basename and !@parts and @prefix;

   my $prefix = join "/", @prefix;
   my $glob   = join "/", @parts;

   return ( $prefix ) if !@parts;

   ( my $re = $glob ) =~ s{(\?)    |  (\*)     |  ([^?*]+)    }
                          {$1&&"." || $2&&".*" || quotemeta $3}xeg;

   return ( $prefix, qr/^$re$/ );
}

sub _expand_pattern
{
   my $self = shift;
   my ( $prefix, $re ) = $self->_split_pattern( @_, 0 );

   return ( $prefix ) if !$re;

   my ( $keys ) = $self->{s3}->list_bucket(
      prefix => $prefix,
      delimiter => "/",
   )->get;

   return map { $_->{key} =~ $re ? $_->{key} : () } @$keys;
}

sub _start_progress
{
   my $self = shift;
   my ( $len_total, $len_so_far_ref ) = @_;

   my $start_time = time;

   my $timer = IO::Async::Timer::Periodic->new(
      on_tick => sub {
         my $now = time;
         my $rate = $$len_so_far_ref / ( $now - $start_time );
         my $eta = ( $len_total - $$len_so_far_ref ) / $rate;

         printf "Done %.2f%% (%d of %d) %.2f KB/sec; ETA %d sec\n",
            100 * $$len_so_far_ref / $len_total, $$len_so_far_ref, $len_total,
            $rate / 1000, ceil( $eta );
      },
      interval => 1,
   );
   $self->add_child( $timer->start );
   return $timer;
}

sub cmd_ls
{
   my $self = shift;
   my ( $s3pattern, %options ) = @_;
   my $LONG = $options{long};

   my ( $prefix, $re ) = $self->_split_pattern( $s3pattern // "", 1 );

   my ( $keys, $prefixes ) = $self->{s3}->list_bucket(
      prefix => $prefix,
      delimiter => "/",
   )->get;

   while( @$keys or @$prefixes ) {
      if( !@$prefixes or @$keys and $keys->[0]{key} lt $prefixes->[0] ) {
         my $e = shift @$keys;
         next if $re and $e->{key} !~ $re;

         if( $LONG ) {
            printf "%-38s %15d %s\n", $e->{key}, $e->{size}, $e->{last_modified};
         }
         else {
            printf "%-38s\n", $e->{key};
         }
      }
      elsif( !@$keys or @$prefixes and $prefixes->[0] lt $keys->[0]{key} ) {
         my $name = shift @$prefixes;
         next if $re and $name !~ $re;

         printf "%-38s DIR\n", $name;
      }
   }
}

sub cmd_cat
{
   my $self = shift;
   my ( $s3path ) = @_;

   $self->{s3}->get_object(
      key    => $s3path,
      on_chunk => sub {
         my ( $header, $chunk ) = @_;
         print $chunk;
      },
   )->get;
}

sub cmd_get
{
   my $self = shift;
   my ( $s3path, $localpath ) = @_;

   my $fh;
   my $len_so_far;
   my $progress_timer;

   $self->{s3}->get_object(
      key    => $s3path,
      on_chunk => sub {
         my ( $header, $chunk ) = @_;

         if( !$fh ) {
            open $fh, ">", $localpath or die "Cannot write $localpath - $!";
            $len_so_far = 0;
            my $len_total = $header->content_length;

            $progress_timer = $self->_start_progress( $len_total, \$len_so_far );
         }

         $fh->print( $chunk );
         $len_so_far += length $chunk;
      },
   )->get;

   print "Successfully got $s3path to $localpath\n";

   $self->remove_child( $progress_timer );
}

sub cmd_put
{
   my $self = shift;
   my ( $localpath, $s3path ) = @_;

   open my $fh, "<", $localpath or die "Cannot read $localpath - $!";
   my $len_total = -s $fh;
   my $len_so_far = 0;

   my $progress_timer = $self->_start_progress( $len_total, \$len_so_far );
   my $result = $self->{s3}->put_object(
      key    => $s3path,
      value_length => $len_total,
      gen_value => sub {
         my ( $pos, $len ) = @_;
         return undef if eof $fh;

         seek( $fh, SEEK_SET, $pos );
         $len = read $fh, my $chunk, $len or die "Cannot read() - $!";
         $len_so_far = max( $len_so_far, $pos + $len );

         return $chunk;
      },
   )->get;

   close $fh;

   print "Successfully put $localpath to $s3path\n";

   $self->remove_child( $progress_timer );
}

sub cmd_rm
{
   my $self = shift;
   my ( $s3pattern ) = @_;

   my @s3paths = $self->_expand_pattern( $s3pattern );
   if( !@s3paths ) {
      print STDERR "Nothing matched $s3pattern\n";
      exit 1;
   }

   # TODO: Future concurrently
   foreach my $s3path ( @s3paths ) {
      $self->{s3}->delete_object(
         key    => $s3path,
      )->get;
      print "Removed $s3path\n";
   }
}

1;
