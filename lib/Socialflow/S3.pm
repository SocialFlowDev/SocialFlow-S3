package Socialflow::S3;

use strict;
use warnings;
use feature qw( switch );
use base qw( IO::Async::Notifier );

use Future;
use Future::Utils qw( fmap1 fmap_void );
use IO::Async::Timer::Periodic;
use Net::Async::Webservice::S3 0.05;

use Digest::MD5;
use File::Basename qw( dirname );
use File::Path qw( make_path );
use List::Util qw( max );
use POSIX qw( ceil strftime );
use POSIX::strptime qw( strptime );
use Time::HiRes qw( time );
use Time::Local qw( timegm );

use constant PART_SIZE => 100*1024*1024; # 100 MiB

use constant FILES_AT_ONCE => 4;

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $args->{s3} ||= Net::Async::Webservice::S3->new(
      access_key => delete $args->{access_key},
      secret_key => delete $args->{secret_key},
      list_max_keys => 1000,

      read_size => 256*1024,
   );

   $args->{s3}->{http}->configure( max_connections_per_host => 0 );

   $self->{status_lines} = 0;

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

# Status message printing
sub print_message
{
   my $self = shift;
   my ( $msg ) = @_;

   # Clear an old status message
   print STDERR "\e\x4D\e[K" for 1 .. $self->{status_lines};
   $self->{status_lines} = 0;

   print STDERR "$msg\n";
}

sub print_status
{
   my $self = shift;
   my ( $status ) = @_;

   $status =~ s/\n$//; # only the final one

   $self->print_message( $status );

   $self->{status_lines} = () = split m/\n/, $status, -1;
}

sub _fname_glob_to_re
{
   my ( $glob ) = @_;

   ( my $re = $glob ) =~ s{(\?)       |  (\*\*)   |  (\*)       |  ([^?*]+)    }
                          {$1&&"[^/]" || $2&&".*" || $3&&"[^/]*"|| quotemeta $4}xeg;

   return $re;
}

sub _split_pattern
{
   my $self = shift;
   my ( $pattern, $keep_basename ) = @_;

   my @parts = split m{/}, $pattern, -1;
   my @prefix;
   push @prefix, shift @parts while @parts and $parts[0] !~ m/[?*]/;

   die "TODO: Directory globs not yet suported" if @parts > 1;

   @parts = ( pop @prefix ) if $keep_basename and !@parts and @prefix;

   my $prefix = join "/", @prefix;
   my $glob   = join "/", @prefix, @parts;

   $prefix .= "/" if length $prefix;

   return ( $prefix ) if !@parts;

   my $re = _fname_glob_to_re( $glob );

   if( length $parts[-1] ) {
      $re = qr/^$re$/;
   }
   else {
      $re = qr/^$re/; # We know the RE pattern now ends in a /, unanchored
   }

   return ( $prefix, $re );
}

sub _expand_pattern
{
   my $self = shift;
   my ( $prefix, $re ) = $self->_split_pattern( @_, 0 );

   return ( $prefix ) if !$re;

   my ( $keys ) = $self->{s3}->list_bucket(
      prefix => "data/$prefix",
      delimiter => "/",
   )->get;

   # Strip 'data/' prefix
   substr($_->{key}, 0, 5) = "" for @$keys;

   return map { $_->{key} =~ $re ? $_->{key} : () } @$keys;
}

sub _make_filter_sub
{
   my ( $only, $exclude ) = @_;

   $only    = [ map { my $re = _fname_glob_to_re $_; qr/^$re$/ } @$only ];
   $exclude = [ map { my $re = _fname_glob_to_re $_; qr/^$re$/ } @$exclude ];

   return sub {
      my ( $file ) = @_;

      $file =~ $_ and return 0 for @$exclude;

      @$only or return 1;
      $file =~ $_ and return 1 for @$only;
      return 0;
   };
}

sub _start_progress_one
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

use constant {
   BYTES => 0,
   TIME  => 1,
};

sub _start_progress_bulk
{
   my $self = shift;
   my ( $slots, $total_files, $total_bytes, $completed_files_ref, $completed_bytes_ref ) = @_;

   my $start_time = time;
   my @times;

   # Easiest way to avoid division by zero errors in this code is to add a tiny amount (0.001)
   # to all the byte totals, which doesn't affect the percentage display very much.

   my $timer = IO::Async::Timer::Periodic->new(
      interval => 1,
      on_tick => sub {
         my $done_bytes = $$completed_bytes_ref;
         my $completed_files = $$completed_files_ref;

         my $slotstats = join "\n", map {
            my ( $s3path, $total, $done ) = @$_;

            $done_bytes += $done;
            sprintf "  [%6d of %6d; %2.1f%%] %s", $done, $total, 100 * $done / ($total+0.001), $s3path;
         } @$slots;

         # Maintain a 30-second time queue
         unshift @times, [ $done_bytes, time ];
         pop @times while @times > 30;

         # A reasonable estimtate of data rate is 50% of last second, 30% of last 30 seconds, 20% overall
         my $ratestats;
         if( @times > 2 ) {
            my $remaining_bytes = $total_bytes - $done_bytes;
            my $rate = ( 0.50 * ( $times[0][BYTES] - $times[1][BYTES]  ) / ( $times[0][TIME] - $times[1][TIME] ) ) +
                       ( 0.30 * ( $times[0][BYTES] - $times[-1][BYTES] ) / ( $times[0][TIME] - $times[-1][TIME] ) ) +
                       ( 0.20 * ( $times[0][BYTES] - 0                 ) / ( $times[0][TIME] - $start_time ) );

            my $remaining_secs = $remaining_bytes / $rate;

            $ratestats = sprintf "%d KiB/sec; ETA %d sec (at %s)",
               $rate / 1024, $remaining_secs, strftime( "%H:%M:%S", localtime time + $remaining_secs );
         }

         $self->print_status(
            sprintf( "[%3d of %3d; %2.1f%%] [%6d of %6d; %2.1f%%] %s\n",
               $completed_files, $total_files, 100 * $completed_files / $total_files,
               $done_bytes,      $total_bytes, 100 * $done_bytes / ($total_bytes+0.001),
               $ratestats // " -- " ) .
            $slotstats );
      },
   );
   $self->add_child( $timer );
   $timer->start;

   return $timer;
}

## support FUNCTIONs
{
   my $fmt_iso8601 = "%Y-%m-%dT%H:%M:%SZ";

   sub strftime_iso8601
   {
      return strftime $fmt_iso8601, gmtime $_[0];
   }

   sub strptime_iso8601
   {
      # TODO: This ought to be doable using a regexp and core's POSIX::mktime
      #   Some care needs to be taken with timezone offsets though
      return timegm strptime $_[0], $fmt_iso8601;
   }
}

sub put_meta
{
   my $self = shift;
   my ( $path, $metaname, $value ) = @_;

   $self->{s3}->put_object(
      key => "meta/$path/$metaname",
      value => $value,
   );
}

sub get_meta
{
   my $self = shift;
   my ( $path, $metaname ) = @_;

   $self->{s3}->get_object(
      key => "meta/$path/$metaname",
   );
}

sub test_skip
{
   my $self = shift;
   my ( $skip_logic, $s3path, $localpath ) = @_;

   my $f;

   given( $skip_logic ) {
      when( "all" ) {
         $f = Future->new->done( 0 );
      }
      when( "stat" ) {
         my ( $size, $mtime ) = ( stat $localpath )[7,9];
         defined $size or return Future->new->done( 0 );

         $f = $self->{s3}->head_object(
            key => "data/$s3path"
         )->then( sub {
            my ( $header, $meta ) = @_;

            return Future->new->done( 0 ) unless defined $meta->{Mtime};

            return Future->new->done(
               $header->content_length == $size &&
               strptime_iso8601( $meta->{Mtime} ) == $mtime
            );
         })->or_else( sub {
            my ( $error, $request, $response ) = $_[0]->failure;
            return Future->new->done( 0 ) if $response->code == 404;
            return $_[0];
         });
      }
      default {
         die "Unrecognised 'skip_logic' value $skip_logic";
      }
   }

   return $f;
}

sub put_file
{
   my $self = shift;
   my ( $localpath, $s3path, %args ) = @_;
   my $on_progress = $args{on_progress};

   open my $fh, "<", $localpath or die "Cannot read $localpath - $!";

   my ( $len_total, $mtime ) = ( stat $fh )[7,9];
   my $len_so_far = 0;

   my $md5 = Digest::MD5->new;
   my $md5_pos = 0;

   my $gen_pos = 0;

   my $gen_parts = sub {
      return if $gen_pos >= $len_total;

      my $part_start = $gen_pos;
      my $part_length = $len_total - $part_start;
      $part_length = PART_SIZE if $part_length > PART_SIZE;

      $gen_pos += $part_length;

      my $buffer = "";
      my $gen_value = sub {
         my ( $pos, $len ) = @_;
         my $end = $pos + $len;

         while( $end > length $buffer ) {
            read( $fh, $buffer, $end - length $buffer, length $buffer ) or die "Cannot read() - $!";
         }

         my $overall_end = $part_start + $end;
         $len_so_far = $overall_end if $overall_end > $len_so_far;
         $on_progress->( $len_so_far, $len_total );

         if( $overall_end > $md5_pos ) {
            $md5->add( substr $buffer, $md5_pos - $part_start );
            $md5_pos = $overall_end;
         }

         return substr( $buffer, $pos, $len );
      };

      return $gen_value, $part_length;
   };

   # special-case for zero-byte long files as otherwise we'll generate no
   # parts at all
   $gen_parts = sub {
      return if $gen_pos > 0;

      $gen_pos = 1;
      return "", 0;
   } if $len_total == 0;

   $self->{s3}->put_object(
      key       => "data/$s3path",
      gen_parts => $gen_parts,
      meta      => {
         Mtime     => strftime_iso8601( $mtime ),
      },
   )->then( sub {
      close $fh;
      $self->put_meta( $s3path, "md5sum", $md5->hexdigest . "\n" );
   });
}

sub get_file
{
   my $self = shift;
   my ( $s3path, $localpath, %args ) = @_;
   my $on_progress = $args{on_progress};

   if( $args{mkdir} and ! -d dirname( $localpath ) ) {
      make_path( dirname $localpath );
   }

   open my $fh, ">", $localpath or die "Cannot write $localpath - $!";

   my $len_total;
   my $len_so_far;

   my $md5 = Digest::MD5->new;

   Future->needs_all(
      $self->get_meta( $s3path, "md5sum" )
         ->transform( done => sub { chomp $_[0]; $_[0] } ),
      $self->{s3}->get_object(
         key    => "data/$s3path",
         on_chunk => sub {
            my ( $header, $chunk ) = @_;
            $md5->add( $chunk );

            if( !defined $len_total ) {
               $len_so_far = 0;
               $len_total = $header->content_length;

               $on_progress->( $len_so_far, $len_total );
            }

            $fh->print( $chunk );
            $len_so_far += length $chunk;
            $on_progress->( $len_so_far, $len_total );
         },
      )
   )->then( sub {
      my ( $exp_md5sum, undef, $header, $meta ) = @_;

      my $got_md5sum = $md5->hexdigest;
      if( $exp_md5sum ne $got_md5sum ) {
         die "Expected MD5sum '$exp_md5sum', got '$got_md5sum'\n";
      }

      close $fh;

      if( defined $meta->{Mtime} ) {
         my $mtime = strptime_iso8601( $meta->{Mtime} );
         utime( $mtime, $mtime, $localpath ) or die "Cannot set mtime - $!";
      }

      Future->new->done;
   });
}

sub delete_file
{
   my $self = shift;
   my ( $s3path ) = @_;

   Future->needs_all(
      $self->{s3}->delete_object(
         key    => "data/$s3path",
      ),
      $self->{s3}->list_bucket(
         prefix => "meta/$s3path/",
         delimiter => "/",
      )->then( sub {
         my ( $keys, $prefixes ) = @_;
         my @metanames = map { $_->{key} } @$keys;
         return Future->new->done unless @metanames;

         Future->needs_all(
            map { $self->{s3}->delete_object( key => $_ ) } @metanames
         );
      })
   );
}

sub cmd_ls
{
   my $self = shift;
   my ( $s3pattern, %options ) = @_;
   my $LONG = $options{long};
   my $RECURSE = $options{recurse};

   my ( $prefix, $re ) = $self->_split_pattern( $s3pattern // "", 1 );

   my ( $keys, $prefixes ) = $self->{s3}->list_bucket(
      prefix => "data/$prefix",
      delimiter => ( $RECURSE ? "" : "/" ),
   )->get;

   my @files;
   if( $LONG ) {
      @files = ( fmap1 {
         my $key = $_[0]->{key};
         $self->{s3}->head_object(
            key => $key
         )->then( sub {
            my ( $header, $meta ) = @_;

            return Future->new->done( {
               name => substr( $key, 5 ),
               size => $header->content_length,
               mtime => ( defined $meta->{Mtime} ? strptime_iso8601( $meta->{Mtime} ) : undef ),
            } );
         });
      } foreach => $keys, return => $self->loop->new_future, concurrent => 20 )->get;
   }
   else {
      @files = map { +{ name => substr $_->{key}, 5 } } @$keys;
   }

   while( @files or @$prefixes ) {
      if( !@$prefixes or @files and $files[0]{name} lt $prefixes->[0] ) {
         my $e = shift @files;
         my $name = $e->{name};
         next if $re and $name !~ $re;

         if( $LONG ) {
            # Timestamps in local timezone
            my @mtime = localtime $e->{mtime};
            my $timestamp = defined $e->{mtime} ? strftime( "%Y-%m-%d %H:%M:%S", @mtime ) : "-- unknown --";
            printf "%-38s %15d %s\n", $name, $e->{size}, $timestamp;
         }
         else {
            printf "%-38s\n", $name;
         }
      }
      elsif( !@files or @$prefixes and $prefixes->[0] lt $files[0]{name} ) {
         my $name = substr shift @$prefixes, 5;
         next if $re and substr( $name, 0, -1 ) !~ $re;

         printf "%-38s DIR\n", $name;
      }
   }
}

sub cmd_cat
{
   my $self = shift;
   my ( $s3path ) = @_;

   my $md5 = Digest::MD5->new;

   my $exp_md5sum;
   Future->needs_all(
      $self->get_meta( $s3path, "md5sum" )
         ->on_done( sub { ( $exp_md5sum ) = @_; chomp $exp_md5sum } ),
      $self->{s3}->get_object(
         key    => "data/$s3path",
         on_chunk => sub {
            my ( $header, $chunk ) = @_;
            $md5->add( $chunk );
            print $chunk;
         },
      )
   )->get;

   my $got_md5sum = $md5->hexdigest;
   if( $exp_md5sum ne $got_md5sum ) {
      die "Expected MD5sum '$exp_md5sum', got '$got_md5sum'\n";
   }
}

sub cmd_uncat
{
   my $self = shift;
   my ( $s3path ) = @_;

   my $md5 = Digest::MD5->new;

   $self->add_child( my $stdin = IO::Async::Stream->new_for_stdin( on_read => sub { 0 } ) );

   my $eof;
   my $gen_parts = sub {
      return if $eof;
      return $stdin->read_exactly( PART_SIZE )
         ->on_done( sub {
            ( my $part, $eof ) = @_;
            $md5->add( $part );
         });
   };

   $self->{s3}->put_object(
      key       => "data/$s3path",
      gen_parts => $gen_parts,
      meta      => {
         Mtime     => strftime_iso8601( time ),
      },
   )->then( sub {
      $self->put_meta( $s3path, "md5sum", $md5->hexdigest . "\n" );
   })->get;
}

sub cmd_get
{
   my $self = shift;
   my ( $s3path, $localpath ) = @_;

   my $len_so_far;
   my $progress_timer;

   $self->get_file(
      $s3path, $localpath,
      on_progress => sub {
         $len_so_far = $_[0];
         $progress_timer ||= $self->_start_progress_one( $_[1], \$len_so_far );
      },
   )->get;

   print "Successfully got $s3path to $localpath\n";

   $self->remove_child( $progress_timer );
}

sub cmd_put
{
   my $self = shift;
   my ( $localpath, $s3path ) = @_;

   my $len_so_far;
   my $progress_timer;

   $self->put_file(
      $localpath, $s3path,
      on_progress => sub {
         $len_so_far = $_[0];
         $progress_timer ||= $self->_start_progress_one( $_[1], \$len_so_far );
      },
   )->get;

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
      $self->delete_file( $s3path )->get;
      print "Removed $s3path\n";
   }
}

sub cmd_push
{
   my $self = shift;
   my ( $localroot, $s3root, %args ) = @_;

   my $concurrent = $args{concurrent} || FILES_AT_ONCE;
   my $skip_logic = $args{skip_logic} || "stat";
   my $filter = _make_filter_sub( $args{only}, $args{exclude} );

   # Determine the list of files first by entirely synchronous operations
   my $total_bytes = 0;
   my @files;

   # BFS by stack
   my @stack = ( undef );
   while( @stack ) {
      my $relpath = shift @stack;

      my $localpath = join "/", grep { defined } $localroot, $relpath;

      $self->print_message( "Scanning $localpath..." );
      opendir my $dirh, $localpath or die "Cannot opendir $localpath - $!\n";

      my @moredirs;
      foreach ( sort readdir $dirh ) {
         next if $_ eq "." or $_ eq "..";

         my $ent = join "/", grep { defined } $relpath, $_;

         stat "$localroot/$ent" or next;

         if( -d _ ) {
            push @moredirs, $ent;
         }
         elsif( -f _ ) {
            next unless $filter->( $ent );
            my $bytes = -s _;
            push @files, [ $ent, $bytes ];
            $total_bytes += $bytes;
         }
      }

      unshift @stack, @moredirs;
   }

   my $total_files = scalar @files;

   $self->print_message( sprintf "Found %d files totalling %d bytes (%.1f MiB)",
      $total_files, $total_bytes, $total_bytes / (1024*1024) );

   my $completed_files = 0;
   my $completed_bytes = 0;

   my @uploads;
   my $timer = $self->_start_progress_bulk( \@uploads, $total_files, $total_bytes, \$completed_files, \$completed_bytes );

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      my $localpath = "$localroot/$relpath";
      # Allow $s3root="" to mean upload into root
      my $s3path    = join "/", grep { length } $s3root, $relpath;

      $self->test_skip( $skip_logic, $s3path, $localpath )->then( sub {
         my ( $skip ) = @_;
         if( $skip ) {
            $self->print_message( "SKIP  $localpath => $s3path" );
            return Future->new->done;
         }

         $self->print_message( "START $localpath => $s3path" );
         push @uploads, my $slot = [ $s3path, $size, 0 ];
         $timer->invoke_event( on_tick => );

         return $self->put_file(
            $localpath, $s3path,
            on_progress => sub { ( $slot->[2] ) = @_ },
         )->on_done( sub {
            $self->print_message( "DONE  $localpath => $s3path" );
            $completed_files += 1;
            $completed_bytes += $size;

            @uploads = grep { $_ != $slot } @uploads;
            $timer->invoke_event( on_tick => );
         });
      });
   } foreach => \@files,
     return => $self->loop->new_future,
     concurrent => $concurrent )->get;

   $self->print_message( "All files done" );
   $self->remove_child( $timer );
}

sub cmd_pull
{
   my $self = shift;
   my ( $s3root, $localroot, %args ) = @_;

   my $concurrent = $args{concurrent} || FILES_AT_ONCE;
   my $skip_logic = $args{skip_logic} || "stat";
   my $filter = _make_filter_sub( $args{only}, $args{exclude} );

   $self->print_message( "Listing files on S3..." );
   my ( $keys ) = $self->{s3}->list_bucket(
      prefix => "data/$s3root",
      # no delimiter
   )->get;

   my $total_bytes = 0;
   my $total_files = 0;
   my @files;

   foreach ( @$keys ) {
      # Trim "data/" prefix
      next unless $filter->( my $name = substr( $_->{key}, 5 ) );

      $total_bytes += $_->{size};
      $total_files += 1;
      push @files, [ $name, $_->{size} ];
   }

   $self->print_message( sprintf "Found %d files totalling %d bytes (%.1f MiB)",
      $total_files, $total_bytes, $total_bytes / (1024*1024) );

   my $completed_files = 0;
   my $completed_bytes = 0;

   my @downloads;
   my $timer = $self->_start_progress_bulk( \@downloads, $total_files, $total_bytes, \$completed_files, \$completed_bytes );

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      # Allow $s3root="" to mean download from root
      my $s3path    = join "/", grep { length } $s3root, $relpath;
      my $localpath = "$localroot/$relpath";

      $self->test_skip( $skip_logic, $s3path, $localpath )->then( sub {
         my ( $skip ) = @_;
         if( $skip ) {
            $self->print_message( "SKIP  $localpath <= $s3path" );
            return Future->new->done;
         }

         $self->print_message( "START $localpath <= $s3path" );
         push @downloads, my $slot = [ $s3path, $size, 0 ];
         $timer->invoke_event( on_tick => );

         return $self->get_file(
            $s3path, $localpath,
            on_progress => sub { ( $slot->[2] ) = @_ },
            mkdir => 1,
         )->on_done( sub {
            $self->print_message( "DONE  $localpath <= $s3path" );
            $completed_files += 1;
            $completed_bytes += $size;

            @downloads = grep { $_ != $slot } @downloads;
            $timer->invoke_event( on_tick => );
         });
      });
   } foreach => \@files,
     return => $self->loop->new_future,
     concurrent => $concurrent )->get;

   $self->print_message( "All files done" );
   $self->remove_child( $timer );
}

1;
