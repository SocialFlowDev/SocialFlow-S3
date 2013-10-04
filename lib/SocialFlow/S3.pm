package SocialFlow::S3;

use strict;
use warnings;
use feature qw( switch );
use base qw( IO::Async::Notifier );
no if $] >= 5.017011, warnings => 'experimental::smartmatch';

use Future;
use Future::Utils qw( fmap1 fmap_void );
use IO::Async::Process;
use IO::Async::Stream;
use IO::Async::Timer::Periodic;
use Net::Async::Webservice::S3 0.13; # no-parts bugfix

use Digest::MD5;
use File::Basename qw( dirname );
use File::Path qw( make_path );
use List::Util qw( max );
use POSIX qw( ceil strftime );
use POSIX::strptime qw( strptime );
use Scalar::Util qw( blessed );
use Time::HiRes qw( time );
use Time::Local qw( timegm );

use constant PART_SIZE => 100*1024*1024; # 100 MiB

use constant FILES_AT_ONCE => 4;

our $VERSION = "0.03";

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $args->{s3} ||= Net::Async::Webservice::S3->new(
      access_key => delete $args->{access_key},
      secret_key => delete $args->{secret_key},
      ssl        => delete $args->{ssl},
      list_max_keys => 1000,

      read_size => 256*1024,
   );

   $args->{s3}->{http}->configure( max_connections_per_host => 0 );

   $args->{timeout}       //= 10;
   $args->{stall_timeout} //= 30;

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

   foreach (qw( quiet )) {
      $self->{$_} = delete $args{$_} if exists $args{$_};
   }

   foreach (qw( timeout stall_timeout )) {
      next unless exists $args{$_};

      $self->{s3}->configure( $_ => $args{$_} );
      $self->{$_} = delete $args{$_};
   }

   if( my $bucket = delete $args{bucket} ) {
      ( $bucket, my $prefix ) = split m{/}, $bucket, 2;
      $self->{s3}->configure(
         bucket => $bucket,
         prefix => $prefix,
      );
   }

   if( my $keyid = delete $args{crypto_keyid} ) {
      $self->{crypto_keyid} = $keyid;
   }

   $self->SUPER::configure( %args );
}

# Status message printing
sub print_message
{
   my $self = shift;
   my ( $msg ) = @_;

   return if $self->{quiet};

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

   return ( $prefix ) if !@parts;

   $prefix .= "/" if length $prefix;

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

         my $status;
         if( defined $len_total and $len_total > 0 ) {
            $status = sprintf "Done %.2f%% (%d of %d) ",
               100 * $$len_so_far_ref / $len_total, $$len_so_far_ref, $len_total;
         }
         else {
            $status = sprintf "Done %d ", $$len_so_far_ref;
         }

         if( $rate > 0 ) {
            $status .= sprintf "%.2f KB/sec ", $rate / 1000;

            if( defined $len_total ) {
               my $eta = ( $len_total - $$len_so_far_ref ) / $rate;
               $status .= sprintf "ETA %d sec", ceil( $eta );
            }
         }
         else {
            $status .= "--stalled--";
         }

         $self->print_status( $status );
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
   my ( $slots, $total_files, $total_bytes, $completed_files_ref, $completed_bytes_ref, $skipped_bytes_ref ) = @_;

   my $start_time = time;
   my @times;

   # Easiest way to avoid division by zero errors in this code is to add a tiny amount (0.001)
   # to all the byte totals, which doesn't affect the percentage display very much.

   my $timer = IO::Async::Timer::Periodic->new(
      interval => 1,
      on_tick => sub {
         my $done_bytes = $$completed_bytes_ref;
         my $completed_files = $$completed_files_ref;

         my $slotstats = join "", map {
            my ( $s3path, $total, $done ) = @$_;

            if( $done eq "test" ) {
               sprintf "  [-- testing %d --] %s\n", $total, $s3path
            }
            else {
               $done_bytes += $done;
               sprintf "  [%6d of %6d; %2.1f%%] %s\n", $done, $total, 100 * $done / ($total+0.001), $s3path;
            }
         } @$slots;

         # Maintain a 30-second time queue of bytes actually transferred (i.e. not skipped)
         unshift @times, [ $done_bytes - $$skipped_bytes_ref, time ];
         pop @times while @times > 30;

         # A reasonable estimtate of data rate is 50% of last second, 30% of last 30 seconds, 20% overall
         my $ratestats;
         if( @times > 2 ) {
            my $remaining_bytes = $total_bytes - $done_bytes;
            my $rate = ( 0.50 * ( $times[0][BYTES] - $times[1][BYTES]  ) / ( $times[0][TIME] - $times[1][TIME] ) ) +
                       ( 0.30 * ( $times[0][BYTES] - $times[-1][BYTES] ) / ( $times[0][TIME] - $times[-1][TIME] ) ) +
                       ( 0.20 * ( $times[0][BYTES] - 0                 ) / ( $times[0][TIME] - $start_time ) );

            if( $rate > 0 ) {
               my $remaining_secs = $remaining_bytes / $rate;
               $ratestats = sprintf "%d KiB/sec; ETA %d sec (at %s)",
                  $rate / 1024, $remaining_secs, strftime( "%H:%M:%S", localtime time + $remaining_secs );
            }
            else {
               $ratestats = "0 KiB/sec; ETA ---";
            }
         }

         $self->print_status( $slotstats .
            sprintf( "[%3d of %3d; %2.1f%%] [%6d of %6d; %2.1f%%] %s",
               $completed_files, $total_files, 100 * $completed_files / $total_files,
               $done_bytes,      $total_bytes, 100 * $done_bytes / ($total_bytes+0.001),
               $ratestats // " -- " ) );
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
      timeout => $self->{timeout},
   );
}

sub get_meta
{
   my $self = shift;
   my ( $path, $metaname ) = @_;

   $self->{s3}->get_object(
      key => "meta/$path/$metaname",
      timeout => $self->{timeout},
   );
}

# Thre's no harm in DELETEing an object that does not exist, but if we test first then we
# can avoid giving DELETE permission unnecessarily
sub delete_meta
{
   my $self = shift;
   my ( $path, $metaname ) = @_;

   $self->{s3}->get_object(
      key => "meta/$path/$metaname",
      timeout => $self->{timeout},
   )->followed_by( sub {
      my $f = shift;

      if( $f->failure ) {
         my ( $failure, $request, $response ) = $f->failure;
         return Future->new->done if $response and $response->code == 404;
         return $f;
      }

      $self->{s3}->delete_object(
         key => "meta/$path/$metaname",
      )
   });
}

sub test_skip
{
   my $self = shift;
   my ( $skip_logic, $s3path, $localpath ) = @_;

   my $f;

   # 'given' creates a lexical $_ which upsets the split() in the md5sum case; 'for' does not
   for( $skip_logic ) {
      when( "all" ) {
         $f = Future->new->done( 0 );
      }
      when( "stat" ) {
         my ( $size, $mtime ) = ( stat $localpath )[7,9];
         defined $size or return Future->new->done( 0 );

         # Fetch the md5sum meta anyway even if we aren't going to use it, because if
         # it's missing we definitely want to re-upload
         $f = Future->needs_all(
            $self->{s3}->head_object( key => "data/$s3path" ),
            $self->get_meta( $s3path, "md5sum" )->transform( done => sub { chomp $_[0]; $_[0] } ),
         )->then( sub {
            my ( $header, $meta, $s3md5 ) = @_;

            return Future->new->done( 0 ) unless defined $meta->{Mtime};

            return Future->new->done(
               $header->content_length == $size && strptime_iso8601( $meta->{Mtime} ) == $mtime,
               $s3md5,
            );
         })->or_else( sub {
            my ( $error, $request, $response ) = $_[0]->failure;
            return Future->new->done( 0 ) if $response && $response->code == 404;
            return $_[0];
         });
      }
      when( "md5sum" ) {
         $f = $self->test_skip( "stat", $s3path, $localpath )->then( sub {
            my ( $skip, $s3md5 ) = @_;
            return Future->new->done( 0 ) if !$skip;

            # TODO: IO::Async probably wants a Future-returning process running method
            my $localmd5_f = $self->loop->new_future;
            $self->loop->run_child(
               command => [ "md5sum", $localpath ],
               on_finish => sub {
                  my ( $pid, $exitcode, $stdout, $stderr ) = @_;
                  if( $exitcode == 0 ) {
                     my ( $md5sum ) = split m/\s+/, $stdout;
                     $localmd5_f->done( $md5sum );
                  }
                  else {
                     $localmd5_f->fail( "Unable to run md5sum ($exitcode) - $stderr" );
                  }
               },
            );

            $localmd5_f->then( sub {
               my ( $localmd5 ) = @_;
               return Future->new->done( $localmd5 eq $s3md5 );
            })->or_else( sub {
               my $f = shift;
               my ( $failure, $request, $response ) = $f->failure;
               # Missind md5sum == don't skip; anything else == error
               return Future->new->done( 0 ) if $response && $response->code == 404;
               return $_[0];
            });
         });
      }
      default {
         die "Unrecognised 'skip_logic' value $skip_logic";
      }
   }

   return $f;
}

sub stat_file
{
   my $self = shift;
   my ( $s3path ) = @_;

   $self->{s3}->head_object(
      key => "data/$s3path",
   )->or_else( sub {
      my $f = shift;
      return Future->new->done( undef) if ( $f->failure )[2]->code == 404;
      return $f; # propagate other errors
   });
}

sub _put_file_from_fh
{
   my $self = shift;
   my ( $fh, $s3path, %args ) = @_;

   my $on_progress = $args{on_progress};

   my %meta = (
      Mtime => strftime_iso8601( delete $args{mtime} ),
   );

   my $md5 = Digest::MD5->new;
   my $more_func;
   my $fh_stream;

   if( my $keyid = $self->{crypto_keyid} ) {
      $meta{Keyid} = $keyid;

      # pipe the data through 'gpg --encrypt --recipient $keyid' -
      my $gpg_process = IO::Async::Process->new(
         command => [ "gpg", "--encrypt", "--recipient", $keyid, "--no-tty", "-" ],
         stdin  => { via => "pipe_write" },
         stdout => { via => "pipe_read" },
         on_finish => sub {
            my ( undef, $exitcode ) = @_;
            $exitcode == 0 and return;

            die "gpg exited non-zero $exitcode\n";
         },
      );
      $gpg_process->stdout->configure( on_read => sub { 0 } );
      $self->add_child( $gpg_process );

      # But we need to be reading it ourselves anyway, as the main meta/PATH/md5sum
      # checksum has to store the plaintext sum
      my $fh_in = IO::Async::Stream->new(
         read_handle => $fh,
         on_read => sub {}, # reading only by Futures
         close_on_read_eof => 0, # we'll close it ourselves - TODO: IO::Async might want to defer this one
         read_high_watermark => 10 * 1024*1024, # 10 MiB
         read_low_watermark  =>  5 * 1024*1024, #  5 MiB
      );
      $self->add_child( $fh_in );
      undef $fh;

      my $eof;
      $gpg_process->stdin->write( sub {
         return undef if $eof;
         return $fh_in->read_atmost( 64 * 1024 ) # 64 KiB
            ->and_then( sub {
               my $f = shift;
               ( my $content, $eof ) = $f->get;

               $md5->add( $content );

               $f;
            })
      })->on_done( sub {
         $gpg_process->stdin->close;
      });

      $fh_stream = $gpg_process->stdout;
      $fh = $fh_stream->read_handle;
      $more_func = sub { $_[0] };
   }
   else {
      $fh_stream = IO::Async::Stream->new(
         read_handle => $fh,
         on_read => sub { 0 },
      );
      $self->add_child( $fh_stream );

      $more_func = sub {
         $md5->add( $_[0] );
         $_[0];
      };
   }

   stat( $fh ) or die "Cannot stat FH - $!";

   my $gen_parts;
   if( -f _ ) {
      # Ignore the fh_stream here

      my $len_total = -s _;
      my $read_pos = 0;

      $gen_parts = sub {
         return if $read_pos >= $len_total;

         my $part_start = $read_pos;
         my $part_length = $len_total - $part_start;
         $part_length = PART_SIZE if $part_length > PART_SIZE;

         $read_pos += $part_length;

         my $gen_value = sub {
            my ( $pos, $len ) = @_;
            my $ret = sysread( $fh, my $buffer, $len );
            defined $ret or die "Cannot read - $!";

            return $buffer;
         };

         return $gen_value, $part_length;
      };
   }
   elsif( -p _ or -S _ ) {
      # pipe or socket
      # this case is used for all GPG-driven input
      my $eof;
      $gen_parts = sub {
         return if $eof;
         my $f = $fh_stream->read_exactly( PART_SIZE )
            ->on_done( sub {
               ( my $part, $eof ) = @_;
            });
         return ( $f, PART_SIZE );
      };
   }
   else {
      die "Cannot put from $fh - must be a regular file, pipe, or socket\n";
   }

   my @more_futures;

   my $part_offset = 0;
   my $f = $self->{s3}->put_object(
      key       => "data/$s3path",
      meta      => \%meta,
      gen_parts => sub {
         my ( $part, $part_len ) = $gen_parts->() or return;
         my $part_start = $part_offset;
         $part_offset += $part_len;

         if( !ref $part ) {
            return $more_func->( $part );
         }
         elsif( blessed $part and $part->isa( "Future" ) ) {
            return $part->then( sub {
               my ( $more ) = @_;
               return Future->new->done( $more_func->( $more ) );
            });
         }
         elsif( ref $part eq "CODE" ) {
            my $buffer = "";
            return sub {
               my ( $pos, $len ) = @_;
               my $end = $pos + $len;
               if( length $buffer < $end ) {
                  my $more = $part->( length $buffer, $end - length $buffer );
                  $buffer .= $more_func->( $more );
               }
               $on_progress->( $part_start + $end );
               return substr( $buffer, $pos, $len );
            }, $part_len
         }
         else {
            die "TOOD: Not sure what to do with part";
         }
      },
   )->then( sub {
      $self->put_meta( $s3path, "md5sum", $md5->hexdigest . "\n" );
   });

   return $f unless @more_futures;
   return Future->needs_all( $f, @more_futures );
}

sub put_file
{
   my $self = shift;
   my ( $localpath, $s3path, %args ) = @_;

   open my $fh, "<", $localpath or die "Cannot read $localpath - $!";

   my ( $len_total, $mtime ) = ( stat $fh )[7,9];
   $args{on_progress}->( 0, $len_total );

   $self->_put_file_from_fh( $fh, $s3path,
      mtime => $mtime,
      %args,
   );
}

sub _get_file_to_code
{
   my $self = shift;
   my ( $s3path, $on_data, %args ) = @_;

   my $md5 = Digest::MD5->new;

   my $initial = 1;

   my $gpg_stdin;
   my $gpg_future; # undef unless we're waiting for GPG as well

   Future->needs_all(
      $self->get_meta( $s3path, "md5sum" )
         ->transform( done => sub { chomp $_[0]; $_[0] } ),
      $self->{s3}->get_object(
         key    => "data/$s3path",
         on_chunk => sub {
            my ( $header, $data ) = @_;

            if( $initial ) {
               $initial--;

               if( defined $header->header( "X-Amz-Meta-Keyid" ) ) {
                  my $orig_on_data = $on_data;

                  $gpg_future = $self->loop->new_future;
                  my $gpg_process = IO::Async::Process->new(
                     command => [ "gpg", "--decrypt", "-" ],
                     stdin  => { via => "pipe_write" },
                     stdout => {
                        on_read => sub {
                           my ( undef, $buffref ) = @_;
                           $md5->add( $$buffref );
                           $orig_on_data->( $header, $$buffref );
                           $$buffref = "";
                        },
                     },
                     on_finish => sub {
                        my ( undef, $exitcode ) = @_;
                        $gpg_future->done;
                        $exitcode == 0 and return;

                        die "gpg exited non-zero $exitcode\n";
                     },
                  );
                  $self->add_child( $gpg_process );

                  $gpg_stdin = $gpg_process->stdin;

                  $on_data = sub {
                     $gpg_stdin->write( $_[1] );
                  };
               }
            }

            $md5->add( $data ) if defined $data and !$gpg_stdin;
            $on_data->( $header, $data );
         },
      )
   )->and_then( sub {
      my $f = shift;
      return $f unless defined $gpg_future;

      # Close pipe to gpg and wait for it to finish
      $gpg_stdin->close_when_empty;
      $gpg_future->then( sub { $f } );
   })->then( sub {
      my ( $exp_md5sum, undef, $header, $meta ) = @_;
      $on_data->( $header, undef ); # Indicate EOF

      my $got_md5sum = $md5->hexdigest;
      if( $exp_md5sum ne $got_md5sum ) {
         die "Expected MD5sum '$exp_md5sum', got '$got_md5sum'\n";
      }

      Future->new->done( $header, $meta );
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

   $self->_get_file_to_code(
      $s3path,
      sub {
         my ( $header, $data ) = @_;
         return unless defined $data;

         if( !defined $len_total ) {
            $len_so_far = 0;
            $len_total = $header->content_length;

            $on_progress->( $len_so_far, $len_total );
         }

         $fh->print( $data );
         $len_so_far += length $data;
         $on_progress->( $len_so_far, $len_total );
      },
   )->then( sub {
      my ( $header, $meta ) = @_;

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
   my $STDOUT = $options{stdout} || \*STDOUT;

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
               enc   => defined $meta->{Keyid},
            } );
         });
      } foreach => $keys, concurrent => 20 )->get;
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
            printf $STDOUT "%-38s %15d %s %s\n", $name, $e->{size}, $timestamp, $e->{enc} ? "ENC" : "   ";
         }
         else {
            printf $STDOUT "%-38s\n", $name;
         }
      }
      elsif( !@files or @$prefixes and $prefixes->[0] lt $files[0]{name} ) {
         my $name = substr shift @$prefixes, 5;
         next if $re and substr( $name, 0, -1 ) !~ $re;

         printf $STDOUT "%-38s DIR\n", $name;
      }
   }
}

sub cmd_cat
{
   my $self = shift;
   my ( $s3path, %args ) = @_;

   my $STDOUT = $args{stdout} || \*STDOUT;

   # Only do progress output if STDOUT is not a terminal
   my $do_progress = !-t \*STDOUT;

   my $len_so_far = 0;
   my $progress_timer;

   $self->_get_file_to_code(
      $s3path,
      sub {
         my ( $header, $data ) = @_;
         return unless defined $data;

         if( $do_progress ) {
            $progress_timer ||= $self->_start_progress_one( $header->content_length, \$len_so_far );
            $len_so_far += length $data;
         }

         print $STDOUT $data;
      },
   )->get;

   $self->print_message( "Successfully got $s3path to <stdout>" ) if $do_progress;
}

sub cmd_uncat
{
   my $self = shift;
   my ( $s3path, %args ) = @_;

   my $STDIN = $args{stdin} || \*STDIN;

   if( $args{no_overwrite} ) {
      defined $self->stat_file( $s3path )->get and
         die "Not overwriting S3 file $s3path (use the --force)\n";
   }

   $self->_put_file_from_fh( $STDIN, $s3path,
      mtime => time,
      on_progress => sub {
         # TODO
      },
      %args,
   )->get;
}

sub cmd_get
{
   my $self = shift;
   my ( $s3path, $localpath, %args ) = @_;

   my $len_so_far;
   my $progress_timer;

   if( $args{no_overwrite} ) {
      stat( $localpath ) and
         die "Not overwriting local file $localpath (use the --force)\n";
   }

   $self->get_file(
      $s3path, $localpath,
      on_progress => sub {
         $len_so_far = $_[0];
         $progress_timer ||= $self->_start_progress_one( $_[1], \$len_so_far );
      },
   )->get;

   $self->print_message( "Successfully got $s3path to $localpath" );

   $self->remove_child( $progress_timer );
}

sub cmd_put
{
   my $self = shift;
   my ( $localpath, $s3path, %args ) = @_;

   my $len_so_far;
   my $progress_timer;

   if( $args{no_overwrite} ) {
      defined $self->stat_file( $s3path )->get and
         die "Not overwriting S3 file $s3path (use the --force)\n";
   }

   $self->put_file(
      $localpath, $s3path,
      on_progress => sub {
         $len_so_far = $_[0];
         $progress_timer ||= $self->_start_progress_one( $_[1], \$len_so_far );
      },
   )->get;

   $self->print_message( "Successfully put $localpath to $s3path" );

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
      print "Removed $s3path\n" unless $self->{quiet};
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
   my $skipped_files   = 0;
   my $completed_bytes = 0;
   my $skipped_bytes   = 0;

   my @uploads;
   my $timer = $self->_start_progress_bulk( \@uploads, $total_files, $total_bytes, \$completed_files, \$completed_bytes, \$skipped_bytes );

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      my $localpath = "$localroot/$relpath";
      # Allow $s3root="" to mean upload into root
      my $s3path    = join "/", grep { length } $s3root, $relpath;

      push @uploads, my $slot = [ $s3path, $size, "test" ];

      $self->test_skip( $skip_logic, $s3path, $localpath )->then( sub {
         my ( $skip ) = @_;
         if( $skip ) {
            $self->print_message( "SKIP  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;
            $skipped_files   += 1;
            $skipped_bytes   += $size;

            @uploads = grep { $_ != $slot } @uploads;
            $timer->invoke_event( on_tick => );
            return Future->new->done;
         }

         $self->print_message( "START $relpath" );
         $slot->[2] = 0;
         $timer->invoke_event( on_tick => );

         return $self->put_file(
            $localpath, $s3path,
            on_progress => sub { ( $slot->[2] ) = @_ },
         )->on_done( sub {
            $self->print_message( "DONE  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;

            @uploads = grep { $_ != $slot } @uploads;
            $timer->invoke_event( on_tick => );
         });
      });
   } foreach => \@files,
     concurrent => $concurrent )->get;

   $self->print_message( sprintf "All files done\n" . 
      "  %d files (%d transferred, %d skipped)\n  %d bytes (%d transferred, %d skipped)",
      $completed_files, $completed_files - $skipped_files, $skipped_files,
      $completed_bytes, $completed_bytes - $skipped_bytes, $skipped_bytes );
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
      my $name = substr $_->{key}, 5;
      $name =~ s{^\Q$s3root\E/}{};

      next unless $filter->( $name );

      $total_bytes += $_->{size};
      $total_files += 1;
      push @files, [ $name, $_->{size} ];
   }

   $self->print_message( sprintf "Found %d files totalling %d bytes (%.1f MiB)",
      $total_files, $total_bytes, $total_bytes / (1024*1024) );

   my $completed_files = 0;
   my $skipped_files   = 0;
   my $completed_bytes = 0;
   my $skipped_bytes   = 0;

   my @downloads;
   my $timer = $self->_start_progress_bulk( \@downloads, $total_files, $total_bytes, \$completed_files, \$completed_bytes, \$skipped_bytes );

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      # Allow $s3root="" to mean download from root
      my $s3path    = join "/", grep { length } $s3root, $relpath;
      my $localpath = "$localroot/$relpath";

      $self->test_skip( $skip_logic, $s3path, $localpath )->then( sub {
         my ( $skip ) = @_;
         if( $skip ) {
            $self->print_message( "SKIP  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;
            $skipped_files   += 1;
            $skipped_bytes   += $size;
            return Future->new->done;
         }

         $self->print_message( "START $relpath" );
         push @downloads, my $slot = [ $s3path, $size, 0 ];
         $timer->invoke_event( on_tick => );

         return $self->get_file(
            $s3path, $localpath,
            on_progress => sub { ( $slot->[2] ) = @_ },
            mkdir => 1,
         )->on_done( sub {
            $self->print_message( "DONE  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;

            @downloads = grep { $_ != $slot } @downloads;
            $timer->invoke_event( on_tick => );
         });
      });
   } foreach => \@files,
     concurrent => $concurrent )->get;

   $self->print_message( sprintf "All files done\n" . 
      "  %d files (%d transferred, %d skipped)\n  %d bytes (%d transferred, %d skipped)",
      $completed_files, $completed_files - $skipped_files, $skipped_files,
      $completed_bytes, $completed_bytes - $skipped_bytes, $skipped_bytes );
   $self->remove_child( $timer );
}

1;
