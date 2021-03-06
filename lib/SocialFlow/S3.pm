package SocialFlow::S3;

use strict;
use warnings;
use feature qw( switch );
use base qw( IO::Async::Notifier );
no if $] >= 5.017011, warnings => 'experimental::smartmatch';

use Future 0.22; # ->done when cancelled bugfix
use Future::Utils 0.22 qw( call_with_escape try_repeat fmap_scalar fmap_void );
use IO::Async::Listener;
use IO::Async::Process;
use IO::Async::Stream;
use IO::Async::Timer::Periodic;
use Net::Async::Webservice::S3 0.17; # concurrent ->put_object

use Cwd qw( abs_path );
use Digest::MD5;
use File::Basename qw( dirname );
use File::Path qw( make_path );
use IO::Termios;
use List::Util qw( max sum );
use List::UtilsBy qw( sort_by );
use POSIX qw( ceil strftime );
use POSIX::strptime qw( strptime );
use Scalar::Util qw( blessed );
use Term::Size;
use Time::HiRes qw( time );
use Time::Local qw( timegm );

use SocialFlow::S3::GpgAgentStream;

use constant DEFAULT_PART_SIZE => 100*1024*1024; # 100 MiB

use constant FILES_AT_ONCE => 4;

our $VERSION = "0.04";

my $stderr_width = ( Term::Size::chars \*STDERR )[0] // 80;
$SIG{WINCH} = sub {
   $stderr_width = ( Term::Size::chars \*STDERR )[0] // 80;
};

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

   $args->{s3}->{http}->configure(
      # TODO: Currently it's essential to turn off pipelining so that multiple
      # concurrent uploads of parts of a single file will work
      #   https://rt.cpan.org/Ticket/Display.html?id=89776
      pipeline => 0,
      max_connections_per_host => 0,
   );

   $args->{timeout}       //= 10;
   $args->{stall_timeout} //= 30;
   $args->{get_retries}   //= 3;
   $args->{part_size}     //= DEFAULT_PART_SIZE;

   $args->{progress}      //= 0;
   $args->{debug}         //= 0;

   $self->{status_lines} = 0;
   $self->{prompt_lines} = 0;
   $self->{prompt}       = "";

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

   foreach (qw( quiet progress debug get_retries part_size )) {
      $self->{$_} = delete $args{$_} if exists $args{$_};
   }

   foreach (qw( timeout stall_timeout )) {
      next unless exists $args{$_};

      $self->{s3}->configure( $_ => $args{$_} );
      $self->{$_} = delete $args{$_};
   }

   if( my $bucket = delete $args{bucket} ) {
      ( $bucket, my $prefix ) = split m(/), $bucket, 2;
      $prefix .= "/" if defined $prefix and length $prefix and $prefix !~ m(/$);
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

   # Make one atomic print for smoothness
   my $buffer = "";

   # Clear an old status message
   $buffer .= "\e\x4D\e[K" for 1 .. $self->{status_lines} + $self->{prompt_lines};
   $self->{status_lines} = 0;

   foreach ( split m/\n/, $msg ) {
      if( $stderr_width and length > $stderr_width ) {
         $buffer .= substr($_, 0, $stderr_width-3) . "...";
      }
      else {
         $buffer .= $_;
      }
      $buffer .= "\n";
   }

   $buffer .= $self->{prompt};

   print STDERR $buffer;
}

sub print_status
{
   my $self = shift;
   my ( $status ) = @_;

   $status =~ s/\n$//; # only the final one

   $self->print_message( $status );

   $self->{status_lines} = () = split m/\n/, $status, -1;
}

sub print_prompt
{
   my $self = shift;
   my ( $prompt ) = @_;

   $prompt .= "\n" unless $prompt =~ m/\n\Z/;

   # Make one atomic print for smoothness
   my $buffer = "";

   # Clear an old prompt
   $buffer .= "\e\x4D\e[K" for 1 .. $self->{prompt_lines};
   $buffer .= $prompt;

   print STDERR $buffer;

   $self->{prompt_lines} = ( () = split m/\n/, $prompt, -1 ) - 1;
   $self->{prompt} = $prompt;
}

sub clear_prompt
{
   my $self = shift;

   print STDERR "\e\x4D\e[K" for 1 .. $self->{prompt_lines};

   $self->{prompt_lines} = 0;
   $self->{prompt}       = "";
}

sub print_debug
{
   my $self = shift;
   my ( $level, $message ) = @_;

   $self->print_message( "DEBUG$level $message" ) if $self->{debug} >= $level;
}

# Join filepaths by ensuring exactly one '/' between each component
sub _joinpath
{
   my @str = @_;
   $_ =~ s(^/)() for @str[1..$#str];
   $_ =~ s(/$)() for @str[0..$#str-1];
   return join "/", @str;
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
      prefix => _joinpath( "data", $prefix ),
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
         my $skipped_bytes = $skipped_bytes_ref ? $$skipped_bytes_ref : 0;

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
         unshift @times, [ $done_bytes - $skipped_bytes, time ];
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

# Filesystem abstraction methods, so we can override them for unit testing to
# virtualise the filesystem

sub fopen_read
{
   my $self = shift;
   my %args = @_;

   my $path = $args{path};

   open my $fh, "<", $path or die "Cannot read $path - $!";
   return $fh;
}

sub fopen_write
{
   my $self = shift;
   my %args = @_;

   my $path = $args{path};

   if( $args{mkdir} and ! -d dirname( $path ) ) {
      make_path( dirname $path );
   }

   open my $fh, ">", $path or die "Cannot write $path - $!";
   return $fh;
}

sub fstat_type_size_mtime
{
   my $self = shift;
   my %args = @_;

   my ( $size, $mtime ) = ( stat ($args{fh} // $args{path}) )[7,9];

   return
      -d _ ? "d" : -f _ ? "f" : "?",
      $size,
      $mtime;
}

sub freaddir
{
   my $self = shift;
   my %args = @_;

   my $path = $args{path};

   opendir my $dirh, $path or die "Cannot opendir $path - $!\n";
   return readdir $dirh;
}

sub futime
{
   my $self = shift;
   my %args = @_;

   my $path = $args{path};

   utime( $args{atime}, $args{mtime}, $path ) or die "Cannot utime $path - $!";
}

# S3 abstractions

sub put_meta
{
   my $self = shift;
   my ( $path, $metaname, $value ) = @_;

   $self->{s3}->put_object(
      key => _joinpath( "meta", $path, $metaname ),
      value => $value,
      timeout => $self->{timeout},
   );
}

sub get_meta
{
   my $self = shift;
   my ( $path, $metaname ) = @_;

   $self->{s3}->get_object(
      key => _joinpath( "meta", $path, $metaname ),
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
      key => _joinpath( "meta", $path, $metaname ),
      timeout => $self->{timeout},
   )->followed_by( sub {
      my $f = shift;

      if( $f->failure ) {
         my ( $message, $name, $response ) = $f->failure;
         return Future->new->done if $name and $name eq "http" and
                                     $response and $response->code == 404;
         return $f;
      }

      $self->{s3}->delete_object(
         key => _joinpath( "meta", $path, $metaname ),
      )
   });
}

# Make an 'else_with_f' sub that ignores http 404 responses
sub _gen_ignore_404
{
   my ( $return_on_404 ) = @_;

   sub {
      my ( $f, $message, $name, $response ) = @_;
      return Future->new->done( $return_on_404 ) if $name and $name eq "http" and
                                                    $response and $response->code == 404;
      return $f;
   };
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
         my ( undef, $localsize, $localmtime ) = $self->fstat_type_size_mtime( path => $localpath );
         if( !defined $localsize ) { 
            $self->print_debug( 1 => "no skip - local file missing" );
            return Future->new->done( 0 );
         }

         # Fetch the md5sum meta anyway even if we aren't going to use it, because if
         # it's missing we definitely want to re-upload
         $f = Future->needs_all(
            $self->{s3}->head_object( key => _joinpath( "data", $s3path ) )->else_with_f( sub {
               my ( $f, $message, $name, $response ) = @_;
               $self->print_debug( 1 => "no skip - $s3path data missing" )
                  if $name and $name eq "http" and $response and $response->code == 404;
               return $f;
            }),
            $self->get_meta( $s3path, "md5sum" )->transform( done => sub { chomp $_[0]; $_[0] } )->else_with_f( sub {
               my ( $f, $message, $name, $response ) = @_;
               $self->print_debug( 1 => "no skip - $s3path meta md5sum missing" )
                  if $name and $name eq "http" and $response and $response->code == 404;
               return $f;
            }),
         )->then( sub {
            my ( $header, $meta, $s3md5 ) = @_;

            if( !defined $meta->{Mtime} ) {
               $self->print_debug( 1 => "no skip - $s3path missing Mtime" );
               return Future->new->done( 0 );
            }

            my $s3size = $header->content_length;
            if( $s3size != $localsize ) {
               $self->print_debug( 1 => "no skip - lengths differ (S3 $s3size; local $localsize)" );
               return Future->new->done( 0 );
            }

            my $s3mtime = strptime_iso8601( $meta->{Mtime} );
            if( $s3mtime != $localmtime ) {
               $self->print_debug( 1 => "no skip - mtime differs (S3 $s3mtime; local $localmtime)" );
               return Future->new->done( 0 );
            }

            return Future->new->done( 1, $s3md5 );
         })->else_with_f( _gen_ignore_404( 0 ) );
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

               if( $s3md5 ne $localmd5 ) {
                  $self->print_debug( 1 => "no skip - MD5sum differs (S3 $s3md5; local $localmd5)" );
                  return Future->new->done( 0 );
               }

               return Future->new->done( 1 );
            })->else_with_f( _gen_ignore_404( 0 ) );
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
      key => _joinpath( "data", $s3path ),
   )->else_with_f( _gen_ignore_404( undef ) );
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
         command => [ "gpg", "--encrypt", "--recipient", $keyid, "--batch", "-" ],
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
            ->then_with_f( sub {
               ( my $f, my $content, $eof ) = @_;

               $md5->add( $content );

               $f;
            })
      })->on_done( sub {
         $gpg_process->stdin->close;
      });

      $fh_stream = $gpg_process->stdout;
      $fh = $fh_stream->read_handle;
   }
   else {
      $fh_stream = IO::Async::Stream->new(
         read_handle => $fh,
         on_read => sub { 0 },
      );
      $self->add_child( $fh_stream );

      $more_func = sub {
         $md5->add( ref $_[0] ? ${$_[0]} : $_[0] );
      };
   }

   stat( $fh ) or die "Cannot stat FH - $!";

   my $part_size = $self->{part_size};

   my $gen_parts;
   if( -f _ ) {
      # Ignore the fh_stream here
      $self->remove_child( $fh_stream ); undef $fh_stream;

      my $len_total = -s _;
      my $read_pos = 0;

      $gen_parts = sub {
         return if $read_pos >= $len_total;

         my $part_start = $read_pos;
         my $part_length = $len_total - $part_start;
         $part_length = $part_size if $part_length > $part_size;

         $read_pos += $part_length;

         # Need to atomically ensure the -entire- chunk is read from the disk
         # initially before we return, in case of concurrency
         my $buffer = "";
         while( length $buffer < $part_length ) {
            sysread( $fh, $buffer, $part_length, length $buffer ) or die "Cannot read - $!";
         }

         $more_func->( $buffer ) if $more_func;

         return sub {
            my ( $pos, $len ) = @_;
            return substr( $buffer, $pos, $len );
         }, $part_length;
      };
   }
   elsif( -p _ or -S _ ) {
      # pipe or socket
      # this case is used for all GPG-driven input

      $fh_stream->configure(
         # Set waterlevels to ensure the stream buffer doesn't grow arbitrarily
         read_high_watermark => $part_size * 1.2,
         read_low_watermark  => $part_size * 0.6,

         # Need the stream -not- to ->remove itself from the Loop when it hits EOF
         #   TODO: IO::Async might want to defer this one
         close_on_read_eof => 0,
      );

      my $eof;
      $gen_parts = sub {
         return if $eof;
         my $f = $fh_stream->read_exactly( $part_size )
            ->then( sub {
               # Avoid copying $_[0] because it may be a large string
               my $contentref = \$_[0];
               $eof = $_[1];

               $fh_stream->remove_from_parent if $eof;

               $more_func->( $contentref ) if $more_func;
               my $code = sub {
                  my ( $pos, $len ) = @_;
                  return substr( $$contentref, $pos, $len );
               };
               Future->new->done( $code, length $$contentref );
            });
         return $f;
      };
   }
   else {
      die "Cannot put from $fh - must be a regular file, pipe, or socket\n";
   }

   my @more_futures;

   my $f = $self->{s3}->put_object(
      key        => _joinpath( "data", $s3path ),
      meta       => \%meta,
      gen_parts  => $gen_parts,
      on_write   => $on_progress,
      concurrent => $args{concurrent},
      stall_timeout => $self->{stall_timeout},
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

   my $fh = $self->fopen_read( path => $localpath );

   my ( undef, $len_total, $mtime ) = $self->fstat_type_size_mtime( fh => $fh );
   $args{on_progress}->( 0, $len_total ) if $args{on_progress};

   $self->_put_file_from_fh( $fh, $s3path,
      mtime => $mtime,
      %args,
   );
}

# We have to act like a gpg-agent enough to ask passphrases from the user.
# We'll be starting multiple gpg processes and we can't let them all talk to
# the terminal, so we'll proxy inbetween to ensure we only ask for each
# passphrase once.

sub prompt_and_readline
{
   my $self = shift;
   my ( $prompt ) = @_;

   my $value_f = $self->loop->new_future;

   my $stdin = $self->{stdin} ||= do {
      my $stdin = IO::Async::Stream->new_for_stdin( on_read => sub { 0 } );
      $self->add_child( $stdin );
      $self->{stdin_termios} = IO::Termios->new( \*STDIN );
      $stdin;
   };
   my $termios = $self->{stdin_termios};

   $self->print_prompt( $prompt );
   my $wasecho = $termios->getflag_echo;
   $termios->setflag_echo( 0 );

   $stdin->read_until( qr/\r?\n/ )->on_done( sub {
      $termios->setflag_echo( $wasecho );
      $self->clear_prompt;
   });
}

my $gpg_agent_sock_path = ".sfs3-fake-gpg-agent.sock"; # TODO: tmpdir? cleanup?
END {
   unlink $gpg_agent_sock_path if -e $gpg_agent_sock_path;
}

sub start_gpg_agent
{
   my $self = shift;

   return if $self->{gpg_agent_running};

   # A cache of futures giving the actual passphrases
   my %passphrases;

   my $listener = IO::Async::Listener->new(
      handle_class => "SocialFlow::S3::GpgAgentStream",
      on_accept => sub {
         my ( $listener, $stream ) = @_;
         $listener->add_child( $stream );
         $stream->write( "OK Pleased to meet you\n" );
         $stream->configure(
            on_get_passphrase => sub {
               my ( $stream, $cache_id, $errmsg, $prompt, $desc ) = @_;

               my $value_f = $passphrases{$cache_id} ||=
                  $self->prompt_and_readline( $desc )->transform(
                     # hex encode it for GPG
                     done => sub { chomp $_[0]; unpack "H*", $_[0] }
                  );

               $value_f->on_done( sub {
                  my ( $value ) = @_;
                  $stream->write( "OK $value\n" );
               });
            }
         );
      },
   );
   $self->add_child( $listener );

   # GPG agent socket path has to be absolute or gpg won't accept it
   $ENV{GPG_AGENT_INFO} = join ":", abs_path( $gpg_agent_sock_path ), $$, 1;

   unlink $gpg_agent_sock_path if -e $gpg_agent_sock_path;
   $listener->listen(
      addr => { family => "unix", socktype => "stream", path => $gpg_agent_sock_path },
   )->get; # this should be synchronous

   $self->{gpg_agent_running}++;
}

sub _get_file_to_code
{
   my $self = shift;
   my ( $s3path, $on_data, %args ) = @_;

   my $md5 = Digest::MD5->new;

   # A buffer of data saved before $on_more was set. This is usually needed
   # only in synchronous cases, such as during the unit tests.
   my $prebuffer_more;
   my $on_more;
   # TODO: it may be possible make this part neater, but that would involve
   # being able to have a byte pipeline through an incomplete Future, which
   # is currently not possible.

   Future->needs_all(
      $self->get_meta( $s3path, "md5sum" )
         ->transform( done => sub { chomp $_[0]; $_[0] } ),

      $self->{s3}->head_then_get_object(
         key      => _joinpath( "data", $s3path ),
         on_chunk => sub {
            my ( $header, $data ) = @_;
            if( $on_more ) {
               $on_more->( $data );
            }
            else {
               $prebuffer_more .= $data if defined $data;
            }
         },
         stall_timeout => $self->{stall_timeout},
      )->then( sub {
         my ( $value_f, $header, $meta ) = @_;

         if( defined $meta->{Keyid} ) {
            my $gpg_future = $self->loop->new_future;
            $self->start_gpg_agent;
            my $gpg_process = IO::Async::Process->new(
               command => [ "gpg", "--decrypt", "--batch", "--use-agent", "-" ],
               stdin  => { via => "pipe_write" },
               stdout => {
                  on_read => sub {
                     my ( undef, $buffref ) = @_;
                     $md5->add( $$buffref );
                     $on_data->( $header, $$buffref, $meta );
                     $$buffref = "";
                  },
               },
               setup => [
                  stderr => [ open => ">>", "/dev/null" ],
               ],
               on_finish => sub {
                  my ( undef, $exitcode ) = @_;
                  $exitcode == 0 and return $gpg_future->done;

                  $gpg_future->fail( "GPG exited non-zero $exitcode", gpg => $exitcode );
               },
            );
            $self->add_child( $gpg_process );

            my $gpg_stdin = $gpg_process->stdin;

            $on_more = sub {
               $gpg_stdin->write( $_[0] );
            };
            if( defined $prebuffer_more ) {
               $on_more->( $prebuffer_more );
               undef $prebuffer_more;
            }

            return $value_f->then( sub {
               # Close pipe to gpg and wait for it to finish
               $gpg_stdin->close_when_empty;
               return $gpg_future->then( sub { $value_f } );
            });
         }
         else {
            $on_more = sub {
               $md5->add( $_[0] ) if defined $_[0];
               $on_data->( $header, $_[0], $meta );
            };
            if( defined $prebuffer_more ) {
               $on_more->( $prebuffer_more );
               undef $prebuffer_more;
            }

            return $value_f;
         }
      })->on_ready( sub {
         # Perls before 5.18 have a bug, wherein the reference cycle between
         # two nested ANON closures, such as is created above, isn't always
         # cleaned up properly. We can workaround this by manually breaking
         # the cycle here
         undef $on_more;
      })
   )->then( sub {
      my ( $exp_md5sum, undef, $header, $meta ) = @_;
      $on_data->( $header, undef ); # Indicate EOF

      my $got_md5sum = $md5->hexdigest;
      if( $exp_md5sum ne $got_md5sum ) {
         return Future->new->fail(
            "MD5sum failed for $s3path - expected MD5sum '$exp_md5sum', got '$got_md5sum'\n",
            get_file => md5sum => $exp_md5sum, $got_md5sum,
         );
      }

      Future->new->done( $header, $meta );
   });
}

sub get_file
{
   my $self = shift;
   my ( $s3path, $localpath, %args ) = @_;
   my $on_progress = $args{on_progress};

   my $fh = $self->fopen_write( path => $localpath, mkdir => $args{mkdir} );

   my $len_total;
   my $len_so_far;

   my $delay = 0.5;
   my $retries = $self->{get_retries};

   ( try_repeat {
      my ( $prev_f ) = @_;

      # Add a small delay after failure before retrying
      my $delay_f =
         $prev_f ? $self->loop->delay_future( after => ( $delay *= 2 ) )
                 : Future->new->done;

      $delay_f->then( sub {
         # clear previous content in case of retry
         undef $len_total;

         # Note: -technically- this truncate doesn't clear the in-memory scalar
         # handle that t/20get_file.t 's unit test uses. But that's OK as we're
         # going to overwrite its content anyway.
         #   https://rt.perl.org/rt3/Public/Bug/Display.html?id=40241
         $fh->seek( 0, 0 );
         $fh->truncate( 0 );

         $self->_get_file_to_code(
            $s3path,
            sub {
               my ( $header, $data ) = @_;
               return unless defined $data;

               if( !defined $len_total ) {
                  $len_so_far = 0;
                  $len_total = $header->content_length;

                  $on_progress->( $len_so_far, $len_total ) if $on_progress;
               }

               $fh->print( $data );
               $len_so_far += length $data;
               $on_progress->( $len_so_far, $len_total ) if $on_progress;
            },
         )
      });
   } while => sub {
      my $f = shift;
      my ( $failure, $name, $response ) = $f->failure or return 0; # success
      return 0 if $name and $name eq "http" and
                  $response and $response->code =~ m/^4/ # don't retry HTTP 4xx
                            and $response->code != 400;  # but do retry 400 itself because S3 sometimes throws those :/
      return --$retries;
   })->then( sub {
      my ( $header, $meta ) = @_;

      close $fh;

      if( defined $meta->{Mtime} ) {
         my $mtime = strptime_iso8601( $meta->{Mtime} );
         $self->futime( path => $localpath, mtime => $mtime, atime => $mtime );
      }

      Future->new->done;
   });
}

# A sortof combination of get and put; reads local and S3 files and compares.
# Future returns false if no differences, or one of "size", "mtime", "bytes"
sub cmp_file
{
   my $self = shift;
   my ( $localpath, $s3path, %args ) = @_;
   my $on_progress = $args{on_progress};

   my $fh = $self->fopen_read( path => $localpath );

   my ( undef, $locallen, $localmtime ) = $self->fstat_type_size_mtime( fh => $fh );

   my $len_total;
   my $len_so_far;

   my $delay = 0.5;
   my $retries = $self->{get_retries};

   call_with_escape {
      my $escape_f = shift;

      try_repeat {
         my ( $prev_f ) = @_;

         # Add a small delay after failure before retrying
         my $delay_f =
            $prev_f ? $self->loop->delay_future( after => ( $delay *= 2 ) )
                    : Future->new->done;

         $delay_f->then( sub {
            $fh->seek( 0, 0 );
            $len_so_far = 0;

            $self->_get_file_to_code(
               $s3path,
               sub {
                  my ( $header, $s3data, $s3meta ) = @_;
                  return unless defined $s3data;

                  if( !defined $len_total ) {
                     $len_so_far = 0;
                     $len_total = $header->content_length;

                     if( $len_total != $locallen ) {
                        $escape_f->done( "size" );
                        return;
                     }
                     if( defined $s3meta->{Mtime} ) {
                        my $s3mtime = strptime_iso8601( $s3meta->{Mtime} );
                        if( $s3mtime != $localmtime ) {
                           $escape_f->done( "mtime" );
                           return;
                        }
                     }

                     $on_progress->( $len_so_far, $len_total ) if $on_progress;
                  }

                  my $len = length $s3data;

                  $fh->read( my $localdata, $len );

                  if( $localdata ne $s3data ) {
                     $escape_f->done( "bytes" );
                  }

                  $len_so_far += $len;
                  $on_progress->( $len_so_far, $len_total ) if $on_progress;
               },
            )->then_done();
         });
      } while => sub {
         my $f = shift;
         my ( $failure, $name, $response ) = $f->failure or return 0; # success
         return 0 if $name and $name eq "http" and
                     $response and $response->code =~ m/^4/ # don't retry HTTP 4xx
                               and $response->code != 400;  # but do retry 400 itself because S3 sometimes throws those :/
         return --$retries;
      };
   };
}

sub delete_file
{
   my $self = shift;
   my ( $s3path ) = @_;

   Future->needs_all(
      $self->{s3}->delete_object(
         key    => _joinpath( "data", $s3path ),
      ),
      $self->{s3}->list_bucket(
         prefix => _joinpath( "meta", $s3path, "/" ),
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
      prefix => _joinpath( "data", $prefix ),
      delimiter => ( $RECURSE ? "" : "/" ),
   )->get;

   my @files;
   if( $LONG ) {
      @files = ( fmap_scalar {
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

sub cmd_get
{
   my $self = shift;
   my ( $s3path, $localpath, %args ) = @_;

   my $len_so_far = 0;
   my $progress_timer;

   if( $localpath eq "-" ) {
      # Only do progress output if STDOUT is not a terminal
      my $do_progress = $self->{progress} && !-t \*STDOUT;

      $self->_get_file_to_code(
         $s3path,
         sub {
            my ( $header, $data ) = @_;
            return unless defined $data;

            if( $do_progress ) {
               $progress_timer ||= $self->_start_progress_one( $header->content_length, \$len_so_far );
               $len_so_far += length $data;
            }

            print STDOUT $data;
         },
      )->get;
   }
   else {
      if( $args{no_overwrite} ) {
         stat( $localpath ) and
            die "Not overwriting local file $localpath (use the --force)\n";
      }

      $self->get_file(
         $s3path, $localpath,
         ( $self->{progress} ? 
            ( on_progress => sub {
               $len_so_far = $_[0];
               $progress_timer ||= $self->_start_progress_one( $_[1], \$len_so_far );
            } ) : () ),
      )->get;
   }

   $self->print_message( "Successfully got $s3path to $localpath" );

   $self->remove_child( $progress_timer ) if $progress_timer;
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

   if( $localpath eq "-" ) {
      $self->_put_file_from_fh( \*STDIN, $s3path,
         mtime => time,
         ( $self->{progress} ? ( on_progress => sub {
               $len_so_far = $_[0];
               $progress_timer ||= $self->_start_progress_one( undef, \$len_so_far );
            } ) : () ),
         %args,
      )->get;
   }
   else {
      $self->put_file(
         $localpath, $s3path,
         ( $self->{progress} ? ( on_progress => sub {
               $len_so_far = $_[0];
               $progress_timer ||= $self->_start_progress_one( $_[1], \$len_so_far );
            } ) : () ),
         %args,
      )->get;
   }

   $self->print_message( "Successfully put $localpath to $s3path" );

   $self->remove_child( $progress_timer ) if $progress_timer;
}

sub cmd_rm
{
   my $self = shift;
   my ( $s3pattern, %args ) = @_;

   my @s3paths = $self->_expand_pattern( $s3pattern );
   if( !@s3paths ) {
      print STDERR "Nothing matched $s3pattern\n";
      exit 1;
   }

   if( $args{recurse} ) {
      my @keys;
      ( fmap_void {
         my $s3path = shift;
         $self->{s3}->list_bucket(
            prefix => _joinpath( "data", $s3path, "/" ),
            delimiter => "",
         )->on_done( sub {
            my ( $keys ) = @_;
            push @keys,
               $s3path,
               map { substr $_->{key}, 5 } @$keys;
         });
      } concurrency => 4,
        foreach => \@s3paths )->get;

     @s3paths = @keys;
   }

   ( fmap_void {
      my $s3path = shift;
      $self->delete_file( $s3path )
         ->on_done( sub { print "Removed $s3path\n" unless $self->{quiet} } );
    } concurrent => 4,
      foreach => \@s3paths )->get;
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

      my $localpath = _joinpath( grep { defined } $localroot, $relpath );

      $self->print_message( "Scanning $localpath..." );

      my @moredirs;
      foreach ( sort $self->freaddir( path => $localpath ) ) {
         next if $_ eq "." or $_ eq "..";

         my $ent = _joinpath( grep { defined } $relpath, $_ );

         my ( $type, $size ) = $self->fstat_type_size_mtime( path => "$localroot/$ent" );

         if( $type eq "d" ) {
            push @moredirs, $ent;
         }
         elsif( $type eq "f" ) {
            next unless $filter->( $ent );
            push @files, [ $ent, $size ];
            $total_bytes += $size;
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
   my $aborted_files   = 0;
   my $aborted_bytes   = 0;

   my @uploads;
   my $timer;
   if( $self->{progress} ) {
      $timer = $self->_start_progress_bulk( \@uploads, $total_files, $total_bytes, \$completed_files, \$completed_bytes, \$skipped_bytes );
   }

   my $recent_aborts = 0;

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      my $localpath = _joinpath( $localroot, $relpath );
      # Allow $s3root="" to mean upload into root
      my $s3path    = _joinpath( grep { length } $s3root, $relpath );

      push @uploads, my $slot = [ $relpath, $size, "test" ];

      $self->test_skip( $skip_logic, $s3path, $localpath )->then( sub {
         my ( $skip ) = @_;
         if( $skip ) {
            $self->print_message( "SKIP  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;
            $skipped_files   += 1;
            $skipped_bytes   += $size;

            @uploads = grep { $_ != $slot } @uploads;
            $timer->invoke_event( on_tick => ) if $timer;
            return Future->new->done;
         }

         $self->print_message( "START $relpath" );
         $slot->[2] = 0;
         $timer->invoke_event( on_tick => ) if $timer;

         return $self->put_file(
            $localpath, $s3path,
            on_progress => sub { ( $slot->[2] ) = @_ },
         )->on_done( sub {
            $self->print_message( "DONE  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;

            @uploads = grep { $_ != $slot } @uploads;
            $timer->invoke_event( on_tick => ) if $timer;
         });
      })->on_done( sub { $recent_aborts = 0 } )
      ->else_with_f( sub {
         my ( $f ) = @_;

         $self->print_message( "ABORT $relpath" );
         $aborted_files += 1;
         $aborted_bytes += $size;

         $recent_aborts++;

         if( $recent_aborts >= 5 ) {
            # Too many recent failures; stop there
            return $f;
         }
         else {
            return Future->new->done( "ABORT" );
         }
      });
   } foreach => \@files,
     concurrent => $concurrent )->get;

   $self->remove_child( $timer ) if $timer;


   if( $aborted_files ) {
      $self->print_message( sprintf "ABORTED due to failure\n" .
         "  %d files (%d bytes) aborted\n" .
         "  %d files (%d transferred, %d skipped)\n  %d bytes (%d transferred, %d skipped)",
         $aborted_files, $aborted_bytes,
         $completed_files, $completed_files - $skipped_files, $skipped_files,
         $completed_bytes, $completed_bytes - $skipped_bytes, $skipped_bytes );
      exit 1;
   }
   else {
      $self->print_message( sprintf "All files done\n" . 
         "  %d files (%d transferred, %d skipped)\n  %d bytes (%d transferred, %d skipped)",
         $completed_files, $completed_files - $skipped_files, $skipped_files,
         $completed_bytes, $completed_bytes - $skipped_bytes, $skipped_bytes );
   }
}

sub cmd_pull
{
   my $self = shift;
   my ( $s3root, $localroot, %args ) = @_;

   # Trim trailing "/";
   s{/$}{} for $s3root, $localroot;

   my $concurrent = $args{concurrent} || FILES_AT_ONCE;
   my $skip_logic = $args{skip_logic} || "stat";
   my $filter = _make_filter_sub( $args{only}, $args{exclude} );

   my $s3root_data = _joinpath( "data", length $s3root ? ( $s3root ) : () );

   $self->print_message( "Listing files on S3..." );
   my ( $keys ) = $self->{s3}->list_bucket(
      prefix => $s3root_data,
      # no delimiter
   )->get;

   my $total_bytes = 0;
   my $total_files = 0;
   my @files;

   foreach ( @$keys ) {
      my $name = $_->{key};
      # Trim "data/$s3root" prefix
      $name =~ s{^\Q$s3root_data\E/}{};

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
   my $aborted_files   = 0;
   my $aborted_bytes   = 0;

   my @downloads;

   my $timer;
   if( $self->{progress} ) {
      $timer = $self->_start_progress_bulk( \@downloads, $total_files, $total_bytes, \$completed_files, \$completed_bytes, \$skipped_bytes );
   }

   my $recent_aborts = 0;

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      # Allow $s3root="" to mean download from root
      my $s3path    = _joinpath( grep { length } $s3root, $relpath );
      my $localpath = _joinpath( $localroot, $relpath );

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
         push @downloads, my $slot = [ $relpath, $size, 0 ];
         $timer->invoke_event( on_tick => ) if $timer;

         return $self->get_file(
            $s3path, $localpath,
            on_progress => sub { ( $slot->[2] ) = @_ },
            mkdir => 1,
         )->on_done( sub {
            $self->print_message( "DONE  $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;

            @downloads = grep { $_ != $slot } @downloads;
            $timer->invoke_event( on_tick => ) if $timer;
         });
      })->on_done( sub { $recent_aborts = 0 } )
      ->else_with_f( sub {
         my ( $f ) = @_;

         $self->print_message( "ABORT $relpath" );
         $aborted_files += 1;
         $aborted_bytes += $size;

         $recent_aborts++;

         if( $recent_aborts >= 5 ) {
            # Too many recent failures; stop there
            return $f;
         }
         else {
            return Future->new->done( "ABORT" );
         }
      });
   } foreach => \@files,
     concurrent => $concurrent )->get;

   $self->remove_child( $timer ) if $timer;

   if( $aborted_files ) {
      $self->print_message( sprintf "ABORTED due to failure\n" .
         "  %d files (%d bytes) aborted\n" .
         "  %d files (%d transferred, %d skipped)\n  %d bytes (%d transferred, %d skipped)",
         $aborted_files, $aborted_bytes,
         $completed_files, $completed_files - $skipped_files, $skipped_files,
         $completed_bytes, $completed_bytes - $skipped_bytes, $skipped_bytes );
      exit 1;
   }
   else {
      $self->print_message( sprintf "All files done\n" . 
         "  %d files (%d transferred, %d skipped)\n  %d bytes (%d transferred, %d skipped)",
         $completed_files, $completed_files - $skipped_files, $skipped_files,
         $completed_bytes, $completed_bytes - $skipped_bytes, $skipped_bytes );
   }
}

sub cmd_cmp
{
   my $self = shift;
   my ( $s3root, $localroot, %args ) = @_;

   my $concurrent = $args{concurrent} || FILES_AT_ONCE;
   my $filter = _make_filter_sub( $args{only}, $args{exclude} );

   # Determine the local files first by entirely synchronous operations
   my @localfiles;

   # BFS by stack
   my @stack = ( undef );
   while( @stack ) {
      my $relpath = shift @stack;

      my $localpath = _joinpath( grep { defined } $localroot, $relpath );

      $self->print_message( "Scanning $localpath..." );

      my @moredirs;
      foreach ( sort $self->freaddir( path => $localpath ) ) {
         next if $_ eq "." or $_ eq "..";

         my $ent = _joinpath( grep { defined } $relpath, $_ );

         my ( $type, $size ) = $self->fstat_type_size_mtime( path => "$localroot/$ent" );

         if( $type eq "d" ) {
            push @moredirs, $ent;
         }
         elsif( $type eq "f" ) {
            next unless $filter->( $ent );
            push @localfiles, [ $ent, $size ];
         }
      }

      unshift @stack, @moredirs;
   }

   my $s3root_data = _joinpath( "data", length $s3root ? ( $s3root ) : () );

   $self->print_message( "Listing files on S3..." );
   my ( $keys ) = $self->{s3}->list_bucket(
      prefix => $s3root_data,
      # no delimiter
   )->get;

   my $s3total_bytes = 0;
   my @s3files;

   foreach ( @$keys ) {
      my $name = $_->{key};
      # Trim "data/$s3root" prefix
      $name =~ s{^\Q$s3root_data\E/}{};

      next unless $filter->( $name );

      push @s3files, [ $name, $_->{size} ];
      $s3total_bytes += $_->{size};
   }

   # Need both lists of filenames sorted in the same order
   @localfiles = sort_by { $_->[0] } @localfiles;
   @s3files    = sort_by { $_->[0] } @s3files;

   my $trees_differ; # the sets of files in each root differ
   my $files_differ; # the contents or metadata of files differ

   my $done_files = 0;
   my $done_bytes = 0;

   my @compares;
   my $timer;

   # We'll presume that download from S3 is slower than read from local disk, so
   # display progress in terms of S3 bytes transferred
   if( $self->{progress} ) {
      $timer = $self->_start_progress_bulk( \@compares, (scalar @s3files), $s3total_bytes, \$done_files, \$done_bytes, undef );
   }

   ( fmap_void {
      my ( $localent, $s3ent ) = @{$_[0]};

      # One of these may not exist, but if both do they'll have the same path anyway
      my $relpath = ( $localent || $s3ent )->[0];

      if( !$localent ) {
         $self->print_message( "S3-ONLY    $relpath" );
         $done_files += 1;
         $done_bytes += $s3ent->[1];
         $timer->invoke_event( on_tick => ) if $timer;

         $trees_differ++;
         return Future->new->done;
      }
      elsif( !$s3ent ) {
         $self->print_message( "LOCAL-ONLY $relpath" );

         $trees_differ++;
         return Future->new->done;
      }

      my ( undef, $localsize ) = @$localent;
      my ( undef, $s3size    ) = @$s3ent;

      # Allow $s3root="" to mean download from root
      my $s3path    = _joinpath( grep { length } $s3root, $relpath );
      my $localpath = _joinpath( $localroot, $relpath );

      push @compares, my $slot = [ $relpath, $s3size, 0 ];

      $self->cmp_file( $localpath, $s3path,
         on_progress => sub { ( $slot->[2] ) = @_ },
      )->on_done( sub {
         my ( $diff ) = @_;

         if( $diff ) {
            $files_differ++;
            $self->print_message( sprintf "%-10s %s", uc $diff, $relpath );
         }
         else {
            $self->print_message( "OK         $relpath" );
         }

         $done_files += 1;
         $done_bytes += $s3size;

         @compares = grep { $_ != $slot } @compares;
         $timer->invoke_event( on_tick => ) if $timer;
      });
   } generate => sub {
      # none left - stop
      return unless @localfiles or @s3files;

      if( !@s3files or $localfiles[0][0] lt $s3files[0][0] ) {
         return [ shift @localfiles, undef ];
      }
      if( !@localfiles or $s3files[0][0] lt $localfiles[0][0] ) {
         return [ undef, shift @s3files ];
      }

      return [ shift @localfiles, shift @s3files ];
   }, concurrent => $concurrent )->get;

   $self->remove_child( $timer ) if $timer;

   if( $trees_differ ) {
      $self->print_message( "All done - trees DIFFER" );
      return 2;
   }
   elsif( $files_differ ) {
      $self->print_message( "All done - files DIFFER" );
      return 1;
   }
   else {
      $self->print_message( "All done - no differences found" );
      return 0;
   }
}

sub cmd_md5check
{
   my $self = shift;
   my ( $s3root, %args ) = @_;

   # Trim trailing "/";
   s{/$}{} for $s3root;

   my $concurrent = $args{concurrent} || FILES_AT_ONCE;
   my $filter = _make_filter_sub( $args{only}, $args{exclude} );

   my $s3root_data = _joinpath( "data", length $s3root ? ( $s3root ) : () );

   $self->print_message( "Listing files on S3..." );
   my ( $keys ) = $self->{s3}->list_bucket(
      prefix => $s3root_data,
      # no delimiter
   )->get;

   my $total_bytes = 0;
   my @files;

   foreach ( @$keys ) {
      my $name = $_->{key};

      # Trim "data/$s3root" prefix
      $name =~ s{^\Q$s3root_data\E/}{};

      next unless $filter->( $name );

      push @files, [ $name, $_->{size} ];
      $total_bytes += $_->{size};
   }

   $self->print_message( sprintf "Found %d files totalling %d bytes (%.1f MiB)",
      scalar @files, $total_bytes, $total_bytes / (1024*1024) );

   my $completed_files = 0;
   my $completed_bytes = 0;
   my $aborted_files   = 0;
   my $aborted_bytes   = 0;

   my @downloads;

   my $timer;
   if( $self->{progress} ) {
      $timer = $self->_start_progress_bulk( \@downloads, scalar @files, $total_bytes, \$completed_files, \$completed_bytes, \0 );
   }

   ( fmap_void {
      my ( $relpath, $size ) = @{$_[0]};

      # Allow $s3root="" to mean compare at from root
      my $s3path = _joinpath( grep { length } $s3root, $relpath );

      $self->print_message( "START $relpath" );
      push @downloads, my $slot = [ $relpath, $size, 0 ];
      $timer->invoke_event( on_tick => ) if $timer;

      return $self->_get_file_to_code( $s3path, sub {
         $slot->[2] += length $_[1] if defined $_[1];
         return;
      })->then( sub {
         Future->new->done( 1 )
      })->else_with_f( sub {
         my ( $f, $message, $op, @args ) = @_;
         return Future->new->done( 0 ) if $op eq "get_file" and $args[0] eq "md5sum";
         return $f;
      })->on_done( sub {
         my ( $ok ) = @_;

         if( $ok ) {
            $self->print_message( "OK    $relpath" );
            $completed_files += 1;
            $completed_bytes += $size;
         }
         else {
            $self->print_message( "BAD   $relpath" );
            $aborted_files += 1;
            $aborted_bytes += $size;
         }

         @downloads = grep { $_ != $slot } @downloads;
         $timer->invoke_event( on_tick => ) if $timer;
      });
   } foreach => \@files,
     concurrent => $concurrent )->get;

   $self->remove_child( $timer ) if $timer;

   if( $aborted_files ) {
      $self->print_message( "All done - md5sums DIFFER" );
      return 1;
   }
   else {
      $self->print_message( "All done - no differences found" );
      return 0;
   }
}

1;
