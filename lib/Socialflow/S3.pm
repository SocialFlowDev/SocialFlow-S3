package Socialflow::S3;

use strict;
use warnings;
use base qw( IO::Async::Notifier );

use Future;
use IO::Async::Timer::Periodic;
use Net::Async::Webservice::S3 0.05;

use Digest::MD5;
use Fcntl qw( SEEK_SET );
use List::Util qw( max );
use List::MoreUtils qw( any );
use POSIX qw( ceil );
use Time::HiRes qw( time );

use constant PART_SIZE => 100*1024*1024; # 100 MiB

use constant FILES_AT_ONCE => 1; # TODO: Any higher an NaHTTP itself breaks :(

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
      prefix => "data/$prefix",
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

sub put_file
{
   my $self = shift;
   my ( $localpath, $s3path, %args ) = @_;
   my $on_progress = $args{on_progress};

   open my $fh, "<", $localpath or die "Cannot read $localpath - $!";

   my $len_total = -s $fh; # Total length of the file
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

   $self->{s3}->put_object(
      key    => "data/$s3path",
      gen_parts => $gen_parts,
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

   my $fh;
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

            if( !$fh ) {
               open $fh, ">", $localpath or die "Cannot write $localpath - $!";
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
      my ( $exp_md5sum ) = @_;

      my $got_md5sum = $md5->hexdigest;
      if( $exp_md5sum ne $got_md5sum ) {
         die "Expected MD5sum '$exp_md5sum', got '$got_md5sum'\n";
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

   my ( $prefix, $re ) = $self->_split_pattern( $s3pattern // "", 1 );

   my ( $keys, $prefixes ) = $self->{s3}->list_bucket(
      prefix => "data/$prefix",
      delimiter => "/",
   )->get;

   while( @$keys or @$prefixes ) {
      if( !@$prefixes or @$keys and $keys->[0]{key} lt $prefixes->[0] ) {
         my $e = shift @$keys;
         my $key = $e->{key};
         next if $re and $key !~ $re;

         $key =~ s{^data/}{};

         if( $LONG ) {
            printf "%-38s %15d %s\n", $key, $e->{size}, $e->{last_modified};
         }
         else {
            printf "%-38s\n", $key;
         }
      }
      elsif( !@$keys or @$prefixes and $prefixes->[0] lt $keys->[0]{key} ) {
         my $name = shift @$prefixes;
         next if $re and $name !~ $re;

         $name =~ s{^data/}{};

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
      key => "data/$s3path",
      gen_parts => $gen_parts,
   )->then( sub {
      $self->put_meta( $s3path, "md5sum", $md5->hexdigest . "\n" );
   })->get;

   $self->remove_child( $stdin );
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
         $progress_timer ||= $self->_start_progress( $_[1], \$len_so_far );
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
         $progress_timer ||= $self->_start_progress( $_[1], \$len_so_far );
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
   my ( $localroot, $s3root ) = @_;

   # Determine the list of files first by entirely synchronous operations
   my $total_bytes = 0;
   my @files;

   # BFS by stack
   my @stack = ( undef );
   while( @stack ) {
      my $relpath = shift @stack;
      my $localpath = join "/", grep { defined } $localroot, $relpath;

      print STDERR "Scanning $localpath...\n";
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
            my $bytes = -s _;
            push @files, [ $ent, $bytes ];
            $total_bytes += $bytes;
         }
      }

      unshift @stack, @moredirs;
   }

   my $total_files = scalar @files;

   printf STDERR "Found %d files totalling %d bytes (%.1f MiB)\n",
      $total_files, $total_bytes, $total_bytes / (1024*1024);

   my $completed_files = 0;
   my $completed_bytes = 0;

   my @upload_slots;

   $self->add_child( my $timer = IO::Async::Timer::Periodic->new(
      interval => 1,
      on_tick => sub {
         my $done_bytes = $completed_bytes;

         my $slotstats = join "\n", map {
            my ( $f, $localpath, $s3path, $total, $done ) = @$_;

            $done_bytes += $done;
            sprintf "  [%6d of %6d; %2.1f%%] %s", $done, $total, 100 * $done / $total, $s3path;
         } @upload_slots;

         printf STDERR "[%3d of %3d; %2.1f%%] [%6d of %6d; %2.1f%%]\n",
            $completed_files, $total_files, 100 * $completed_files / $total_files,
            $done_bytes,      $total_bytes, 100 * $done_bytes / $total_bytes;
         print STDERR "$slotstats\n";
      },
   )->start );

   while( @files or @upload_slots ) {
      # Start any more uploads that are pending
      while( @files and @upload_slots < FILES_AT_ONCE ) {
         my ( $relpath, $size ) = @{ shift @files };

         my $localpath = "$localroot/$relpath";

         # Allow $s3root="" to mean upload into root
         my $s3path    = join "/", grep { length } $s3root, $relpath;

         print STDERR "START $localpath => $s3path\n";

         push @upload_slots, my $slot = [ undef, $localpath, $s3path, $size, 0 ];

         my $f = $self->put_file(
            $localpath, $s3path,
            on_progress => sub { ( $slot->[4] ) = @_ },
         );
         $slot->[0] = $f;
      }

      # Await atleast one to be completed
      my @f = map { $_->[0] } @upload_slots;
      $f[0]->await until any { $_->is_ready } @f;

      # Expire complete ones
      for( my $idx = 0; $idx < @upload_slots; $idx++ ) {
         next if !$upload_slots[$idx]->[0]->is_ready;

         my ( $f, $localpath, $s3path, $size ) = @{ $upload_slots[$idx] };

         $f->get;

         print STDERR "DONE  $localpath => $s3path\n";
         $completed_files += 1;
         $completed_bytes += $size;

         splice @upload_slots, $idx, 1, ();
         $idx--;
      }
   }

   print STDERR "All files done\n";
   $self->remove_child( $timer );
}

1;
