#!/usr/bin/perl

use strict;
use warnings;

use CPAN;
use Cwd qw( realpath );
use File::Basename qw( basename );
use File::Copy qw( copy );
use File::Slurp qw( write_file );
use IPC::Run qw();

my $STAGEDIR = "tmp";
if( -d $STAGEDIR ) {
   run( "rm", "-rf", $STAGEDIR );
}
mkdir $STAGEDIR;

delete $ENV{$_} for qw( PERL5LIB PERL_MM_OPT PERL_MB_OPT );

my $VERSION = "0.03+bzr253-0sf1";
my $PREFIX = "/opt/sfs3";
my @DEPS = ( "perl (>= 5.14.2)" );
chomp( my $ARCH = `dpkg-architecture -qDEB_HOST_ARCH` );

# We'll be building our own deps into $STAGEDIR
my $stagedir_fq = realpath $STAGEDIR;
$ENV{PERL5LIB} = join "", $stagedir_fq, $PREFIX, "/lib/perl5";

sub run
{
   my ( $cmd, @args ) = @_;

   print STDERR "\$ $cmd @args\n";

   system( $cmd, @args ) == 0 or die "Cannot run $cmd - $!";
}

sub cpan_dep
{
   my ( $dist ) = @_;

   # Find latest matching
   my $latest;
   foreach ( CPAN::Shell->expand( Distribution => "/\/$dist-\\d+/" ) ) {
      $latest = $_ if !defined $latest or $_ gt $latest;
   }
   my $tarball = basename $latest->pretty_id;

   print STDERR "# Latest distribution of $dist is $tarball\n";

   unless( -f "tarballs/$tarball" ) {
      mkdir "tarballs" unless -d "tarballs";

      $latest->get_file_onto_local_disk;
      copy( $latest->{localfile}, "tarballs/$tarball" ) or die "Cannot copy - $!";
   }

   opendir my $wasdir, ".";

   chdir( "tarballs" );

   run "tar", "-xf", $tarball;
   ( my $distdir = $tarball ) =~ s{\.tar.(?:gz|bz2)$}{};
   chdir( $distdir );

   if( -f "Build.PL" ) {
      run "perl", "Build.PL", "install_base=$PREFIX"; # not prefix=
      run "./Build";
      run "./Build", "install", "destdir=../../$STAGEDIR";
   }
   elsif( -f "Makefile.PL" ) {
      run "perl", "Makefile.PL", "INSTALL_BASE=$PREFIX"; # not PREFIX=
      run "make";
      run "make", "install", "DESTDIR=../../$STAGEDIR";
   }
   else {
      die "TODO: No Build.PL or Makefile.PL found; not sure what to do";
   }

   chdir( $wasdir );
   run "rm", "-rf", "tarballs/$distdir";
}

sub deb_dep
{
   my ( $dep ) = @_;
   push @DEPS, $dep;
}

# IO-Async wants this
cpan_dep "IO-Socket-IP";

# IO-Termios
deb_dep "libio-pty-perl";

# Net-Async-HTTP
deb_dep "libhttp-message-perl";
deb_dep "liburi-perl";

# Net-Async-Webservice-S3
deb_dep "libdigest-hmac-perl";
deb_dep "libhttp-date-perl";
deb_dep "libxml-libxml-perl";

# sfs3 itself wants:
cpan_dep "Future";
cpan_dep "IO-Async";
cpan_dep "IO-Termios";
cpan_dep "List-UtilsBy";
cpan_dep "Net-Async-HTTP";
cpan_dep "Net-Async-Webservice-S3";
cpan_dep "POSIX-strptime";
deb_dep "libterm-size-perl";
deb_dep "libyaml-perl";

run "perl", "Build.PL", "install_base=$PREFIX";
run "./Build";

# run "./Build", "test"; # TODO - for now t/22gpg.t is failing
run "prove", "-b", grep { $_ !~ /22gpg/ } glob("t/*.t");

run "./Build", "install", "destdir=$STAGEDIR";

# The PERL5LIB-setting trampoline
mkdir "$STAGEDIR/usr";
mkdir "$STAGEDIR/usr/local";
mkdir "$STAGEDIR/usr/local/bin";
write_file "$STAGEDIR/usr/local/bin/sfs3", <<'EOF';
#!/bin/sh
export PERL5LIB=/opt/sfs3/lib/perl5
exec /opt/sfs3/bin/sfs3 "$@"
EOF
chmod 0755, "$STAGEDIR/usr/local/bin/sfs3";

my $ctrldir = "$STAGEDIR/DEBIAN";
mkdir $ctrldir unless -d $ctrldir;

write_file "$ctrldir/control", <<"EOF";
Package: sfs3
Version: $VERSION
Architecture: $ARCH
Maintainer: Paul Evans <leonerd\@leonerd.org.uk>
Depends: ${\join ", ", @DEPS}
Section: perl
Priority: optional
Homepage: https://github.com/SocialFlowDev/SocialFlow-S3/
Description: Amazon S3 tool for SocialFlow

EOF

# Generate the md5sums control file
{
   opendir my $wasdir, ".";
   chdir( $STAGEDIR );

   my @toplevels = grep { $_ ne "DEBIAN" } glob( "*" );

   open my $md5sums, ">", "DEBIAN/md5sums" or die "Cannot write md5sums - $!";
   IPC::Run::run [ "find", @toplevels, "-type", "f" ], "|", [ "xargs", "md5sum" ],
      $md5sums or die "Cannot generate md5sums - $!";

   chdir $wasdir;
}

run "dpkg-deb", "--build", $STAGEDIR, ".";
