package SocialFlow::S3::Crypt;

use strict;
use warnings;

use Carp;

use Crypt::Rijndael;
use Digest::MD5 'md5';

## Cipher::CFB is massively slow, as it operates on every byte individually.
## Cipher::Rijndael does have a CFB mode, but it doesn't support splitting
#    data over multiple calls (see https://rt.cpan.org/Ticket/Display.html?id=86385)
#
## So here we reimplement CFB mode aaaagain... *sigh*

sub new
{
   my $class = shift;
   my %args = @_;

   my $scheme = delete $args{scheme};
   my ( $cipher, $mode ) = $scheme =~ m/^(.+)-([^-]+)$/ or croak "Cannot parse scheme";

   # TODO - support other ciphers/modes
   $cipher eq "aes-256" or croak "Only 'aes-256' is currently supported as a cipher";
   $mode eq "cfb" or croak "Only cfb is currently supported as a mode";

   my $keysize   = Crypt::Rijndael->keysize;
   my $blocksize = Crypt::Rijndael->blocksize;

   # TODO: Salting? But then how to store the salt for retrieval later?
   my $key = substr( md5( $args{passphrase} ), 0, $keysize );

   my $iv = $args{iv} //
            ( $args{random_iv} && join "", map { chr rand 256 } 1 .. $blocksize ) or
            croak "Need an 'iv' or 'random_iv' set to true";

   my $self = bless {
      cipher => Crypt::Rijndael->new( $key, Crypt::Rijndael::MODE_ECB ),
      iv     => $iv,
      partial => "",
   }, $class;

   return $self;
}

sub iv
{
   my $self = shift;
   return $self->{iv};
}

sub encrypt
{
   my $self = shift;
   my ( $plaintext ) = @_;

   my $cipher    = $self->{cipher};
   my $blocksize = $cipher->blocksize;

   my $blocks = int( length($plaintext) / $blocksize );

   my $block = $self->{iv} or croak "Require IV";
   my $ciphertext = "";

   foreach my $i ( 0 .. $blocks-1 ) {
      my $pt_block = substr( $plaintext, $i*$blocksize, $blocksize );
      my $ct_block = $pt_block ^ $cipher->encrypt( $block );
      $ciphertext .= $ct_block;

      $block = $ct_block;
   }
   $self->{iv} = $block;

   if( my $partial_len = length($plaintext) - $blocks*$blocksize ) {
      # Final block is just truncated short
      my $pt_block = substr( $plaintext, $blocks*$blocksize );
      $ciphertext .= substr( $pt_block ^ $cipher->encrypt( $block ), 0, $partial_len );
      undef $self->{iv};
   }

   return $ciphertext;
}

# Decryption has to cope with merging blocks over read() boundaries
sub decrypt
{
   my $self = shift;
   my ( $ciphertext ) = @_;

   my $cipher    = $self->{cipher};
   my $blocksize = $cipher->blocksize;

   my $eof = !defined $ciphertext;
   $ciphertext = "" if $eof;

   if( length $self->{partial} ) {
      $ciphertext = $self->{partial} . $ciphertext;
      $self->{partial} = "";
   }

   my $blocks = int( length($ciphertext) / $blocksize );

   my $block = $self->{iv} or croak "Require IV";
   my $plaintext = "";

   foreach my $i ( 0 .. $blocks-1 ) {
      my $ct_block = substr( $ciphertext, $i*$blocksize, $blocksize );
      $plaintext .= $ct_block ^ $cipher->encrypt( $block ); # ->encrypt (sic)

      $block = $ct_block; # Cipher Feedback
   }

   $self->{iv} = $block;

   $self->{partial} = substr( $ciphertext, $blocks*$blocksize );

   if( $eof and length $self->{partial} ) {
      # Final block is just truncated short
      my $ct_block = substr( $ciphertext, $blocks*$blocksize );
      $plaintext .= substr( $ct_block ^ $cipher->encrypt( $block ), 0, length $ct_block );
      undef $self->{iv};
   }

   return $plaintext;
}

1;
