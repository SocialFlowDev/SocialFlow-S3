use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
   module_name => 'Socialflow::S3',
   dist_abstract => 'Amazon S3 tool for Socialflow',
   dist_version => 0,
   requires => {
      'Crypt::Rijndael' => 0,
      'Future' => '0.14', # fmap_void
      'IO::Async::Notifier' => 0,
      'IO::Async::Loop' => 0,
      'IO::Async::Timer::Periodic' => 0,
      'Net::Async::HTTP' => '0.22',
      'Net::Async::Webservice::S3' => '0.09',
      'POSIX::strptime' => 0,
      'YAML' => 0,
   },
   auto_configure_requires => 0, # Don't add M::B to configure_requires
   create_makefile_pl => 'traditional',
   create_readme  => 1,
);

$build->create_build_script;
