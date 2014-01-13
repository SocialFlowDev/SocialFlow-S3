use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
   module_name => 'SocialFlow::S3',
   dist_abstract => 'Amazon S3 tool for Socialflow',
   requires => {
      'Cwd' => 0,
      'Future' => '0.22', # ->done when cancelled bugfix
      'Future::Utils' => '0.22', # fmap_scalar
      'IO::Async::Notifier' => 0,
      'IO::Async::Loop' => 0,
      'IO::Async::Timer::Periodic' => 0,
      'IO::Termios' => 0,
      'Net::Async::HTTP' => '0.30', # NaWS:S3 needs 0.30
      'Net::Async::Webservice::S3' => '0.17',
      'POSIX::strptime' => 0,
      'Term::Size' => 0,
      'YAML' => 0,
   },
   build_requires => {
      'File::Temp' => 0,
      'Test::More' => '0.88', # done_testing
   },
   auto_configure_requires => 0, # Don't add M::B to configure_requires
   create_makefile_pl => 'traditional',
   create_readme  => 1,
);

$build->create_build_script;
