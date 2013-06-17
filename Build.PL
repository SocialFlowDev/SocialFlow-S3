use strict;
use warnings;

use Module::Build;

my $build = Module::Build->new(
   module_name => 'Socialflow::S3',
   requires => {
      'Future' => '0.14', # fmap_void
      'IO::Async::Notifier' => 0,
      'IO::Async::Loop' => 0,
      'IO::Async::Timer::Periodic' => 0,
      'Net::Async::HTTP' => '0.22',
      'Net::Async::Webservice::S3' => '0.05',
   },
);

$build->create_build_script;
