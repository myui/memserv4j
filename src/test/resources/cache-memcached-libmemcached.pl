use Cache::Memcached::libmemcached;
use warnings;
use diagnostics;
use Data::Dumper;

local $| = 1;

$args->{servers} = [ "192.168.142.144:11211" ];
$args->{binary_protocol} = 1;
$args->{no_block} = 1;
$memd = Cache::Memcached::libmemcached->new($args);

print "before set my_key\n";
$memd->set("my_key", "Some value");
print "after set my_key\n";

sleep(3);

print "before set object_key\n";
$memd->set("object_key", { 'complex' => [ "object", 2, 4 ]});
print "after set object_key\n";

sleep(3);

print "before get my_key\n";
$val1 = $memd->get("my_key");
print "$val1\n";

sleep(3);

print "before get object_key\n";
$val2 = $memd->get("object_key");
warn Dumper $val2;

print "finish\n";

exit;
