use strict;
use warnings;
use lib '/probe';
use EvidencePrototype;
use JSON::PP ();
my @vectors=map{[EvidencePrototype::_fence([unpack('C*',pack('H*',$_))])]}('00112233445566778899aabbccddeeff','000102030405060708090a0b0c0d0e0f');
print JSON::PP->new->encode(\@vectors),"\n";
