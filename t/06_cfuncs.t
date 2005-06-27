
use Test::More tests => 4;
use Seq;

$bers = pack("w*", 2, 3, 4, 5);

$len = Seq::next_integer_length($bers);
ok($len == 1, 'length calculation correct');

$val = Seq::next_integer_val($bers);
ok($val == 2, 'value conversion correct');

$len = Seq::next_integer_length('');
ok($len == 1, 'empty string length calculation is 1');

$val = Seq::next_integer_val('');
ok($val == 0, 'empty string value conversion is 0');



## sum_to_doc(string, dxsum, dxn, pos)
#$bers = pack("w*", 
#             3 => 0,   # docid 1 has >1 pos
#			 7 => 1,   # docid 4 has >1 pos
#			 2 => 27,  # docid 5 has 1 pos
#			 12 => 123,# docid 11 has 1 pos
#			 );
#($dxsum, $dxn, $px) = Seq::sum_to_doc($bers, 0, 0, 1);
#ok($dxsum == 1, 'dxsum is 1');
#ok($dxn == 2, 'dxn is 2');
#ok($px == 1, 'px val is 1'); # (0*2+1) translated
#
#($dxsum, $dxn, $px) = Seq::sum_to_doc($bers, 1, 2, 3);
#ok($dxsum == 4, 'dxsum is 4');
#ok($dxn == 4, 'dxn is 4');
#ok($px == 3, 'px val is 3'); # (1*2+1) translated
#
#($dxsum, $dxn, $px) = Seq::sum_to_doc($bers, 4, 4, 5);
#ok($dxsum == 5, 'dxsum is 5');
#ok($dxn == 6, 'dxn is 6');
#ok($px == 54, 'px val is 54'); # (27*2+0) translated
#
#($dxsum, $dxn, $px) = Seq::sum_to_doc($bers, 5, 6, 7);
#ok($dxsum == 11, 'dxsum is 11');
#ok($dxn == 8, 'dxn is 8');
#ok($px == 246, 'px val is 246'); # (123*2+0) translated
#
#($dxsum, $dxn, $px) = Seq::sum_to_doc($bers, 11, 8, 12);
#ok(!defined $dxsum, 'dxsum is undefined');





exit 0;

