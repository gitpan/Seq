

use Test::More tests => 2;
use Seq;

my $px0 = pack "w*", ( 1,5,4,2 );
my $px1 = pack "w*", ( 6,2,3,5,4,4 );
my($px, $count) = Seq::disjunctive_px_merge($px0, $px1, 0, 0);
ok($count == 9, 'count correct');
is_deeply(
  [ unpack "w*", $px  ],
  [ 1, 0,5,0, 2,0, 2,0, 1,0, 1,0, 4,0, 4,0, 4,0 ],
  'position list correct');

exit 0;


