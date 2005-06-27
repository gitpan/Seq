

use Test::More tests => 1;
use Seq;


my $index = Seq->open_read( 'od01' );
my $of = Seq::isr('of');
my $the = Seq::isr('the');
my $gods = Seq::isr('gods');
$index->close_index();

# <[of the] gods>
my  $of_or_the_gods = 
  Seq::merge_seq(Seq::merge_or($of, $the), 
                 $gods, 1);

# [<of gods> <the gods>]
my $ofgods_or_thegods = 
  Seq::merge_or(Seq::merge_seq($of, $gods, 1), 
                Seq::merge_seq($the, $gods, 1));

is_deeply($of_or_the_gods, $ofgods_or_thegods, 
  'OR and AND consistency correct');

exit 0;



