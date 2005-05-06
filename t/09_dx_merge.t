

use Test::More tests => 2;
use Seq;


my $index = Seq->open_read( 'od01' );
my $the_and_gods = Seq::isr($index->query( '(the gods)' ));
my $the_or_gods = Seq::isr($index->query( '[the gods]' ));
my $the = Seq::isr('the');
my $gods = Seq::isr('gods');
$index->close_index();

my $the_gods_conjunct = 
  Seq::conjunctive_dx_merge($the, $gods, 
                            \&Seq::disjunctive_px_merge);
my $the_gods_disjunct = Seq::disjunctive_dx_merge($the, $gods);

is_deeply($the_gods_conjunct, $the_and_gods, 'odyssey isr AND merge correct');

is_deeply($the_gods_disjunct, $the_or_gods, 'odyssey isr OR merge correct');


exit 0;


