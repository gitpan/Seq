

use Test::More tests => 2;
use Seq;


my $index = Seq->open_read( 'od01' );
my $the_and_gods = Seq::isr(($index->query( '(the gods)' ))[0]);
my $the_or_gods = Seq::isr(($index->query( '[the gods]' ))[0]);
my $the = Seq::isr('the');
my $gods = Seq::isr('gods');

my $the_gods_conjunct = Seq::merge_and($the, $gods, 0);
my $the_gods_disjunct = Seq::merge_or($the, $gods, 0);

$index->close_index();

is_deeply($the_gods_conjunct, 
          $the_and_gods, 'odyssey isr AND merge correct');

is_deeply($the_gods_disjunct, 
          $the_or_gods, 'odyssey isr OR merge correct');


exit 0;


