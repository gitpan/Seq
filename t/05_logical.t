
use Test::More tests => 14;
use Seq;

my $index = Seq->open_read( 'testindex' );

my $result = $index->search('<way [we blabla]>');
is_deeply( $result, 
           [[ 'test_doc_2' =>  27, 1 ]],
           'valid + invalid words within alternation');

$result = $index->search('<way [hoohoo blabla]>');
is_deeply( $result, 
           [],
           'invalid alternation');

$result = $index->search('<the [only idiots] way>');
is_deeply( $result, 
           [[ test_doc_2 => 25, 2 ]],
           'valid nonmatching word plus matching word within alternation');

$result = $index->search('<the [idiots way] blabla>');
is_deeply( $result, 
           [],
           'invalid word in top sequence');

$result = $index->search('<the #w6 ever>');
is_deeply( $result, 
           [[ test_doc_2 => 25, 5 ]],
           'true #w search');

$result = $index->search('<the #w2 ever>');
is_deeply( $result, 
           [],
           'false #w search');

$result = $index->search('<the #t5 ever>');
is_deeply( $result, 
           [[ test_doc_2 => 25, 5 ]],
           'true #t search');

$result = $index->search('<the #t4 ever>');
is_deeply( $result, 
           [],
           'false #t search (too short)');

$result = $index->search('<the #t6 ever>');
is_deeply( $result, 
           [],
           'false #t search (too long)');


$index->close_index();




# some extras 

$index = Seq->open_read("od01");
$result = $index->search('<he gave his daughters>');
is_deeply( $result, 
           [[ './t/data/book10' => 160, 3 ]],
           'straight sequence');

$result = $index->search('<he [<handed his> <gave his>] daughters>');
is_deeply( $result, 
           [[ './t/data/book10' => 160, 3 ]],
           'alternation of sequences');


$result = $index->search('<he (<dainties innumerable> <gave his>) daughters>');
is_deeply( $result, 
           [[ './t/data/book10' => 160, 3 ]],
           'boolean AND within sequence');

$result = $index->search('<he gave his {hoohaaa}>');
is_deeply( $result, 
           [[ './t/data/book10' => 160, 3 ]],
           'boolean NOT within sequence (no point actually)');

$result = $index->search("(<he gave his daughters> {dainties})");
is_deeply( $result, 
           [],
           'boolean NOT within AND');

# Still get error on this
#$result = $index->search('(<he gave his daughters> {<innumerable dainties>})');
#is_deeply( $result, 
#           [[ './t/data/book10' => 160, 3 ]],
#           'SEQ within NOT within AND');
#

$index->close_index();

exit 0;

