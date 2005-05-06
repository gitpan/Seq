
use Test::More tests => 2;
use Seq;
no warnings;

my $i = Seq->open_read("od01");

my @query = qw{ < [ by with ] the #w5 gods > };
my ($tree) = Seq::aq(@query);
my $result = Seq::eval_query(@$tree);

my $by   = Seq::isr('by');
my $with = Seq::isr('with');
my $the  = Seq::isr('the');
my $gods  = Seq::isr('gods');

my $control = 
  Seq::new_seq(
    Seq::new_seq(
      Seq::new_or($by, $with),
      $the, 1),
    $gods, 5);

is_deeply($result, $control, 'compile and eval correct');


@query = qw{ < he [ < handed his > < gave his > ] daughters > };
($tree) = Seq::aq(@query);
$result = Seq::eval_query(@$tree);

my $he   = Seq::isr('he');
my $handed   = Seq::isr('handed');
my $his   = Seq::isr('his');
my $gave   = Seq::isr('gave');
my $daughters   = Seq::isr('daughters');

$control = 
  Seq::new_seq(
    Seq::new_seq( 
      $he,
      Seq::new_or( Seq::new_seq($handed, $his, 1),
                   Seq::new_seq($gave, $his, 1) ), 
      1),
    $daughters, 
    1);

is_deeply($result, $control, 'compile and eval correct for alternation of sequences');

