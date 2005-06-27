
use Test::More tests => 5;
use Seq;

$index = Seq->open_read("od01");

# usage: sample(ndocs_or_ratio [, base_set_name]);
my $sample_id = $index->sample(10);
$result = $index->search($sample_id,0,20); # ask for 20
ok( scalar @{$result->[1]} == 10, 'corpus sample size correct');

$sample_id = $index->sample(10, 'the');
$result = $index->search($sample_id);
ok( scalar @{$result->[1]} == 10, 'base set sample size correct');

$result = $index->set_info($sample_id);
ok( $result->{ndocs} == 10, 'sample ndocs entry correct');

my $subsample_id = $index->sample(5, $sample_id);
$result = $index->search($subsample_id);
ok( scalar @{$result->[1]} == 5, 'base subsample size correct');


# test new result in-out functions
$result = $index->search('the');
$dup_result_id = $index->set_from_list( $result->[1] ); # <- destroys $result
$dup_result = $index->search($dup_result_id);
$result = $index->search('the'); # <- because $result is destroyed
is_deeply($result->[1], $dup_result->[1], 'result set in-out deep compare correct');

$index->close_index();

exit 0;

