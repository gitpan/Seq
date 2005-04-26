package Seq;

require 5.005_62;
use strict;
use warnings;
no warnings "recursion";
use vars qw( $VERSION );

use FileHandle;
use CDB_File;
use Digest::MD5 qw{ md5 };

# Constants for data about each word.
use constant NDOCS  => 0;
use constant NWORDS => 1;
use constant DX     => 2; # doc index
use constant PX     => 3; # position index
use constant LASTDOC => 4;

$VERSION = '0.25';
use Inline Config =>
            VERSION => '0.25',
            NAME => 'Seq';
use Inline 'C';




sub open_write {
    my $type = shift;
    my $path = shift; # Name of index (directory).

    my $self = {};

    if( -e "$path/conf" ){

        # Read in conf file.
        $self = _configure($path);
        # IMPORTANT conf variables: seg_max_words, nsegments


        $self->{name} = $path;
        $self->{seg_nwords} = 0;
        $self->{seg_ndocs} = 0;
        $self->{isrs} = {}; 
        $self->{mode} = 'WRONLY';
    }
    else {    # entirely new index
        # Set up a default configuration.

        mkdir $path;
        $self = {
            mode => 'WRONLY',
            name => $path,
            nsegments => 0,
            nwords => 0,
            ndocs => 0,
            seg_max_words => 5 * 1024 * 1024, # 5 million words
            isrs => {},
            seg_nwords => 0,
            seg_ndocs => 0,
            ids => [],
            # What else?
        };
        index_document($self, "initial", "abcdefghijklmnopqrstuvwxyz");
    }

    return bless $self, $type;
}

sub open_read {
    my $type = shift;
    my $path = shift;

    my $self = {};

    if( -e "$path/conf" ){
        $self = _configure($path);

        my %cdb;
        tie %cdb, 'CDB_File', "$path/0/CDB" or die $!;
        $self->{cdb} = \%cdb;

        open(IDS, "<$path/0/ids") or die $!;
        chomp( @{ $self->{ids} } = <IDS> );
        close IDS;

        $self->{xids} = 
            { map { $self->{ids}->[$_] => $_ } 
                0..scalar @{$self->{ids}}-1 };

        $self->{name} = $path;
        $self->{isrs} = {};
        $self->{mode} = 'RDONLY';
    }
    else {
        warn "Index $path does not exist\n";
        return undef;
    }

    isrcache_init($self->{cdb});
    return bless $self, $type;
}



sub close_index {
    my $self = shift;

    if( $self->{mode} eq 'WRONLY' ){
        $self->_write_segment();
    }
    else {
        $self->DESTROY;
    }

    return 1;
}

sub DESTROY {
    my $self = shift;
    untie %{ $self->{cdb} };
    @{ $self->{ids} } = ();
    $self = undef;
    return 1;
}

sub tokenize_std {
    my $doc = shift;
    $doc =~ s|<[^>]+>||g; # Get rid of all tags.
    $doc =~ s|(\d)| $1 |g; # Count each digit.
    $doc = lc $doc;
    return split(m|[\W_]+|, $doc);
}




sub _read_isr {
    my $cdb = shift;
    my $word = shift;
    my $isr = new_isr();
    #return $isr unless exists $cdb->{$word};
    my $ISR = $cdb->{$word};

    my($ndocs, $nwords, $lastdoc, $dxlen) = unpack "L4", $ISR;
    substr($ISR, 0, 16) = '';

    #print STDERR "position = $pos, length = $length\n";
    $isr->[NWORDS] = $nwords;
    $isr->[NDOCS] = $ndocs;
    $isr->[LASTDOC] = $lastdoc;
    $isr->[DX] = substr($ISR, 0, $dxlen);
    substr($ISR, 0, $dxlen) = '';
    my $pxrunlen_len = unpack "L", $ISR;
    substr($ISR, 0, 4) = '';
    my @pxrunlens = unpack "w*", substr($ISR, 0, $pxrunlen_len);
    substr($ISR, 0, $pxrunlen_len) = '';
    my @PX;
    while(@pxrunlens){
        my $runlen = shift @pxrunlens;
        my $PX = substr($ISR, 0, $runlen);
        substr($ISR, 0, $runlen) = '';
        push @PX, $PX;
    }
    $isr->[PX] = \@PX;

    return $isr;
}


sub index_document {
    my $self = shift;
    my $doc_name = shift;
    my $document = shift;
    my $isrs = $self->{isrs};
    my $docid = $self->{seg_ndocs}++;
    my $position = 0;
    my %seen = (); # words in this doc -> list of position deltas
    my %last = (); # words in this doc -> last position seen
    
    push @{ $self->{ids} }, $doc_name;

    # Record term position deltas
    for my $word ( split /\W+/, $document ){
        $position++;
        push @{ $seen{$word} }, 
            $position - (exists $last{$word} ? $last{$word} : 0);
        $last{$word} = $position;
    }

    while(my($word, $deltas) = each %seen){
        $isrs->{$word} = new_isr() unless exists $isrs->{$word};
        my $isr = $isrs->{$word};
        my $docdelta = $docid - $isr->[LASTDOC];
        $isr->[LASTDOC] = $docid;
        $isr->[NDOCS]++;
        $isr->[NWORDS] += scalar @$deltas;
        if( @$deltas == 1 ){
            $isr->[DX] .= 
                pack("w*",
                    2 * $docdelta,       # 
                    $deltas->[0]); # the single position delta
        }
        else { # term count > 1
            $deltas = pack "w*", @$deltas;
            $isr->[DX] .= 
                pack("w*", 
                2 * $docdelta + 1, # doc delta*2, +1 for >1 term count in doc
                scalar @{ $isr->[PX] }); # index to position deltas in PX
            push @{ $isr->[PX] }, $deltas;
        }
    }

    $self->{seg_nwords} += $position;
    
    if( $self->{seg_nwords} >= $self->{seg_max_words} ){
        $self->_write_segment();
    }
    return $position;
}

sub new_isr {
    my $isr = [];
    $isr->[NDOCS] = 0;
    $isr->[NWORDS] = 0;
    $isr->[DX] = '';
    $isr->[PX] = [];
    $isr->[LASTDOC] = 0;
    return $isr;
}



sub _write_segment {
    my $self = shift;
    my $path = $self->{name};
    my $nsegments = $self->{nsegments}++;
    my $isrs = $self->{isrs};
    my $count = 0;
   
    mkdir "$path/$nsegments";
    my $new = new CDB_File("$path/$nsegments/NEW", 
                            "$path/$nsegments/CDB.tmp") or 
        die "$0: new CDB_File failed: $!\n";

    my @words = keys %$isrs;
    for my $word ( @words ){

        if( (++$count) % 50 == 0 ){
            print STDERR chr(13), "(", $count, ")\t\t";
        }

        $new->insert($word, _serialize_isr($isrs->{$word}));
        delete $isrs->{$word};
    }

    %{ $isrs } = ();

    $new->finish or die $!;

    rename "$path/$nsegments/NEW", "$path/$nsegments/CDB";

    open IDS, ">$path/$nsegments/ids";
    print IDS "$_\n" for @{$self->{ids}};
    close IDS;
    @{ $self->{ids} } = ();

    # segment conf
    open CONF, ">$path/$nsegments/conf";
    print CONF "seg_nwords:", $self->{seg_nwords}, "\n";
    print CONF "seg_ndocs:", $self->{seg_ndocs}, "\n";
    close CONF;


    # top level conf
    $self->{nwords} += $self->{seg_nwords};
    $self->{ndocs} += $self->{seg_ndocs};

    open CONF, ">$path/conf";
    binmode CONF;
    print CONF 'seg_max_words:', $self->{seg_max_words}, "\n";
    print CONF "# DO NOT EDIT BELOW THIS LINE\n";
    print CONF 'nwords:', $self->{nwords}, "\n";
    print CONF 'nsegments:', $self->{nsegments}, "\n";
    print CONF 'ndocs:', $self->{ndocs}, "\n";
    close CONF;

    $self->{seg_nwords} = 0;
    $self->{seg_ndocs} = 0;

    return "\nwrote segment with $count isrs.\n";
}

sub _serialize_isr {
    my $isr = shift;

    my $newisr = '';
    $newisr .= pack "L", $isr->[NDOCS];   # num of docs
    $newisr .= pack "L", $isr->[NWORDS];  # num of positions.
    $newisr .= pack "L", $isr->[LASTDOC]; # last document with this word

    $newisr .= pack "L", length $isr->[DX]; # runlength
    $newisr .= $isr->[DX]; 

    my $runlengths = pack "w*", map {length $_} @{ $isr->[PX] };
    $newisr .= pack "L", length $runlengths;
    $newisr .= $runlengths;
    $newisr .= join '', @{ $isr->[PX] }; # BER delta lists

    return $newisr;
}


sub _configure {
    my $path = shift;
    my $self = {};    

    # File "conf" contains seg_max_words, nwords.

    open CONF, "<$path/conf" or die $!;
    while(<CONF>){
        next if m|^#|;
        chomp;
        my ($key, $value) = split m|:|;
        $self->{$key} = $value;
    }
    close CONF;

    return $self;
}



sub optimize_index {
    my $path = shift;

    my @dirs = sort { $a <=> $b }
               grep { /^\d+$/ }
               map { s/^.+?\/(\d+)$/$1/; $_ }
               glob("$path/*");

    print STDERR "Compacting segments ", join(" ", @dirs), "\n";

    # gather necessary info for each segment
    my (@segments, %words);
    for my $segment (@dirs){
        my $conf = _configure("$path/$segment");
        my %cdb;
        tie %cdb, 'CDB_File', "$path/$segment/CDB";
        my %localwords = ();
        $localwords{$_} = 1 for grep {length($_) < 26} keys %cdb;
        $words{$_} = 1 for keys %localwords;
        print STDERR "Gathered ", scalar keys %words, " words at segment $segment.           \r";
        push @segments, [ $conf, \%cdb, "$path/$segment", \%localwords ];
    }


    # new consolidated index segment
    mkdir "$path/NEWSEGMENT";

    # create new cdb
    print STDERR "Creating new compacted segment.                        \n";
    my $newidx = new CDB_File("$path/NEWSEGMENT/NEW", 
                            "$path/NEWSEGMENT/CDB.tmp") or 
        die "$0: new CDB_File failed: $!\n";

    my $ntokens = scalar keys %words;
    my $t0 = time;
    while(my($word, undef) = each %words){
        $ntokens--;
        if(time-$t0 > 2){
            print STDERR "Compacting $ntokens th word: $word                       \r";
            $t0 = time;
        }
        my $isr = new_isr();
        my $ndocs = 0;
        for my $segment (@segments){
            my($conf, $cdb, undef, $localwords) = @$segment;
            if(exists $localwords->{$word}){ 
                $isr = _append_isr($isr, $ndocs, _read_isr($cdb, $word));
            }
            $ndocs += $conf->{seg_ndocs};
        }
        $newidx->insert($word, _serialize_isr($isr));
    }

    # write ids file, delete older segments
    open IDS, ">$path/NEWSEGMENT/ids";
    for my $segment (@segments){
        print STDERR "Writing document ids for segment ", $segment->[2], ", deleting segment dir.\r";
        open SEGIDS, $segment->[2] . "/ids"; # path to ids file
        while(<SEGIDS>){
            print IDS $_;
        }
        unlink glob($segment->[2] . "/*");
        rmdir $segment->[2];
    }

    print STDERR "\nWriting disk hash.\n";
    $newidx->finish;
    rename "$path/NEWSEGMENT/NEW", "$path/NEWSEGMENT/CDB";

    my ($nwords, $ndocs);
    $nwords += $_->[0]->{seg_nwords} for @segments;
    $ndocs += $_->[0]->{seg_ndocs} for @segments;

    open CONF, ">$path/NEWSEGMENT/conf";
    print CONF "seg_nwords:", $nwords, "\n";
    print CONF "seg_ndocs:", $ndocs, "\n";
    close CONF;

    my $conf = _configure($path);
    open CONF, ">$path/conf";
    binmode CONF;
    print CONF 'seg_max_words:', $conf->{seg_max_words}, "\n";
    print CONF "# DO NOT EDIT BELOW THIS LINE\n";
    print CONF "nsegments:1\n";
    print CONF "nwords:", $nwords, "\n";
    print CONF "ndocs:", $ndocs, "\n";
    close CONF;


    rename "$path/NEWSEGMENT", "$path/0";
}


sub _append_isr {
    my($isr0, $seg0ndocs, $isr1) = @_;
    return $isr0 unless $isr1->[NDOCS];

    # convert from ber-string to integer array
    my @isr0dx = unpack "w*", $isr0->[DX]; $isr0->[DX] = \@isr0dx;
    my @isr1dx = unpack "w*", $isr1->[DX]; $isr1->[DX] = \@isr1dx;

    # adjust first doc delta to previous segment's doc count
    my $adjust = ($seg0ndocs - $isr0->[LASTDOC]);
    my $ptrflag = $isr1->[DX]->[0] % 2;
    my $delta0 = int($isr1->[DX]->[0] / 2);
    $isr1->[DX]->[0] = ($delta0 + $adjust) * 2 + $ptrflag;

    # adjust px ptrs to previous isr's PX length
    my $pxn = scalar @{ $isr0->[PX] };
    while(@{ $isr1->[DX] }){
        my $delta = shift @{ $isr1->[DX] };
        my $pxptr = shift @{ $isr1->[DX] };
        if($delta % 2){ # odd, means pxptr is a PX array index
            $pxptr += $pxn;
        }
        push @{ $isr0->[DX] }, $delta, $pxptr;
    }
    push @{ $isr0->[PX] }, @{ $isr1->[PX] };
    $isr0->[NDOCS] += $isr1->[NDOCS];
    $isr0->[NWORDS] += $isr1->[NWORDS];
    $isr0->[LASTDOC] = $seg0ndocs + $isr1->[LASTDOC];

    # convert from integer array back to ber-string
    $isr0->[DX] = pack "w*", @{ $isr0->[DX] };
    $isr1->[DX] = pack "w*", @{ $isr1->[DX] };

    return $isr0;
}







# ISR CACHING

# Isrs are cached in a closure initialized with the cdb disk hash
{
    my %isrs = ();
    my %nrequests = ();
    my %timestamp = ();
    my $cdb = {};
    my %ids = (); # cached terms -> ids
    my %terms = (); # cached ids -> terms
    my $tid = 0; # term id counter

    # this is called by the open_read function to 
    # instantiate the cdb object. That way only 
    # the individual word is necessary to call isr().
    sub isrcache_init {
        $cdb = shift;
    }

    # return the isr for a term or id
    sub isr {
        my ($word, $isr) = @_;
        return ($isrs{id($word)} = $isr) if defined $isr; # set

        # return the empty isr if no occurrence.
        return new_isr() unless (exists $isrs{$word}) or 
                                (exists $cdb->{$word});
        $nrequests{$word}++;
        $timestamp{$word} = time;
        return $isrs{$word} if exists $isrs{$word};

        # About to add to in-memory isr cache, so
        # remove the top 10 isrs according to least-requested and 
        # least-recently requested, if limit reached
        if(10000 < scalar keys %isrs){
            my $time = time;
            my @words = sort { $a <=> $b } 
                        map { $nrequests{$_} / ($time - $timestamp{$_}) }
                        keys %isrs;
            delete $isrs{$_} for @words[0..9];
        }

        $isr = _read_isr($cdb, $word);
        $isrs{$word} = $isr;
        return $isr;
    }

    # replace query or sub-parts with cached result ids
    sub transcribe_query {
        my $term = shift;
        for my $subterm (sort {$isrs{$ids{$a}}->[NDOCS] <=> 
                               $isrs{$ids{$b}}->[NDOCS]} keys %ids){
            my $i = index($term, $subterm);
            if($i>-1){
                substr($term, $i, length($subterm)) = 
                    $ids{$subterm};
                last;
            }
        }
        return $term;
    }

    sub next_tid {
        return '_' . $tid++ . '_';
    }

    # return an id to a multi-term query, generating new one if necessary
    sub id {
        my $term = shift;
        return $term unless $term =~ / /; # no single-word searches
        $term =~ s/_\d+_/$terms{$&}/g; # retranslate to full term
        if(!exists $ids{$term}){
            $ids{$term} = next_tid;
            $terms{$ids{$term}} = $term;
        }
        return $ids{$term};
    }

    # list result sets
    sub set_list {
        my @terms = map { [ $_ => $terms{$_} ] } keys %terms;
        return \@terms;
    }

    sub set_delete {
        my(undef, $id) = @_;
        if(exists $terms{$id}){
            delete $ids{$terms{$id}};
            delete $terms{$id};
            delete $isrs{$id};
        }
        return 1;
    }
}




# CREATING SAMPLES

# creates a sample of documents based on either the whole corpus 
# or a subset. Inserts sample into cache, returns the identifier.
sub sample {
    my ($self, $r, $base) = @_;
    my $N = $self->{ndocs}; # whole corpus
    my $setid = next_tid;

    if(!defined $base){ # sample from whole corpus?
        isr($setid, _list_isr(_sample($r, $N)));
        return $setid;
    }

    $base = isr($self->query($base)); # base can be a query or docset
    $N = $base->[NDOCS];
    my @nth = @{ _sample($r, $N) };
    my $isr = new_isr();
    $isr->[NDOCS] = scalar @nth;

    my $docsum = 0;
    my $lastdoc = 0;
    my @DX = unpack "w*", $base->[DX];
    my $nth = 0;
    while(@DX){
        last unless @nth;
        my $delta = shift @DX;
        my $ptr = shift @DX;
        $docsum += int($delta/2);
        next unless $nth++ == $nth[0];
        shift @nth;
        if($delta % 2){
            $isr->[DX] .= pack("w*", 
                               ($docsum-$lastdoc) * 2 + 1, 
                               scalar @{ $isr->[PX] });
            push @{ $isr->[PX] }, $base->[PX]->[$ptr];
            $isr->[NWORDS]++ for unpack("w*", $base->[PX]->[$ptr]);
        }
        else {
            $isr->[DX] .= pack("w*",
                               ($docsum-$lastdoc) * 2,
                               $ptr);
            $isr->[NWORDS]++;
        }
        $lastdoc = $docsum;
    }
    isr($setid, $isr);
    return $setid;
}


# returns a docset (sample-isr) from a sorted list of internal docids
sub _list_isr {
    my $list = shift;
    my $isr = new_isr();
    $isr->[PX] = [ pack("w*", 0, 0) ]; # one pos+runlen serves for all
    my $lastdoc = 0;

	while(@$list){
        my $docid = shift @$list;
        $isr->[NDOCS]++;
        $isr->[DX] .= pack("w*", ($docid-$lastdoc)*2+1, 0);
        $lastdoc = $docid;
    }
    return $isr;
}

# creates a list of sorted integers in the range of n
sub _sample {
    my ($r, $N) = @_;
    return [0..$N-1] if $r > $N;

    my $pop = $N;
    my @ids = ();
    for my $samp (reverse 1..$r){
        my $cumprob = 1.0;
        my $x = rand;
        while($x < $cumprob){
            $cumprob -= $cumprob * $samp / $pop;
            $pop--;
        }
        push @ids, $N-$pop-1;
    }
    return \@ids;
}


# SEARCHING

# finds docs with search terms in logical relationship indicated in 
# query. 
sub search {
    my $self = shift;
    my ($query, $offset, $runlen) = @_;

    return $self->set_slice(
               $self->query($query), 
               $offset, 
               $runlen);
}

# return a result isr id from a query string. Side effect: caches
# query and result set.
sub query {
    my ($self, $query) = @_;
    my @qtokens = split(/ /, 
                      transcribe_query(
                          canonical($query)));
    return $qtokens[0] if @qtokens == 1;
    my ($aligner, $matcher) = cq(@qtokens);
    my $canonical = join(" ", @qtokens);
    isr($canonical, _execute_query($aligner, $matcher)); # add to isr cache
    return id($canonical);
}


sub canonical {
    my $qstr = shift;
    $qstr =~ s/[\[\]\|\<\>\(\)\{\}]/ $& /g;
    $qstr =~ s/\s+(#[wWtT]\d+)/$1/g;
    $qstr =~ s/^\s+//g;
    $qstr =~ s/\s+$//g;
    $qstr =~ s/\s+/ /g;
    return $qstr;
}


# Search results are document sets represented as a 'result_isr'.
# 'result_isr' is a slightly different format than a regular "term" isr.
# PX contains all PX lists, even if there is only one in a document.
# DX is the same format, only it always has an index to PX list. 
# PX has two numbers per position: the delta, and a runlength of the match.
sub _execute_query {
    my($aligner, $matcher) = @_;
    my $isr = new_isr();

    my $docid = 1;
    my $lastdoc = 0;
    my $nextdoc = 0;
    while(my $nextdoc = $aligner->($docid)){
        unless($docid == $nextdoc){
            $docid = $nextdoc;
            next;
        }
        my ($pos0, $posN) = (0, 0);
        my $lastpos = 0;
        my $px = '';
        while(($pos0, $posN) = $matcher->($pos0, $posN)){
            last if $pos0 < 0;
            $isr->[NWORDS]++;
            $px .= pack("w*", $pos0-$lastpos, $posN-$pos0); # delta, runlen
            $lastpos = $pos0;
        }
        if($px){
            $isr->[NDOCS]++;
            $isr->[DX] .= pack("w*", 
                           ($docid - $lastdoc)*2+1, # always has px list
                            scalar @{ $isr->[PX] }); 
            push @{ $isr->[PX] }, $px;
            $lastdoc = $docid;
        }
        $docid++;
    }

    return $isr;
}



# MANIPULATING SETS (isrs)

# Returns a partial result set from a term-, result-, or sample-isr.
# Format is :
# [ [ docid, match position, (runlength,) match position, (runlength) ...]
#   [ docid, match position, (runlength,) match position, (runlength) ...]
#   ... ]
sub set_slice {
    my($self, $isr, $offset, $length) = @_;
    $isr = isr($isr) unless ref $isr; # id or reference?
    $offset ||= 0;
    $length ||= $isr->[NDOCS]+1;

    my @results = ();
    my @DX = unpack "w*", $isr->[DX];
    my $docsum = 0;
    while(@DX){
        my $delta = shift @DX;
        my $ptr = shift @DX;
        $docsum += int($delta/2);
        next unless ($offset-- <= 0);
        last if ($length-- <= 0);
        my @docresult = ($self->{ids}->[$docsum]);
        push @docresult, ($delta % 2) ?
                         unpack "w*", $isr->[PX]->[$ptr] :
                         $ptr;
        push @results, \@docresult;
    }
    return \@results;
}

# Returns a result set from a sorted list of external ids [plus positions]
# (same format that is returned from search())
sub set_from_list {
    my($self, $list) = @_;
    my $isr = new_isr();
    my $lastdoc = 0;
    $list = $self->xid_iid($list); # xlate into internal ids (in place)
	
    while(@$list){
        my $doc = shift @$list;
        my $docid = shift @$doc;
        $isr->[NDOCS]++;
        $isr->[DX] .= pack("w*", 
                          ($docid-$lastdoc)*2+1, 
						  scalar @{$isr->[PX]} );
        push @{ $isr->[PX] }, (@$doc ? pack("w*", @$doc) : (0,0));
        $doc = []; # Destroy! Raaa!
        $lastdoc = $docid;
    }
    my $setid = next_tid;
    isr($setid, $isr);
    return $setid;
}


# return some basic info
sub set_info {
    my ($self, $isr) = @_;
    $isr = isr($isr) unless ref $isr; # id or reference?

    return { ndocs => $isr->[NDOCS],
             nwords => $isr->[NWORDS], };
}

# Translate a result list to sorted internal ids
sub xid_iid {
    my($self, $list) = @_;

    $_->[0] = $self->{xids}->{$_->[0]} for @$list; # xlate
    my @new = sort { $a->[0] <=> $b->[0] } @$list;
    return \@new;
}




# SET and SEQUENCE OPERATORS closure
# Return proper aligners/matchers/caps according to operator.
# This is the place to add new operators.
{
    my %aligners = (
        '<' => \&seq_aligner,
        '[' => \&or_aligner,
        '(' => \&seq_aligner,
        '{' => \&not_aligner,
    );

    my %matchers = (
        '<' => \&seq_matcher,
        '[' => \&or_matcher,
        '(' => \&and_matcher,
        '{' => \&not_matcher,
    );

    # "caps" are code refs that end chains
    my %caps = (
        '>' => [ sub { return $_[0] },          # sequence align
                 sub { return $_[0], $_[0] } ], # sequence match
        ']' => [ sub { return undef },          # boolean OR align
                 sub { return () } ],           # boolean OR match
        ')' => [ sub { return $_[0] },          # boolean AND align
                 sub { return (-1, -1) } ],     # boolean AND match
        '}' => [ sub { return $_[0] },          # boolean NOT align
                 sub { return $_[0], $_[0] } ], # boolean NOT match
     );

    my %ops = (
        '<' => '>',
        '[' => ']',
        '(' => ')',
        '{' => '}',
    );


    sub aligner {
        my $op = shift;
        return $aligners{$op}->(@_);
    }

    sub matcher {
        my $op = shift;
        return $matchers{$op}->(@_);
    }

    sub cap {
        my $op = shift;
        return @{ $caps{substr $op,0,1} } 
            if exists $caps{substr $op,0,1};
        return undef;
    }

    sub balanced {
        my($left, $right) = @_;
        return $ops{$left} 
            if exists $ops{$left} 
               and ($ops{$left} eq substr($right,0,1));
    }

    sub balance {
        my $left = shift;
        return $ops{$left} if exists $ops{$left};
        return undef;
    }
}

# Compile Query
sub cq {
    return undef unless @_; # empty list
    my ($op, @rest) = @_;

    my ($align, $nalign, $match, $nmatch, $interval);

    if( balanced($op, $rest[0]) ){ # this chain is done
        ($interval) = parse_interval($rest[0]);
        return cap(shift @rest), $interval, @rest;
    }
    elsif( balance($rest[0]) ){ # new chain
        ($align, $match, $interval, @rest) = cq(shift @rest, @rest);
    }
    else {
        my $word = shift @rest;
        ($interval, $word) = parse_interval($word);
        ($align, $match) = isr_align_match($word);
    }
    ($nalign, $nmatch, @rest) = cq($op, @rest);

    return 
        aligner($op, $align, $nalign),
        matcher($op, $match, $nmatch, $interval),
        @rest;
}

sub parse_interval {
    my $word = shift;
    my ($newword, $interval) = split(/#[wWtT]/, $word);
    $interval ||= 1;
    $interval *= -1 if ($word =~ /#[tT]/);
    return $interval, $newword;
}

sub min {
    return 
      (defined $_[0]         ?
          (defined $_[1]     ?
              ($_[0] > $_[1] ?
                  $_[1]      :
                  $_[0] )    :
              $_[0] )        :
          $_[1] );
}


# returns lowest-valued docid in the chain
sub or_aligner {
    my ($this, $next) = @_;

    return sub {
        my $pos = shift;
        my $thispos = $this->($pos);
        my $nextpos = $next->($pos);
        return min($thispos, $nextpos);
    };
}


# returned function does this:
# FIND first instance of ($this0, $thisN) match boundaries
# WHERE $this0 > $top0, $thisN > $topN. 
# RETURN ($this*) or ($next*) according to min($thisN, $nextN)
sub or_matcher {
    my ($this, $next) = @_;
    my ($this0, $thisN, $next0, $nextN) = (0, 0, 0, 0);

    return sub {
        my ($top0, $topN) = @_;

        # return NEXT if there is no THIS.
        return $next->($top0, $topN) unless 
            ($this0, $thisN) = $this->($top0, $topN); # AND-node or ISR

        # find thisN high enough
        while($thisN <= $topN){
            return $next->($top0, $topN) unless
                ($this0, $thisN) = $this->($this0);
        }

        # we have a valid THIS, now get neighboring match.
        return ($this0, $thisN) unless 
            ($next0, $nextN) = $next->($top0, $topN); # OR-node

        return (min($thisN, $nextN) == $thisN) ?
               ($this0, $thisN) :
               ($next0, $nextN);
    };
}


# returns the highest valued document id in the chain
sub seq_aligner {
    my ($this, $next) = @_;
    my $thispos;

    return sub {
        my $pos = shift;
        return undef unless $thispos = $this->($pos);
        return $next->($thispos);
    };
}

# returned function does:
# FIND first instance of $this* and $next* 
# WHERE ($next0-$interval) < $thisN < $nextN
# and $pos < $this0.
# RETURN ($this0, $nextN)
sub seq_matcher {
    my ($this, $next, $interval) = @_;
    my ($this0, $thisN, $next0, $nextN) = (0, 0, 0, 0);
    my $exactadjust = ($interval < 0) ? -$interval : 1; 
    $interval = abs($interval);

    return sub {
        my $pos = shift;
        return () unless 
            ($this0, $thisN) = $this->($pos, $pos); #OR-node or ISR
        return () unless 
            ($next0, $nextN) = $next->($thisN+$exactadjust-1); # AND-chain
        while($thisN < ($next0-$interval)){
            return () unless 
                ($this0, $thisN) = $this->($pos, $next0-$interval-1);
            if($thisN > ($next0-$exactadjust+1)){
                return () unless 
                    ($next0, $nextN) = $next->($thisN);
            }
        }
        return ($this0, $nextN);
    };
}

# Boolean AND matcher. Same as OR matcher except that all members
# must be present somewhere in the document.
# returned function does:
# returns lowest match position after top*, or () if 1) any member
# of the chain not found, or 2) if no member > top*.
sub and_matcher {
    my ($this, $next) = @_;
    my ($this0, $thisN, $next0, $nextN) = (0, 0, 0, 0);

    return sub {
        my ($top0, $topN) = @_;

        # check for existence of THIS member
        return () unless $this->(0,0);

        # return NEXT if there is no THIS past top*.
        # (reduce to rest of chain)
        return $next->($top0, $topN) unless 
            ($this0, $thisN) = $this->($top0, $topN); # node or ISR

        # find thisN high enough
        # (reduce to rest of chain)
        while($thisN <= $topN){
            return $next->($top0, $topN) unless
                ($this0, $thisN) = $this->($this0);
        }

        # return empty if rest of match was missing something
        return () unless 
            ($next0, $nextN) = $next->($top0, $topN); # AND-node

        # return THIS unless rest of chain contained match past top*.
        return ($this0, $thisN) unless
            $next0 >= $top0;

        return (min($thisN, $nextN) == $thisN) ?
               ($this0, $thisN) :
               ($next0, $nextN);
    };
}


# returns the position (docid) it was handed, always. This allows for
# matching within sequences. There is no chain associated with this
# operator. It is unary, therefore it refers to no "next" closure.
sub not_aligner {
    my ($this) = @_;

    return sub {
        my $pos = shift;
        $this->($pos);
        return $pos;
    };
}

# Boolean context: Returns $pos unless a successful match occurred.
# If a match occurred, returns the empty list.
sub not_matcher {
    my ($this) = @_;

    return sub {
        my $pos = shift;
        return ($pos+1, $pos+1) unless $this->(0,0); # no match in doc
        return (); # match found, return negative
    };
}



sub isr_align_match {
    my ($word) = @_;

    my $isr = isr($word);

    if(!$isr->[DX]){ # empty isr?
        return sub { return undef }, # align returns undef
               sub { return () };    # match returns empty
    }

    my $s2p; # which subroutine for sum_to_pos?
    if($word =~ /^_\d+_$/){ # cached result set
        $s2p = \&risr_sum_to_pos;
    } else {
        $s2p = \&sum_to_pos;
    }

    my $docid = 0; # doc id requested
    my $dxsum = 0; # doc id for this isr
    my $dxn = 0; # location in DX
    my $px = ''; # current pos list
    my $pxsum = 0; # token location in doc
    my $pxn = 0; # location in PX string
    my $runlen = 0; # token delta length of match (0 for single term)

    return 
        sub { # the align
            $docid = shift;
            return undef unless defined $dxsum;
            if($docid > $dxsum){  # new doc
                ($dxsum, $dxn, $px) = 
                    sum_to_doc($isr->[DX], $dxsum, $dxn, $docid);
                $px = defined $px ?
                          ($px % 2) ? 
                              $isr->[PX]->[int $px/2] : # string px
                              pack "w", $px/2 :         # single pos delta
                          undef;
                ($pxsum, $pxn) = (0, 0);
            }
            return $dxsum;
        },
        sub { # the match
            return () unless (defined $dxsum) and 
                             ($docid == $dxsum); # the align is valid
            my (undef, $pos) = @_;
            if($pxsum <= $pos){
                ($pxsum, $pxn, $runlen) = $s2p->($px, $pos, $pxn, $pxsum);
            }
            return ($pxsum > $pos) ? ($pxsum, $pxsum+$runlen) : ();
        };
}



1;


__DATA__

=pod

=head1 NAME

Seq - An inverted text index.

=head1 ABSTRACT

THIS IS ALPHA SOFTWARE

Seq is a text indexer and search utility written in Perl and C. It has several special features not to be found in most available similar programs, namely arbitrarily complex sequence and alternation queries, and proximity searches with both exact counts and limits. There is no result ranking (yet). 

The index format draws some ideas from the Lucene search engine, with some simplifications and enhancements. The index segments are stored in a CDB disk hash (from dj bernstein), but support for a standard SQL database backend is coming.

=head1 SYNOPSIS

Index documents:

  # cat textcorpus.txt | tokenize | indexstream index_dir
  # cat textcorpus.txt | tokenize | stopstop | indexstream index_dir
  # optimize index_dir

Search:

  # seqsearch index_dir
  # (type search terms)

=head1 PROGRAMMING API

  use Seq;

  # open for indexing
  $index = Seq->open_write( "indexname" );
  $index->index_document( "docname", $string );
  $index->close_index();

  # open for searching
  $index = Seq->open_read( "indexname" );

  # Find all docs containing a phrase
  $result = $index->search( "this phrase and no other phrase" );

  # result is a list:
  # [ [ docid1, match1, match2 ... matchN ],
  #   [ docid2, match1, ... ] ]
  # 
  # ... where 'match' is the token location of each match within that doc.

  # List information about the result set
  $id = $index->query( "this phrase and no other phrase" );
  $result = $index->set_info( $id );

  # $result is a hashref
  # { ndocs => N, nmatches => M }


=head1 SEARCH SYNTAX

Sequences of words are enclosed in angle brackets '<' and '>'. Alternations are enclosed in square brackets '[' and ']'. These may be nested within each other as long as it makes logical sense. "<the quick [brown grey] fox>" is a simple valid phrase. Nested square brackets don't make sense, logically, so they aren't allowed. Also not allowed are adjacent angle bracket sequences. However, alternations may be adjacent, as in "<I [go went] [to from] the store>". As long as these rules are followed, search terms may be arbitrarily complex. For example:

"<At [a the] council [of for] the [gods <deities we [love <believe in>] with all our [hearts strength]>] on olympus [hercules apollo athena <some guys>] [was were] [praised <condemned to eternal suffering in the underworld>]>"

Two operators are available to do proximity searches. '#wN' represents *at least* N intervening skips between words (the number of between words plus 1). Thus "<The #w8 dog>" would match the text "The quick brown fox jumped over the lazy dog". If #w7 or lesser had been used it would not match, but if #w9 or greater had been used it would still match. Also there is the '#tN' operator, which represents *exactly* N intervening skips. Thus for the above example "<The #t8 dog>", and no other value, would match. These operators can be used after words or alternations, but no other place. 


=head1 AUTHOR

Ira Joseph Woodhead, ira at sweetpota dot to

=head1 SEE ALSO

C<Lucene>
C<Plucene>
C<CDB_File>
C<Inline>

=cut


__C__


/* count how many characters make up the compressed integer 
   at the beginning of the string px. */
int next_integer_length(char* px){
    unsigned int length = 0;
    unsigned char mask = (1 << 7);
    //if(!*px) return 0; // empty string
    while(*px & mask){
        px++;
        length++;
    }
    length++; // final char
    return (int) length;
}

/* convert the compressed integer at the beginning of the string
   px to a int. */
int next_integer_val(char* px){
    unsigned int value = 0;
    unsigned char himask = (1 << 7); // 10000000
    unsigned char lomask = 127;      // 01111111
    while(*px & himask){
        value = ((value << 7) | (*px & lomask));
        px++;
    }
    value = ((value << 7) | *px);
    return value;
}


/* px is a string of chars representing BER compressed integers.
   These are position deltas within a document. pxn is the current
   string index, pxsum is the current sum. sum_to_pos() computes
   the first position in a document past pos.
*/
void sum_to_pos(SV* pxSV, int pos, int pxn, int pxsum){
    char* px = SvPV_nolen( pxSV );

    INLINE_STACK_VARS;
/*
    if(strlen(px) <= pxn){
        INLINE_STACK_RESET;
        INLINE_STACK_PUSH(sv_2mortal(newSViv(0)));
        INLINE_STACK_PUSH(sv_2mortal(newSViv(0)));
        INLINE_STACK_DONE;
        return;
    }
*/

    px += pxn; // advance char pointer to current pxn
    while(*px && (pxsum <= pos)){
        unsigned int len = next_integer_length(px);
        pxsum += next_integer_val(px);
        px += len;
        pxn += len;
    }
    
    INLINE_STACK_RESET;
    INLINE_STACK_PUSH(sv_2mortal(newSViv(pxsum)));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(pxn)));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(0))); // runlen always 0
    INLINE_STACK_DONE;
    return;
}


/* dx is a string of chars representing BER compressed integers.
   These are document id deltas. dxn is the current
   string index, dxsum is the current sum. sum_to_doc() computes
   the first position in the corpus at or past pos, and finds the
   integer index into px (if it exists. if it does not exist, it
   finds the single position delta from dx).
*/
void sum_to_doc(SV* dxSV, int dxsum, int dxn, int pos){
    char* dx = SvPV_nolen( dxSV );
    int pxval = 0;
    int last_dx_delta = 0;

    INLINE_STACK_VARS;

    dx += dxn; // advance char pointer to current dxn
    while(*dx && (dxsum < pos)){
        unsigned int len = next_integer_length(dx);
        last_dx_delta = next_integer_val(dx);
        dxsum += floor(last_dx_delta/2);
        dx += len; // advance ptr
        dxn += len;

        len = next_integer_length(dx);
        pxval = next_integer_val(dx);
        dx += len;
        dxn += len;
    }

    if(dxsum < pos){
        INLINE_STACK_RESET;
        INLINE_STACK_DONE;
        return;
    }
    
    INLINE_STACK_RESET;
    INLINE_STACK_PUSH(sv_2mortal(newSViv(dxsum)));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(dxn)));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(pxval * 2 + (last_dx_delta % 2))));
    INLINE_STACK_DONE;
    return;
}

/* 
   risr_sum_to_pos() computes the first position in a document 
   past pos, plus the length of the match, which is the following number.
*/
void risr_sum_to_pos(SV* pxSV, int pos, int pxn, int pxsum){
    char* px = SvPV_nolen( pxSV );
    int runlen = 0;

    INLINE_STACK_VARS;

    px += pxn; // advance char pointer to current pxn
    while(*px && (pxsum <= pos)){
        unsigned int len = next_integer_length(px);
        pxsum += next_integer_val(px);
        px += len;
        pxn += len;

        len = next_integer_length(px);
        runlen = next_integer_val(px);
           px += len;
        pxn += len;
    }
    
    INLINE_STACK_RESET;
    INLINE_STACK_PUSH(sv_2mortal(newSViv(pxsum)));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(pxn)));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(runlen)));
    INLINE_STACK_DONE;
    return;
}



