package Seq;

require 5.005_62;
use strict;
use warnings;
no warnings "recursion";
use vars qw( $VERSION );

use FileHandle;
use DBI;
use MIME::Base64;

$VERSION = '0.27';
use Inline Config =>
            VERSION => '0.27',
            NAME => 'Seq';
use Inline 'C';


# Constants for data about each word.
use constant NDOCS   => 0;
use constant NWORDS  => 1;
use constant DX      => 2; # doc index
use constant PX      => 3; # position index
use constant LASTDOC => 4;
use constant FLAGS   => 5; #

# flag values
use constant COMPOSITE => 1; # an isr with more than one token in length


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

        $self->{dbh} = DBI->connect("dbi:SQLite:dbname=$path/0/db","","")
          or die $DBI::errstr;

        @{ $self->{ids} } = map { $_->[0] } 
                       @{ dbselect( $self->{dbh}, 
                                    "select xid from docs order by id" ) };
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

    isrcache_init($self->{dbh});
    return bless $self, $type;
}

sub dbselect {
  my $dbh = shift;
  my $sql = shift;
  my $sth = $dbh->prepare($sql);
  $sth->execute(@_);
  my $ref = $sth->fetchall_arrayref;
  $sth->finish;
  return $ref;
}

sub dbinsert {
  my $dbh = shift;
  my $sql = shift;
  my $sth = $dbh->prepare($sql);
  my $retval = $sth->execute(@_);
  $sth->finish;
  return $retval;
}

sub close_index {
    my $self = shift;

    if( $self->{mode} eq 'WRONLY' ){
        $self->_write_segment();
    }
    else {
        $self->{dbh}->disconnect;
        $self->DESTROY;
    }

    return 1;
}

sub DESTROY {
    my $self = shift;
    @{ $self->{ids} } = ();
    %{ $self->{xids} } = ();
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
    my $dbh = shift;
    my $word = shift;
    my $ref = dbselect( $dbh, "select isr from isrs where word=?", ":$word");
    return new_isr() unless $ref and @$ref;
    return _deserialize_isr($ref->[0]->[0]);
}

sub _deserialize_isr {
    my $ISR = decode_base64 $_[0];
    my $isr = new_isr();

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
    $isr->[FLAGS] = 0;
    return $isr;
}


sub newsegment {
    my $path = shift;
    my $dbh = DBI->connect( "dbi:SQLite:dbname=$path/db", "", "",
      { AutoCommit => 0 } ) or die $DBI::errstr;
    $dbh->do( "create table isrs ( word text primary key, isr blob not null)");
    $dbh->do( "create table docs ( id integer primary key autoincrement, xid text unique )");
    return $dbh;
}

sub _write_segment {
    my $self = shift;
    my $path = $self->{name};
    my $nsegments = $self->{nsegments}++;
    my $isrs = $self->{isrs};
    my $count = 0;

    mkdir "$path/$nsegments";
    my $dbh = newsegment("$path/$nsegments");
    my $sth = $dbh->prepare("insert into isrs values (?, ?)");

    my @words = keys %$isrs;
    for my $word ( @words ){

        if( (++$count) % 50 == 0 ){
            print STDERR chr(13), "(", $count, ")\t\t";
        }

		$sth->execute(":$word", _serialize_isr($isrs->{$word}))
          or print STDERR "insert of $word failed.\n";
        delete $isrs->{$word};
    }

    %{ $isrs } = ();

    $sth->finish;

    $sth = $dbh->prepare("insert into docs (xid) values( ? )");
    $sth->execute($_) for @{$self->{ids}};
    $sth->finish;
    @{ $self->{ids} } = ();

    $dbh->commit;
    $dbh->disconnect;

    open CONF, ">$path/$nsegments/conf";
    print CONF 'seg_nwords:', $self->{seg_nwords}, "\n";
    print CONF 'seg_ndocs:', $self->{seg_ndocs}, "\n";
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

    return encode_base64 $newisr;
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
        my $dbh = DBI->connect("dbi:SQLite:dbname=$path/$segment/db","","") 
          or die $DBI::errstr;
        my %localwords = map { $_->[0] => 1 } 
                         @{ dbselect($dbh, "select word from isrs") };
        $words{$_} = 1 for keys %localwords;
        print STDERR "Gathered ", scalar keys %words, " words at segment $segment.           \r";
        my $sth = $dbh->prepare("select word, isr from isrs order by word");
        $sth->execute;
        push @segments, [ $conf, $sth, "$path/$segment", \%localwords, $dbh ];
    }


    # new consolidated index segment
    mkdir "$path/NEWSEGMENT";

    # create new cdb
    print STDERR "Creating new compacted segment.                        \n";
    my $newidx = newsegment("$path/NEWSEGMENT");
    my $isrinsert = $newidx->prepare("insert into isrs values(?,?)");

    my $ntokens = scalar keys %words;
    my $t0 = time;
    for my $word (sort keys %words){
        $ntokens--;
        if(time-$t0 > 2){
            print STDERR "Compacting $ntokens th word: $word                       \r";
            $t0 = time;
        }
        my $isr = new_isr();
        my $ndocs = 0;
        for my $segment (@segments){
            my($conf, $sth, undef, $localwords, $dbh) = @$segment;
            if(exists $localwords->{$word}){ 
                my($dbword, $isr_string) = $sth->fetchrow_array;
                die "Got $dbword instead of $word!\n" unless $dbword eq $word;
                $isr = _append_isr($isr, $ndocs, _deserialize_isr($isr_string));
            }
            $ndocs += $conf->{seg_ndocs};
        }
        $isrinsert->execute($word, _serialize_isr($isr));
    }
    $isrinsert->finish;

    # write docs table, delete older segments
    my $xidinsert = $newidx->prepare("insert into docs (xid) values (?)");
    for my $segment (@segments){
        print STDERR "Writing document ids for segment ", 
                     $segment->[2], ", deleting segment dir.\r";
        my $segids = dbselect($segment->[4], "select xid from docs");
        for(@$segids){
            $xidinsert->execute($_->[0]);
        }
        $segment->[1]->finish;
        $segment->[4]->disconnect;
        unlink glob($segment->[2] . "/*");
        rmdir $segment->[2];
    }
    $xidinsert->finish;

    print STDERR "\nWriting tables.\n";
    $newidx->commit;
    $newidx->disconnect;

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
    my $dbh = '';
    my %ids = (); # cached terms -> ids
    my %terms = (); # cached ids -> terms
    my $tid = 0; # term id counter

    # this is called by the open_read function to 
    # instantiate the cdb object. That way only 
    # the individual word is necessary to call isr().
    sub isrcache_init {
        $dbh = shift;
    }

    # return the isr for a term or id
    sub isr {
        my ($word, $isr) = @_;
        return ($isrs{id($word)} = $isr) if defined $isr; # set

        $isr = exists $isrs{$word} ? $isrs{$word} : _read_isr($dbh,$word);
        # return the empty isr if no occurrence.
        return new_isr() unless $isr;
 
        $nrequests{$word}++;
        $timestamp{$word} = time;

        # About to add to in-memory isr cache, so
        # remove the top 10 isrs according to least-requested and 
        # least-recently requested, if limit reached
#        if(10000 < scalar keys %isrs){
#            my $time = time;
#            my @words = sort { $a <=> $b } 
#                        map { $nrequests{$_} / ($time - $timestamp{$_}) }
#                        keys %isrs;
#            delete $isrs{$_} for @words[0..9];
#        }

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
    return 0 unless opener($qtokens[0]);
    my ($tree) = aq(@qtokens);
    my $canonical = join(" ", @qtokens);
    isr($canonical, eval_query(@$tree)); # add to isr cache
    return id($canonical);
}


sub canonical {
    my $qstr = shift;
    $qstr =~ s/[\[\]\|\<\>\(\)\{\}]/ $& /g;
    $qstr =~ s/(#[wWtT]\d+)/$1/g;
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
    $isr->[LASTDOC] = $lastdoc;
    $isr->[FLAGS] |= COMPOSITE;

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

# Translate a result list (usually from a db query) to sorted internal ids
sub xid_iid {
    my($self, $list) = @_;

    my @new = 
      sort { $a->[0] <=> $b->[0] }
      map { $_->[0] = $self->{xids}->{$_->[0]}; $_ } @$list; # xlate
    return \@new;
}




# SET and SEQUENCE OPERATORS closure
# Return proper aligners/matchers/caps according to operator.
# This is the place to add new operators.
{
    my %ops = (
        '<' => '>',
        '[' => ']',
        '(' => ')',
        '{' => '}',
    );

    sub closer {
        my($left, $right) = @_;
        my $op = substr($left,0,1);
        return $ops{$op} 
            if exists $ops{$op} 
               and ($ops{$op} eq substr($right,0,1));
    }

    sub opener {
        my $left = shift;
        my $op = substr($left,0,1);
        return $ops{$op} if exists $ops{$op};
        return undef;
    }
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


# new architecture

{
    my %dispatch = (
        '<' => \&eval_seq,
        '[' => \&eval_or,
        '(' => \&eval_and,
    );

    sub dispatch {
        return undef unless exists $dispatch{substr($_[0],0,1)};
        return $dispatch{substr($_[0],0,1)};
    }
}

# Assemble Query
sub aq {
    return undef unless @_; # empty list
    my ($op, @rest) = @_;

    my ($this, $subquery);

    if( closer($op, $rest[0]) ){ # this chain is done
        return [ "$op " . shift @rest ], @rest;
    }
    elsif( opener($rest[0]) ){ # new chain
        ($subquery, @rest) = aq(shift @rest, @rest);
        $op .= ' ' .  $subquery->[0];
    }
    else {
        my $word = shift @rest;
        $op .= " $word";
        if($word =~ /^#/){
            $subquery = -$1 if $word =~ /^#[Tt](\d+)$/;
            $subquery =  $1 if $word =~ /^#[Ww](\d+)$/;
        } else {
            $subquery = Seq::isr($word);
        }
    }
    ($this, @rest) = aq($op, @rest);
    $op = shift @$this;
    unshift @$this, $subquery;
    unshift @$this, $op;

    return $this, @rest;
}


sub eval_query {
  my ($qstring, @q) = @_;
  for(@q){
    if( ref($_) and opener($_->[0]) ){ # a subquery
        $_ = eval_query(@$_);
	}
  }
  my $op = dispatch($qstring);
  return $op->(@q);
}

sub eval_and {
  my @and = sort { $a->[1] <=> $b->[1] } @_;
  my $this = shift @and;
  while(@and){
    $this = new_and($this, shift @and);
  }
  return $this;

}

sub eval_or {
  my $this = shift;
  while(@_){
    $this = new_or($this, shift);
  }
  return $this;
}

sub eval_seq {
  my @seq = @_;
  my $this = shift @seq;
  while(@seq){
    my $interval = 1;
    $interval = shift @seq unless ref $seq[0];
    $this = new_seq($this, shift @seq, $interval);
  }
  return $this;
}

# dx = and, px = or
sub new_and {
  my($isr0, $isr1) = @_;
  return conjunctive_dx_merge($isr0, $isr1, \&disjunctive_px_merge);
}

# dx = or, px = or
sub new_or {
  my($isr0, $isr1) = @_;
  return disjunctive_dx_merge($isr0, $isr1);
}

# dx = and, px = seq
sub new_seq {
  my($isr0, $isr1, $interval) = @_;
  my $pxmerge = sequence_px_merge($isr0, $isr1, $interval);
  return conjunctive_dx_merge($isr0, $isr1, $pxmerge);
}

# join two full isrs in an AND relation, combining positions 
# according to a supplied function
sub conjunctive_dx_merge {
    my ($isr0, $isr1, $pxmerge) = @_;
    return new_isr() unless $isr1->[NDOCS]; # empty isr?
    my $isr = new_isr();

    my @dx0 = unpack "w*", $isr0->[DX];
    my @dx1 = unpack "w*", $isr1->[DX];
    my ($flag0, $flag1) = ($isr0->[FLAGS] & COMPOSITE,
                           $isr1->[FLAGS] & COMPOSITE);
    my ($docid, $lastdoc) = (0,0);
    my @DX; my @PX;
    while(@dx0 and @dx1){
        my $cmp = ($dx0[0] >> 1) <=> ($dx1[0] >> 1);
        if($cmp < 0){ # dx1 is larger
            $docid += $dx0[0] >> 1; 
            $dx1[0] = (($dx1[0] >> 1) - ($dx0[0] >> 1)) * 2 + $dx1[0] % 2;
            shift @dx0; shift @dx0;
        }
        elsif ($cmp > 0) { # dx0 is larger
            $docid += $dx1[0] >> 1; 
            $dx0[0] = (($dx0[0] >> 1) - ($dx1[0] >> 1)) * 2 + $dx0[0] % 2;
            shift @dx1; shift @dx1;
        }
        else {
            $docid += $dx0[0] >> 1;
            my $dx0delta = shift @dx0; 
            my $dx1delta = shift @dx1;
            my $px0 = shift @dx0;
            my $px1 = shift @dx1;
            $px0 = ($dx0delta % 2) ?
                      $isr0->[PX]->[$px0] :  # string px
                      pack "w", $px0;        # single position px
            $px1 = ($dx1delta % 2) ?
                      $isr1->[PX]->[$px1] :  # string px
                      pack "w", $px1;        # single position px
            my($px, $nmatches) = $pxmerge->($px0, $px1, $flag0, $flag1);
            next unless $nmatches;
            $isr->[NWORDS] += $nmatches;
            $isr->[NDOCS]++;
            push @DX, ($docid - $lastdoc)*2+1, scalar(@PX);
            push @PX, $px;
            $lastdoc = $docid;
        }
    }
    $isr->[DX] = pack "w*", @DX;
    $isr->[PX] = \@PX;
    $isr->[LASTDOC] = $lastdoc;
    $isr->[FLAGS] |= COMPOSITE;
    return $isr;
}


sub disjunctive_dx_merge {
    my ($isr0, $isr1) = @_;
    return $isr0 unless $isr1->[NDOCS]; # empty isr?
    my $isr = new_isr();

    my @dx0 = unpack "w*", $isr0->[DX];
    my @dx1 = unpack "w*", $isr1->[DX];
    my ($flag0, $flag1) = ($isr0->[FLAGS] & COMPOSITE,
                           $isr1->[FLAGS] & COMPOSITE);
    my ($docid, $lastdoc) = (0,0);
    my @DX; my @PX;
    while(@dx0 and @dx1){
        my($delta0, $delta1, $px0, $px1, $big, $small, $Sisr, $Bisr, $equal);
        my $cmp = ($dx0[0] >> 1) <=> ($dx1[0] >> 1);
        if($cmp < 0){ # dx1 is larger
            ($big, $small, $Sisr, $equal) = 
              (\@dx1, \@dx0, $isr0, 0);
        }
        elsif ($cmp > 0) { # dx0 is larger
            ($big, $small, $Sisr, $equal) = 
              (\@dx0, \@dx1, $isr1, 0);
        }
        else {
            ($big, $small, $Bisr, $Sisr, $equal) = 
              (\@dx0, \@dx1, $isr0, $isr1, 1);
        }

        $docid += $small->[0] >> 1;
        if(!$equal){
            $big->[0] = 
              (($big->[0] >> 1) - ($small->[0] >> 1)) * 2 + $big->[0] % 2;
            $px1 = '';
        } else {
            $px1 = get_px($big, $Bisr);
        }
        $px0 = get_px($small, $Sisr);
        $isr->[NDOCS]++;
        push @DX, ($docid - $lastdoc)*2+1, scalar(@PX);
        push @PX, disjunctive_px_merge($px0, $px1, $flag0, $flag1);
        $isr->[NWORDS] += pop @PX;
        $lastdoc = $docid;
    }
    if(@dx0 or @dx1){
        ($flag0, $isr0, @dx0) = @dx0 ? 
                                ($flag0, $isr0, @dx0) : 
                                ($flag1, $isr1, @dx1);
        while(@dx0){
            $docid += $dx0[0] >> 1;
            $isr->[NDOCS]++;
            push @DX, $dx0[0], scalar @PX;
            push @PX, disjunctive_px_merge(
                        get_px(\@dx0, $isr0), '', $flag0, 0);
            $isr->[NWORDS] += pop @PX;
        }
    }
    $isr->[DX] = pack "w*", @DX;
    $isr->[PX] = \@PX;
    $isr->[LASTDOC] = $docid;
    $isr->[FLAGS] |= COMPOSITE;
    return $isr;
}

sub get_px {
    my ($dx, $isr) = @_;
    my $delta = shift @$dx;
    my $px = shift @$dx;
    $px = ($delta % 2) ?
           $isr->[PX]->[$px] :  # string px
           pack "w", $px;        # single position px
    return $px;
}

sub disjunctive_px_merge {
  my (@px0, @px1);

  # Flags indicate whether to upgrade the lists with intervals
  $_[2] ? 
    ( @px0 = unpack("w*", $_[0]) ) :
    ( @px0 = map { $_, 0 } unpack("w*", $_[0]) );

  $_[3] ? 
    ( @px1 = unpack("w*", $_[1]) ) :
    ( @px1 = map { $_, 0 } unpack("w*", $_[1]) );

  return pack("w*", @px0), @px0/2 unless @px1;
  return pack("w*", @px1), @px1/2 unless @px0;

  my @px = ();
  while(@px0 and @px1){
    if($px0[0] <= $px1[0]){
      my ($pxmin, $rlen) = (shift @px0, shift @px0);
      next unless $pxmin; # don't record zeroes
      $px1[0] -= $pxmin;
      push @px, $pxmin, $rlen;
    }
    else {
      my ($pxmin, $rlen) = (shift @px1, shift @px1);
      next unless $pxmin; # don't record zeroes
      $px0[0] -= $pxmin;
      push @px, $pxmin, $rlen;
    }
  }
  push @px, @px0;
  push @px, @px1;
  return pack("w*", @px), @px/2;
}

# curry the interval and summing functions
sub sequence_px_merge {
 my ($isr0, $isr1, $interval) = @_;

  # Flags indicate which summing function to use
  # both are: (pos, string index, runlen) = 
  #             s2p->(string, min pos, string index, current pos)
  my($s2p0, $s2p1);
  $isr0->[FLAGS] & COMPOSITE ? 
    ($s2p0 = \&risr_sum_to_pos) :
      ($s2p0 = \&sum_to_pos) ;

  $isr1->[FLAGS] & COMPOSITE ? 
    ($s2p1 = \&risr_sum_to_pos) :
      ($s2p1 = \&sum_to_pos) ;

  my $max_int = abs($interval);
  my $min_int = $interval < 0 ? abs($interval) : 0;

  sub {
    my ($px0, $px1) = @_;

    my ($sum0, $sum1, $n0, $n1, $pos, $runlen0, $runlen1) = (0,0,0,0,0,0);
    my @px;
    my $lastmatch = 0;
    while( ($sum0, $n0, $runlen0) = 
             $s2p0->($px0, $pos, $n0, $sum0) ) { # get next position
      last unless $sum0 > $pos; 
      $pos = $sum0;
      if($sum1 < $sum0+$runlen0){
        ($sum1, $n1, $runlen1) = 
          $s2p1->($px1, $sum0+$runlen0, $n1, $sum1);
      }
      my $span = $sum1 - ($sum0+$runlen0);
      last unless $span > 0;
      if( $min_int <= $span and $span <=  $max_int ){
        push @px, $sum0 - $lastmatch, $sum1+$runlen1 - $sum0; # pos delta, runlen
        $lastmatch = $sum0;
      }
    }
    return pack("w*", @px), @px/2;
  }
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




