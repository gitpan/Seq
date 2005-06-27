package Seq;

require 5.005_62;
use strict;
use warnings;
use vars qw( $VERSION );

use FileHandle;
use DBI;
use MIME::Base64;

$VERSION = '0.30';
use Inline Config =>
            VERSION => '0.30',
            NAME => 'Seq';
use Inline 'C';
#use Inline C => Config => LIBS => '-lJudy',
#           INC => '-I/usr/lib';


# debugging
#use ExtUtils::Embed;
#use Inline C => Config => CCFLAGS => "-g"; # add debug stubs to C lib
#use Inline Config => CLEAN_AFTER_BUILD => 0; # cp _Inline/build/../...xs .


# Constants for data about each word.
use constant NDOCS   => 0;
use constant NWORDS  => 1;
use constant DX      => 2; # doc index
use constant PX      => 3; # position index
use constant LASTDOC => 4;
use constant FLAGS   => 5; #

# flag values
use constant COMPOSITE => 1; # an isr with positions having >0 runlength

use constant LEFT_NEGATION  => (1 << 1);
use constant RIGHT_NEGATION => 1;

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


sub segment_dirs {
    my $path = shift;
    my @dirs = sort { $a <=> $b }
               grep { /^\d+$/ }
               map { s/^.+?\/(\d+)$/$1/; $_ }
               glob("$path/*");
    return @dirs;
}


sub fold_segments {
    my $path = shift;

    my @dirs = segment_dirs($path);
    while(@dirs > 1){
        fold_batch($path, @dirs);
        @dirs = segment_dirs($path);
    }
}


sub fold_batch {
    my($path, @dirs) = @_;

    my($ndocs, $nwords, $nsegments) = (0,0,0);
    while(@dirs){
        $nsegments++;
        my @batch = splice(@dirs, 0, @dirs > 10 ? 10 : scalar @dirs);
        my($seg_nwords, $seg_ndocs) = compact_batch($path, @batch);
        $nwords += $seg_nwords;
        $ndocs += $seg_ndocs;
    }

    my $conf = _configure($path);
    open CONF, ">$path/conf";
    binmode CONF;
    print CONF 'seg_max_words:', $conf->{seg_max_words}, "\n";
    print CONF "# DO NOT EDIT BELOW THIS LINE\n";
    print CONF "nsegments:$nsegments\n";
    print CONF "nwords:", $nwords, "\n";
    print CONF "ndocs:", $ndocs, "\n";
    close CONF;
}


sub compact_batch {
    my ($path, @dirs) = @_;

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
        print STDERR status_msg("Gathered ", 
                                 scalar keys %words, 
                                 " words at segment $segment.");
        my $sth = $dbh->prepare("select word, isr from isrs order by word");
        $sth->execute;
        push @segments, [ $conf, $sth, "$path/$segment", \%localwords, $dbh ];
    }


    # new consolidated index segment
    mkdir "$path/NEWSEGMENT";

    # create new db file
    print STDERR status_msg("Creating new compacted segment.");
    my $newidx = newsegment("$path/NEWSEGMENT");
    $newidx->do("PRAGMA default_synchronous = OFF");
    my $isrinsert = $newidx->prepare("insert into isrs values(?,?)");

    my $ntokens = scalar keys %words;
    my $alltokens = $ntokens;
    my $t0 = time;
    my $tstart = $t0;
    for my $word (sort keys %words){
        $ntokens--;
        if(time-$t0 > 2){ # print status every ~2 seconds
            $t0 = time;
            my $elapsed = $t0 - $tstart;
            my $pct = 100 - int 100*$ntokens/$alltokens;
            my $eta = int $ntokens/(($alltokens-$ntokens)/$elapsed);
            $eta = int($eta/60) . ":" . $eta%60;
            $elapsed = int($elapsed/60) . ":" . $elapsed%60;
            print STDERR 
              status_msg("Compacting $ntokens th word ($pct\%, elapsed $elapsed, eta $eta): $word");
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
        print STDERR status_msg("Writing document ids for segment ", 
                                $segment->[2], ", deleting segment dir.");
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

    print STDERR "Writing tables.                                      \r";
    $newidx->commit;
    $newidx->disconnect;

    my ($nwords, $ndocs);
    $nwords += $_->[0]->{seg_nwords} for @segments;
    $ndocs += $_->[0]->{seg_ndocs} for @segments;

    open CONF, ">$path/NEWSEGMENT/conf";
    print CONF "seg_nwords:", $nwords, "\n";
    print CONF "seg_ndocs:", $ndocs, "\n";
    close CONF;

    rename "$path/NEWSEGMENT", "$path/$dirs[0]";

    my $elapsed = time - $tstart;
    $elapsed = int($elapsed/60) . ":" . $elapsed%60;
    print STDERR 
      "Done. Segments $dirs[0] to $dirs[$#dirs] elapsed time: $elapsed\n";
    return $nwords, $ndocs;
}


# format a non-scrolling one-line message for the screen
sub status_msg {
    my $message = shift;
    if(length $message > 80){
        $message = substr($message, 0, 79);
    } else {
        $message .= ' ' x (79 - length $message);
    }
    $message .= "\r";
    return $message;
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

# Isrs are cached in a closure initialized with the database handle
{
    my %isrs = ();
    my %nrequests = ();
    my %timestamp = ();
    my $dbh = '';
    my %ids = (); # cached terms -> ids
    my %terms = (); # cached ids -> terms
    my $tid = 0; # term id counter

    # this is called by the open_read function to 
    # instantiate the dbh object. That way only 
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
        return '@' . $tid++;
    }

    # return an id to a multi-term query, generating new one if necessary
    sub id {
        my $term = shift;
        return $term unless $term =~ / /; # no single-word searches
        $term =~ s/@\d+/$terms{$&}/g; # retranslate to full term
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

    ($base) = $self->query($base);
    $base = isr($base); # base can be a query or docset
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
    $isr->[FLAGS] ||= COMPOSITE;
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
    $offset ||= 0;
    $runlen ||= 10;

    my($id, $canonical) = $self->query($query);
    my %info = %{ $self->set_info($id) };
    return [ [ $id, 
               $canonical, 
               $info{ndocs}, 
               $info{nwords} ],
             $self->set_slice($id, $offset, $runlen) ];
}

# return a result isr id from a query string. Side effect: caches
# query and result set.
sub query {
    my ($self, $query) = @_;
    my $canonical = canonical($query);
    my @qtokens = split(/ /, 
                      transcribe_query($canonical));
    return ($qtokens[0], $canonical) if @qtokens == 1;
    return () unless opener($qtokens[0]);
    my ($tree) = aq(@qtokens);
    isr($canonical, eval_query(@$tree)); # add to isr cache
    return id($canonical), $canonical;
}


sub canonical {
    my $qstr = shift;
    $qstr =~ s/[\[\]\|\<\>\(\)\{\}\^]/ $& /g;
    $qstr =~ s/(#[wWtT]\d+)/$1/g;
    $qstr =~ s/^\s+//g;
    $qstr =~ s/\s+$//g;
    $qstr =~ s/\s+/ /g;
    return $qstr;
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
# query is transformed to list of lists.
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
            $subquery = -$1 if $word =~ /^#[Dd](\d+)$/;
            $subquery =  $1 if $word =~ /^#[Ww](\d+)$/;
        } elsif($word eq '^') {
            $subquery = $word;
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
        my $subq_str = $_->[0];
        $_ = eval_query(@$_);
        isr($subq_str, $_);
    }
  }
  my $op = dispatch($qstring);
  return $op->(@q);
}

# eval a list of isrs by the () relation
# note that negated isrs are preceded by the '^' string
sub eval_and {
  my @and = @_;
  my $neg_flag = 0;
  my $this = shift @and;
  if($this eq '^'){
    $neg_flag |= LEFT_NEGATION;
    $this = shift @and;
  }
  while(@and){
    if($and[0] eq '^'){
        $neg_flag |= RIGHT_NEGATION;
        shift @and;
    }
    $this = merge_and($this, shift(@and), $neg_flag);
    $neg_flag = 0;
  }
  return $this;
}

sub eval_or {
  my $this = shift;
  while(@_){
    $this = merge_or($this, shift);
  }
  return $this;
}

sub eval_seq {
  my @seq = @_;
  my $this = shift @seq;
  while(@seq){
    my $interval = 1;
    $interval = shift @seq unless ref $seq[0];
    $this = merge_seq($this, shift @seq, $interval);
  }
  return $this;
}


# new universal dx_merge takes 
# $isr0, $isr1, $left, $center, $right, [ $interval ]
# where left, center, right are functions on the complements and
# intersection of the two sets

sub merge_and {
  my($isr0, $isr1, $flag) = @_;
  return 0 if ($flag & LEFT_NEGATION) and ($flag & RIGHT_NEGATION);
  my($left, $center, $right) = (\&px_ignore, \&px_merge, \&px_ignore);
  ($left, $center, $right) = 
    (\&px_ignore, \&px_ignore, \&px_upgrade)
      if $flag & LEFT_NEGATION;

  ($left, $center, $right) = 
    (\&px_upgrade, \&px_ignore, \&px_ignore) 
      if $flag & RIGHT_NEGATION;

  return dx_merge($isr0, $isr1, $left, $center, $right);
}

sub merge_or {
  my($isr0, $isr1) = @_;
  return dx_merge($isr0, $isr1, \&px_upgrade, \&px_merge, \&px_upgrade);
}

sub merge_seq {
  my($isr0, $isr1, $interval) = @_;
  return dx_merge($isr0, $isr1, 
                  \&px_ignore, \&px_sequence, \&px_ignore, 
                  $interval);
}


# Universal version of DX merge.
# Split two isrs into left, center, and right components
# (as in a venn diagram), and perform the specified functions
# on each component's px list.
# (A B) = (px_ignore, px_merge, px_ignore)
# [A B] = (px_upgrade, px_merge, px_upgrade)
# (A ^B) = (px_upgrade, px_ignore, px_ignore)
# (^A B) = (px_ignore, px_ignore, px_upgrade)
# <A ^B> = (px_upgrade, px_neg_sequence_left, px_ignore)
# <^A B> = (px_ignore, px_neg_sequence_right, px_upgrade)
sub dx_merge {
    my ($isr0, $isr1, $left, $center, $right, $interval) = @_;
    $interval ||= -1;
    my $isr = new_isr();

    my @dx0 = unpack "w*", $isr0->[DX];
    my @dx1 = unpack "w*", $isr1->[DX];
    my ($docid, $lastdoc, $nmatches, $px) = (0,0,0,'');
    my @DX = (); my @PX = ();
    while(@dx0 and @dx1){
        my $cmp = ($dx0[0] >> 1) <=> ($dx1[0] >> 1);
        if($cmp < 0){ # dx1 is larger
            $docid += $dx0[0] >> 1; 
            $dx1[0] = (($dx1[0] >> 1) - ($dx0[0] >> 1)) * 2 + $dx1[0] % 2;
            ($px, $nmatches) = $left->($isr0, splice(@dx0, 0, 2));
            next unless $nmatches;
        }
        elsif ($cmp > 0) { # dx0 is larger
            $docid += $dx1[0] >> 1; 
            $dx0[0] = (($dx0[0] >> 1) - ($dx1[0] >> 1)) * 2 + $dx0[0] % 2;
            ($px, $nmatches) = $right->($isr1, splice(@dx1, 0, 2));
            next unless $nmatches;
        }
        else {
            $docid += $dx0[0] >> 1;
            ($px, $nmatches) = 
              $center->($isr0, $isr1, 
                        splice(@dx0,0,2), splice(@dx1,0,2), $interval);
            next unless $nmatches;
        }
        $isr->[NWORDS] += $nmatches;
        $isr->[NDOCS]++;
        push @DX, ($docid - $lastdoc)*2+1, scalar(@PX);
        push @PX, $px;
        $lastdoc = $docid;
    }
    if(@dx0 or @dx1){
        my($side_isr, $func, @dx) = @dx0 ? 
                            ($isr0, $left,  @dx0) : 
                            ($isr1, $right, @dx1);
        while(@dx){
            $docid += $dx[0] >> 1;
            my($px, $nmatches) = $func->($side_isr, splice(@dx,0,2));
            next unless $nmatches;
            $isr->[NWORDS] += $nmatches;
            $isr->[NDOCS]++;
            push @DX, ($docid - $lastdoc)*2+1, scalar @PX;
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

# Supporting px functions for universal dx merge
sub px_ignore {
    # px, nmatches
    return (undef, 0);
}

sub px_upgrade {
    my($isr, $delta, $px) = @_;
    my $nmatches;
    if($isr->[FLAGS] & COMPOSITE){ # already upgraded, just count them
      $px = $isr->[PX]->[$px];
      $nmatches++ for unpack("w*", $px);
      $nmatches /= 2;
    } elsif ( $delta % 2 ){ # upgrade a string px
      my @px = unpack "w*", $isr->[PX]->[$px];
      $nmatches = @px;
      $px = pack "w*", map { $_, 0 } @px;
	} else { # upgrade a single position
      $px = pack "w*", $px, 0;
      $nmatches = 1;
    }
    return ($px, $nmatches);
}

# for universal dx_merge, taking two corresponding docs
sub px_merge {
    my($isr0, $isr1, $dx0delta, $px0, $dx1delta, $px1) = @_;
    my ($flag0, $flag1) = ($isr0->[FLAGS] & COMPOSITE,
                           $isr1->[FLAGS] & COMPOSITE);

    $px0 = ($dx0delta % 2) ?
           $isr0->[PX]->[$px0] :  # string px
           pack "w", $px0;        # single position px
    $px1 = ($dx1delta % 2) ?
           $isr1->[PX]->[$px1] :  # string px
           pack "w", $px1;        # single position px

    my($px, $nmatches) = 
      _px_merge($px0, $px1, $flag0, $flag1, 1);
    return ($px, $nmatches);
}

sub px_sequence {
    my($isr0, $isr1, $dx0delta, $px0, $dx1delta, $px1, $interval) = @_;
    my ($flag0, $flag1) = ($isr0->[FLAGS] & COMPOSITE,
                           $isr1->[FLAGS] & COMPOSITE);

    $px0 = ($dx0delta % 2) ?
           $isr0->[PX]->[$px0] :  # string px
           pack "w", $px0;        # single position px
    $px1 = ($dx1delta % 2) ?
           $isr1->[PX]->[$px1] :  # string px
           pack "w", $px1;        # single position px

    my($px, $nmatches) = 
      _px_sequence($px0, $px1, $flag0, $flag1, $interval);
    return ($px, $nmatches);
}




1;


__DATA__

=pod

=head1 NAME

Seq - A search and set manipulation engine for text documents

=head1 ABSTRACT

THIS IS ALPHA SOFTWARE

Seq is a text indexer, search utility, and document set manipulator written in Perl and C. It has several special features not to be found in most available similar programs, because of a special set of requirements. Searches consist of arbitrarily complex nested queries, with conjunction, disjunction and negation of words, phrases, or otherwise designated whole document sets. Phrases have proximity operators, with both exact counts and limits. Any document or result set may be included in a subsequent query, as if it were a subquery. Also there are no stop words, so that any query, even one consisting of very common words, (eg <with the> or <for the>) will generate a result set.

The index format draws some ideas from the Lucene search engine, with some simplifications and enhancements. The index segments are stored in an SQL database, defaulting to the zero-configuration SQLite.

=head1 SEARCH/SET MANIPULATION SYNTAX

Search and set operations are internally identical, therefore any search query can be nested inside a set operation query, and vice versa.

Phrases are enclosed in angle brackets '<' and '>'. Alternations, or disjunctions, are enclosed in square brackets '[' and ']'. Conjunctions are enclosed in parentheses '(' and ')'. Any delimited entity inside of another is called a subquery. Any number of entities (subqueries or words) may be in a subquery. These may be nested one within another without limit, as long as it makes logical sense. "<the quick [brown grey] fox>" is a simple valid phrase. When a subquery occurs in the context of a phrase, it is called "positional". "Positional" context is opposed to overall document set context. For instance, in the query "[dog cat]" the words do not have positional context, so it doesn't matter where they occur in the document. In "<the [dog cat]>" the subquery "[dog cat]" has positional context, so it matters where they are in a document (namely they must come right after the word "the"). 

Negation of entities are indicated with a preceding '^'. Negations can be of any entity, including words, phrases, disjunctions, and conjunctions. Negation must not occur in a void context. For instance, the whole query cannot be negated, eg "^<quick brown fox>" is not allowed but "(^<quick brown fox> <lazy dog>)" is. There must be a positive component to the query at the same level as the negation.

Search terms may be arbitrarily complex. For example:

"<At [a the] council [of for] the [gods <deities we [love <believe in>] with all our [hearts strength]>] on olympus [hercules apollo athena <some guys>] [was were] [praised <condemned to eternal suffering in the underworld>]>"

"(<frankly my dear> ^(<whatever shall I do> <wherever shall I go>))"

Two operators are available to do proximity searches within phrases. '#wN' represents *at least* N intervening skips between words (the number of between words plus 1). Thus "<The #w8 dog>" would match the text "The quick brown fox jumped over the lazy dog". If #w7 or lesser had been used it would not match, but if #w9 or greater had been used it would still match. Also there is the '#dN' operator, which represents *exactly* N intervening skips. Thus for the above example "<The #d8 dog>", and no other value, would match. These operators can be used within phrases only. 


=head1 INDEXING

Several programs are provided to use for indexing documents. A typical index is created like this:

 # cat textcorpus.txt | tokenizestream | indexstream index_dir
 # optimize index_dir

The indexstream program writes index segments, which are then folded together into one by the optimize program.

At minimum, documents must be marked up with <DOCID> and <TEXT> sections. If raw text files are to be indexed, there is a "docize" program which can take a list of filenames and output them to stdout with the appropriate markup, using the file path as docid. For example:

 # docize dir_of_textfiles/ | tokenizestream | indexstream index_dir




=head1 TCP/IP SERVICE API

There is a program included which implements a socket-based service. To use it call:

 # bin/seqsvc listen-port index_dir

The listener takes requests in the form:

 function-name
 arg1
 ...
 argn

with two newlines terminating the request. So for instance to issue a search request with the query "<foo bar>" and retrieve the 20th - 30th document results, open a socket and write

 search
 <foo bar>
 20
 10

The seqsvc program responds with the results and closes the socket.

Results are returned in JSON format (see http://www.crockford.com/JSON/index.html). This is a simple format easily parsed by most programming languages. In particular, if only lists are used, the representation is identical to a Perl list (of lists of lists etc) and therefore can simply be eval'd in Perl. 

The structure of particular results are dependent on the function called, but the most common case is the search() function, whose results are in the form of two lists. The first list contains:

 query elapsed time
 a return code (1 for success, 0 for failure)
 result set unique ID
 canonicalized query
 result document count
 result match count

The second list contains the slice of results requested. These are of the form:

[ docid, match1, runlength1, ... matchN, runlengthN ]

Matches are indicated as deltas. That is, each represents the distance in tokens from the previous match or beginning of the document. Runlength is the length in tokens of the match. If no offset and slice size are given, the first ten result documents are returned.

Here is a complete example of a call and response:

 search
 <for the>
 0
 10

 [
   [
     0.255496,
     1,
     '@0',
     '< for the >',
     1698,
     8534
   ],
   [
     [ '333916', 232, 1, 59, 1, 171, 1, 399, 1, 107, 1 ],
     [ '333920', 81, 1, 7, 1 ],
     [ '333921', 70, 1 ],
     [ '333924', 120, 1, 320, 1, 111, 1, 67, 1 ],
     [ '333925', 347, 1, 67, 1 ],
     [ '333926', 114, 1, 7, 1, 212, 1, 168, 1 ],
     [ '333927', 297, 1 ],
     [ '333930', 44, 1 ],
     [ '333935', 181, 1, 62, 1, 290, 1, 16, 1, 29, 1, 72, 1, 83, 1 ],
     [ '333943', 52, 1 ]
   ]
 ]


Other functions, such as for sampling, are in development.


=head1 INTERNALS

Each query is compiled into a syntax tree and evaluated recursively. The operations all boil down to merging two C<posting lists>, the inverted index data structure for words/result sets. In the following I'll describe the posting lists for single words (tokens), although the format is nearly the same as for general result sets.

A posting list is referred to in the code as an "isr" for historical reasons. The data structure is an array containing, most importantly, a list of documents, and a list of position lists. A position list contains all the positions in a particular document where the word appears. 

The process of executing a query boils down to merging two posting lists into their left complement, intersection, and right complement (think of a Venn diagram with two circles intersecting, creating three regions). All the set and search operations are performed by doing this and then performing different combinations of functions on the documents depending on which region they are in. For instance, consider two posting lists for tokens A and B. Merging them gives us the left complement (all documents where A occurs but B does not), the intersection (where both A and B occur) and the right complement (where B occurs but A does not). If the operation is conjunction (intersection), then both left and right complement are simply ignored, and the intersection is taken as the result after merging the individual position lists. If the operation is disjunction (union), all documents found are returned, with the position lists of the complements essentially unchanged and those in the intersection merged. If the operation is a sequence (phrase search), the two complements are ignored and the intersection's position lists are checked and returned if A and B are found in the correct relation. Another example: If the operation involves a negated token, such as "(A ^B)", then both the intersection and right complement are ignored, and the left complement is taken as the result set.

Where possible, the postings are merged in the optimal order, going from least frequent to most frequent tokens.



=head1 TO DO

Not all behavior is exactly as described in this document. 1 day.

There is a bug in sequencing causing matches to be missed in rare cases. The algorithm needs redesigning. 3 days.

Positional negation eg "<what ^ever>". 2-3 days.

Error handling, for instance syntax checks. 2-3 days.

The database backend to be generalized and network-based rather than on local disk. The socket service to be multiprocess as in typical pre-forking http server. 4-5 days.

Partial result sets for speedier response. 3 days.

Design a parallel database querying system which produces Seq result sets in the Seq backend. ? days.




=head1 PROGRAMMING API

Seq is a library, you can use it directly in a perl program.

  use Seq;

  # open for indexing
  $index = Seq->open_write( "indexname" );
  $index->index_document( "docname", $string );
  $index->close_index();

  Seq::fold_segments("indexname"); # fold all segments together

  # open for searching
  $index = Seq->open_read( "indexname" );

  # Find all docs containing a phrase
  $result = $index->search( "<this phrase and no other phrase>" );

$result is two lists. The first list is data about the result set:

   [ return-code, setid, canonical-query, ndocs, nmatches ],

Second list is a slice of the results, default results 0-9:
   [ [ docid1, match1, length1, match2, length2 ... matchN, lengthN ],
     [ docid2, match1, length1, ... ] ]

... where 'match' is the delta of the token location of each match within that doc.


=head1 AUTHOR

Ira Joseph Woodhead, ira at sweetpota dot to

=head1 SEE ALSO

C<Lucene>
C<Plucene>
C<Inline>

=cut


__C__


#define HIMASK 0x80 // 10000000
#define LOMASK 0x7f // 01111111

/* count how many characters make up the compressed integer 
   at the beginning of the string px. */
int next_integer_length(char* px){
    unsigned int length = 0;
    //if(!*px) return 0; // empty string
    while(*px & HIMASK){
        px++;
        length++;
    }
    length++; // final char
    return (int) length;
}

/* convert the compressed integer at the beginning of the string
   px to an int. */
int next_integer_val(char* px){
    unsigned int value = 0;
    while(*px & HIMASK){
        value = ((value << 7) | (*px & LOMASK));
        px++;
    }
    value = ((value << 7) | *px);
    return (int) value;
}

/* do both above conversions. Return length on the stack and 
   set the value in the pointed-to integer. 
*/
int next_integer(char* px, int* value){
    *value = 0;
    unsigned int length = 0;
    while(*px & HIMASK){
        *value = ((*value << 7) | (*px & LOMASK));
        px++;
        length++;
    }
    length++;
    *value = ((*value << 7) | *px);
    return (int) length;
}

// px is char*, value is int, runlen is int
// px is incremented, value is replaced, runlen is incremented
#define NEXT_INTEGER(px, value, length) \
  value = 0; \
  while(*px & HIMASK){ \
    value = ((value << 7) | (*px & LOMASK)); \
    px++; \
    length++; \
  } \
  value = ((value << 7) | *px); \
  px++; \
  length++; 


/* packs a 32-bit integer into the tail end of a string buffer 
   as a BER, returns length. Adapted from pp_pack.c in perl source.
*/
int int_to_ber(unsigned int i, char* buf){
    int bytes = (sizeof(unsigned int)*8)/7+1;
    char  *in = buf + bytes;

    do {
      *--in = (char)((i & LOMASK) | HIMASK);
      i >>= 7;
    } while (i);
    *(buf+bytes - 1) &= LOMASK; /* clear continue bit */
    return (buf + bytes) - in;
}




/* next_px_0
   takes a px string of type 0 (no runlens encoded) and sets 
   position delta, delta ptr and delta runlen. Leaves runlen ptr and 
   runlen runlen alone.
*/
void next_px_0(char** px, int* delta, 
               char** r_ptr, int* r_runlen){  // runlen 
     *px += next_integer(*px, delta);
}

/* next_px_1
   takes a px string of type 1 (runlens encoded) and sets 
   position delta, delta ptr, delta runlen, runlen ptr, runlen runlen.
*/
void next_px_1(char** px, int* delta, 
               char** r_ptr, int* r_runlen){
    *px += next_integer(*px, delta);
    *r_ptr = *px;
    *r_runlen = next_integer_length(*px);
    *px += *r_runlen;
}



// macro to pack integer and copy to px
#define WRITEDELTA(px,delta,dlen,rptr,rlen,packbuf,pxlen) \
          dlen = int_to_ber(delta, packbuf); \
          memcpy(px, packbuf+sizeof(packbuf)-dlen, dlen); \
          memcpy(px+dlen, rptr, rlen); \
          px += dlen + rlen; \
          pxlen += dlen + rlen; 


/* disjunctive merge
   takes two px strings of either type and merges them disjunctively
*/

void _px_merge(SV* px0SV, SV* px1SV, bool type0, bool type1, 
                          int interval){ // interval just for compatibility
    char* px0 = SvPV_nolen( px0SV );
    char* px1 = SvPV_nolen( px1SV );
    int   dlen  = 0; 
    char* zero = "";
    char*  px0rptr  = zero; char* px1rptr  = zero;
    int    px0rlen  = 1;    int   px1rlen  = 1;
    unsigned int    px0delta = 0; unsigned int   px1delta = 0;
    char buf[(sizeof(unsigned int)*8)/7+1];

    int result_count = 0;
    int result_len = 2*( SvCUR(px0SV) + SvCUR(px1SV) );
    SV* result = newSVpvn(" ", result_len);
    char* result_str = SvPV_nolen( result );
    result_len = 0;


    // coderefs
    void (*px0_next)(char** px,   int* delta, 
                     char** rptr, int* rlen);
    void (*px1_next)(char** px,   int* delta, 
                     char** rptr, int* rlen);


    INLINE_STACK_VARS;
    INLINE_STACK_RESET;

    px0_next = type0 ? next_px_1 : next_px_0;
    px1_next = type1 ? next_px_1 : next_px_0;

    while(*px0 || *px1){
        
        // read next integers from px
        if(px0delta == 0){
          if(!*px0) break;
          px0_next(&px0, &px0delta, &px0rptr, &px0rlen);
        }
        if(px1delta == 0){
          if(!*px1) break;
          px1_next(&px1, &px1delta, &px1rptr, &px1rlen);
        }

        // write appropriate result
        if(px0delta < px1delta){
          WRITEDELTA(result_str, px0delta, dlen, 
                     px0rptr, px0rlen, buf, result_len);
          result_count++;
          px1delta -= px0delta;
          px0delta = 0;
        } else if(px1delta < px0delta){
          WRITEDELTA(result_str, px1delta, dlen, 
                     px1rptr, px1rlen, buf, result_len);
          result_count++;
          px0delta -= px1delta;
          px1delta = 0;
        } else {
          WRITEDELTA(result_str, px0delta, dlen, 
                     px0rptr,px0rlen, buf, result_len);
          result_count++;
          px0delta = 0;
          px1delta = 0;
        }
    }

    // one list is exhausted.
    // continue until both lists are exhausted
    while(*px0 || px0delta){
      if(px0delta){
        WRITEDELTA(result_str, px0delta, dlen, 
                   px0rptr,px0rlen, buf, result_len);
        result_count++;
        px0delta = 0;
      }
      if(*px0){
        px0_next(&px0, &px0delta, &px0rptr, &px0rlen);
      }
    }
    while(*px1 || px1delta){
      if(px1delta){
        WRITEDELTA(result_str, px1delta, dlen, 
                   px1rptr, px1rlen, buf, result_len);
        result_count++;
        px1delta = 0;
      }
      if(*px1){
        px1_next(&px1, &px1delta, &px1rptr, &px1rlen);
      }
    }

    SvCUR_set(result, result_len);
    INLINE_STACK_PUSH(sv_2mortal(result));
    INLINE_STACK_PUSH(sv_2mortal(newSViv(result_count)));
    INLINE_STACK_DONE;
    return;
}




/* px is a string of chars representing BER compressed integers.
   These are position deltas within a document. pxn is the current
   string index, pxsum is the current sum. sum_to_pos() computes
   the first position in a document past pos.
*/
void _sum_to_pos(char* px, int pos, int* pxn, int* pxsum, int* runlen){

  px += *pxn; // advance char pointer to current pxn
  while(*px && (*pxsum <= pos)){
    unsigned int val;
    unsigned int len;

    len = next_integer(px, &val);
    *pxsum += val;
    px += len;
    *pxn += len;
  }
  *runlen = 0;

  return;
}


/* 
   risr_sum_to_pos() computes the first position in a document 
   past pos, plus the length of the match, which is the following number.
*/
void _risr_sum_to_pos(char* px, int pos, int* pxn, int* pxsum, int* runlen){

  px += *pxn; // advance char pointer to current pxn
  while(*px && (*pxsum <= pos)){
    unsigned int val;
    unsigned int len;

    len = next_integer(px, &val);
    *pxsum += val;
    px += len;
    *pxn += len;

    len = next_integer(px, &val);
    *runlen = val;
    px += len;
    *pxn += len;
  }
  return;
}


void _px_sequence(SV* px0SV, SV* px1SV, bool type0, bool type1, 
                       int interval){
  char* px0 = SvPV_nolen( px0SV );
  char* px1 = SvPV_nolen( px1SV );
  int max_int = abs(interval);
  int min_int = interval < 0 ? abs(interval) : 0; // neg means exact dist
  char buf[(sizeof(unsigned int)*8)/7+1];

  int sum0 = 0;    int sum1 = 0;
  int n0 = 0;      int n1 = 0;
  int runlen0 = 0; int runlen1 = 0;
  int pos = 0;
  int lastmatch = 0;
  int span = 0;

  int result_count = 0;
  int result_len = 2*( SvCUR(px0SV) + SvCUR(px1SV) );
  SV* result = newSVpvn(" ", result_len);
  char* result_str = SvPV_nolen( result );
  result_len = 0;

  // coderefs for sum-to-position
  void (*s2p0)(char* px, int pos, int* n, int* sum, int* runlen);
  void (*s2p1)(char* px, int pos, int* n, int* sum, int* runlen); 

  INLINE_STACK_VARS;
  INLINE_STACK_RESET;

  s2p0 = type0 ? _risr_sum_to_pos : _sum_to_pos;
  s2p1 = type1 ? _risr_sum_to_pos : _sum_to_pos;

  for(;;){
    s2p0(px0, pos, &n0, &sum0, &runlen0); // get next position
    if(sum0 <= pos) break; 
    pos = sum0;
    if(sum1 < sum0+runlen0)
      s2p1(px1, sum0+runlen0, &n1, &sum1, &runlen1);
    span = sum1 - (sum0+runlen0);
    if(span <= 0) break;
    if((min_int <= span) && (span <= max_int)){
      span = int_to_ber(sum0 - lastmatch, buf); // pos delta
      memcpy(result_str, buf+sizeof(buf)-span, span); 
      result_str += span;
      result_len += span;

      span = int_to_ber(sum1 + runlen1 - sum0, buf); // runlen
      memcpy(result_str, buf+sizeof(buf)-span, span);
      result_str += span;
      result_len += span;

      lastmatch = sum0;
      result_count++;
    }
  }

  SvCUR_set(result, result_len);
  INLINE_STACK_PUSH(sv_2mortal(result));
  INLINE_STACK_PUSH(sv_2mortal(newSViv(result_count)));
  INLINE_STACK_DONE;
  return;
}


