#!/usr/bin/perl -w 

use strict;

use File::Path qw(rmtree);

# Configuration 
my $url = 'http://wordnetcode.princeton.edu/wn3.1.dict.tar.gz';

my $current_md5 = '072f83df0ca7c387a44dd7ef5b199150';

my $tmpdir    = 'data';
my $base_path = 'dict';

my $lemma_base = "$tmpdir/$base_path/index";
my $synset_base = "$tmpdir/$base_path/data";

# Data to make supporting tables.
#
# This data was hand copied from the documentation here: 
# https://wordnet.princeton.edu/documentation/wndb5wn
my @types = ('noun', 'verb', 'adj', 'adv');

my %type_map = (
    'n' => 'noun',
    'v' => 'verb',
    'a' => 'adjective',
    's' => 'adjective satellite',
    'r' => 'adverb'
    );

my $ptr_symbol_map = {
    'n' => {
	'!' => 'Antonym', 
	'@' => 'Hypernym', 
	'@i' => 'Instance Hypernym', 
	'~'  => 'Hyponym', 
	'~i' => 'Instance Hyponym',
	'#m' => 'Member holonym',
	'#s' => 'Substance holonym', 
	'#p' => 'Part holonym',
	'%m' => 'Member meronym',
	'%s' => 'Substance meronym',
	'%p' => 'Part meronym',
	'=' => 'Attribute',
	'+' => 'Derivationally related form',
	';c' => 'Domain of synset - TOPIC',
	'-c' => 'Member of this domain - TOPIC',
	';r' => 'Domain of synset - REGION', 
	'-r' => 'Member of this domain - REGION',
	';u' => 'Domain of synset - USAGE',
	'-u' => 'Member of this domain - USAGE'
    },
    'v' => {
	'!' => 'Antonym',
	'@' => 'Hypernym',
	'~' => 'Hyponym',
	'*' => 'Entailment',
	'>' => 'Cause',
	'^' => 'Also see',
	'$' => 'Verb Group',
	'+' => 'Derivationally related form',
	';c' => 'Domain of synset - TOPIC',
	';r' => 'Domain of synset - REGION',
	';u' => 'Domain of synset - USAGE',
    },
    'a' => {
	'!' => 'Antonym', 
	'&' => 'Similar to', 
	'<' => 'Participle of verb', 
	'\\\\' => 'Pertainym (pertains to noun)', 
	'=' => 'Attribute', 
	'^' => 'Also see', 
	';c' => 'Domain of synset - TOPIC', 
	';r' => 'Domain of synset - REGION', 
	';u' => 'Domain of synset - USAGE', 
    },
    'r' => {
	'!' => 'Antonym',
	'\\\\' => 'Derived from adjective',
        ';c' => 'Domain of synset - TOPIC',
        ';r' => 'Domain of synset - REGION',
        ';u' => 'Domain of synset - USAGE',
    }
};

my $lexs = {
    '00' => {
	'name' => 'adj.all',
	'desc' => 'all adjective clusters'
    },
    '01' => {
	'name' => 'adj.pert', 
	'desc' => 'relational adjectives (pertainyms)',
    },
    '02' => {
	'name' => 'adv.all',
	'desc' => 'all adverbs',
    },
    '03' => {
	'name' => 'noun.Tops',
	'desc' => 'unique beginner for nouns',
    },
    '04' => {
	'name' => 'noun.act',
	'desc' => 'nouns denoting acts or actions',
    },
    '05' => {
	'name' => 'noun.animal',
	'desc' => 'nouns denoting animals',
    },
    '06' => {
	'name' => 'noun.artifact',
	'desc' => 'nouns denoting man-made objects',
    },
    '07' => {
	'name' => 'noun.attribute',
	'desc' => 'nouns denoting attributes of people and objects',
    },
    '08' => {
	'name' => 'noun.body',
	'desc' => 'nouns denoting body parts',
    },
    '09' => {
	'name' => 'noun.cognition',
	'desc' => 'nouns denoting cognitive processes and contents',
    },
    '10' => {
	'name' => 'noun.communication',
	'desc' => 'nouns denoting communicative processes and contents',
    },
    '11' => {
	'name' => 'noun.event',
	'desc' => 'nouns denoting natural events',
    },
    '12' => {
	'name' => 'noun.feeling',
	'desc' => 'nouns denoting feelings and emotions',
    },
    '13' => {
	'name' => 'noun.food',
	'desc' => 'nouns denoting foods and drinks',
    },
    '14' => {
	'name' => 'noun.group',
	'desc' => 'nouns denoting groupings of people or objects',
    },
    '15' => {
	'name' => 'noun.location',
	'desc' => 'nouns denoting spatial position',
    },
    '16' => {
	'name' => 'noun.motive',
	'desc' => 'nouns denoting goals',
    },
    '17' => {
	'name' => 'noun.object',
	'desc' => 'nouns denoting natural objects (not man-made)',
    },
    '18' => {
	'name' => 'noun.person',
	'desc' => 'nouns denoting people',
    },
    '19' => {
	'name' => 'noun.phenomenon',
	'desc' => 'nouns denoting natural phenomena',
    },
    '20' => {
	'name' => 'noun.plant',
	'desc' => 'nouns denoting plants',
    },
    '21' => {
	'name' => 'noun.possession',
	'desc' => 'nouns denoting possession and transfer of possession',
    },
    '22' => {
	'name' => 'noun.process',
	'desc' => 'nouns denoting natural processes',
    },
    '23' => {
	'name' => 'noun.quantity',
	'desc' => 'nouns denoting quantities and units of measure',
    },
    '24' => {
	'name' => 'noun.relation',
	'desc' => 'nouns denoting relations between people or things or ideas',
    },
    '25' => {
	'name' => 'noun.shape',
	'desc' => 'nouns denoting two and three dimensional shapes',
    },
    '26' => {
	'name' => 'noun.state',
	'desc' => 'nouns denoting stable states of affairs',
    },
    '27' => {
	'name' => 'noun.substance',
	'desc' => 'nouns denoting substances',
    },
    '28' => {
	'name' => 'noun.time',
	'desc' => 'nouns denoting time and temporal relations',
    },
    '29' => {
	'name' => 'verb.body',
	'desc' => 'verbs of grooming, dressing and bodily care',
    },
    '30' => {
	'name' => 'verb.change',
	'desc' => 'verbs of size, temperature change, intensifying, etc.',
    },
    '31' => {
	'name' => 'verb.cognition',
	'desc' => 'verbs of thinking, judging, analyzing, doubting',
    },
    '32' => {
	'name' => 'verb.communication',
	'desc' => 'verbs of telling, asking, ordering, singing',
    },
    '33' => {
	'name' => 'verb.competition',
	'desc' => 'verbs of fighting, athletic activities',
    },
    '34' => {
	'name' => 'verb.consumption',
	'desc' => 'verbs of eating and drinking',
    },
    '35' => {
	'name' => 'verb.contact',
	'desc' => 'verbs of touching, hitting, tying, digging',
    },
    '36' => {
	'name' => 'verb.creation',
	'desc' => 'verbs of sewing, baking, painting, performing',
    },
    '37' => {
	'name' => 'verb.emotion',
	'desc' => 'verbs of feeling',
    },
    '38' => {
	'name' => 'verb.motion',
	'desc' => 'verbs of walking, flying, swimming',
    },
    '39' => {
	'name' => 'verb.perception',
	'desc' => 'verbs of seeing, hearing, feeling',
    },
    '40' => {
	'name' => 'verb.possession',
	'desc' => 'verbs of buying, selling, owning',
    },
    '41' => {
	'name' => 'verb.social',
	'desc' => 'verbs of political and social activities and events',
    },
    '42' => {
	'name' => 'verb.stative',
	'desc' => 'verbs of being, having, spatial relations',
    },
    '43' => {
	'name' => 'verb.weather',
	'desc' => 'verbs of raining, snowing, thawing, thundering',
    },
    '44' => {
	'name' => 'adj.ppl',
	'desc' => 'participial adjectives',
    },
};

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'word-net';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
	    "Could not clone repo $clone_path");

chdir($repo);

run_command("mkdir $tmpdir", "Could not create $tmpdir");

download_and_unpack($url, $tmpdir, $current_md5);

create_supporting_tables($lexs, \%type_map, $ptr_symbol_map);
import_data($lemma_base, $synset_base, \@types);

publish($url);

rmtree($tmpdir);

sub download_and_unpack {
    my $url         = shift;
    my $tmpdir      = shift;
    my $current_md5 = shift;

    chdir($tmpdir);
    run_command("curl -L -O $url", "Could not download $url");
    
    my $file = `ls -1 .`;
    chomp($file);

    my $md5 = `md5sum $file`;
    my @split_md5 = split(/\s+/, $md5);
    $md5 = $split_md5[0];

    if ( $current_md5 eq $md5 ) {
	chdir('..');
	rmtree($tmpdir);
	print "Current data matches downloaded data. Exiting...\n";
	exit;
    } else {
	run_command("tar -xzvf $file", "Could not unpack $file");
	chdir('..');
    }
}

sub import_data {
    my $lemma_base  = shift;
    my $synset_base = shift;
    my $types       = shift;

    process_data_files($synset_base, $types);
}

sub create_supporting_tables {
    my $lexs          = shift;
    my $synset_types  = shift;
    my $pointer_types = shift;

    open LEXSSQL, '>lexs.sql' or die 'Could not open synset.sql';

    foreach my $lex_num ( keys %{$lexs} ) {
	my $lex_name = $lexs->{$lex_num}{'name'};
	my $lex_desc = $lexs->{$lex_num}{'desc'};
	print LEXSSQL "insert into lexs (lex_num,name,contents) values('$lex_num','$lex_name','$lex_desc');\n";
    }
    
    close LEXSSQL;

    run_command("dolt sql -q 'delete from lexs'", 
		'Could not delete lexs table');
    run_command('dolt sql < lexs.sql', 'Could not execute lexs.sql');
    unlink('lexs.sql');

    open SYNSETTYPESSQL, '>synset-types.sql' 
	or die 'Could not open synset-types.sql';
    
    foreach my $type ( keys %{$synset_types} ) {
	my $synset_desc = $synset_types->{$type};
	print SYNSETTYPESSQL "insert into synset_types (synset_type,synset_type_description) values ('$type','$synset_desc');\n";
    }
    
    close SYNSETTYPESSQL;
    run_command("dolt sql -q 'delete from synset_types'",
	       'Could not delete synset_types table');
    run_command('dolt sql < synset-types.sql', 'Could not execute synset-types.sql');
    unlink('synset-types.sql');

    open POINTERTYPESSQL, '>pointer-types.sql' 
	or die 'Could not open pointer-types.sql';

    foreach my $synset_type ( keys %{$pointer_types} ) {
	foreach my $ptr_symbol ( keys %{$pointer_types->{$synset_type}} ) {
	    my $ptr_desc = $pointer_types->{$synset_type}{$ptr_symbol};
	    
	    print POINTERTYPESSQL "insert into pointers (synset_type,pointer_type_symbol,pointer_description) values ('$synset_type','$ptr_symbol','$ptr_desc');\n";
	}
    }

    close POINTERTYPESSQL;
    run_command("dolt sql -q 'delete from pointers'",
	       'Could not delete pointers table');
    run_command('dolt sql < pointer-types.sql', 'Could not execute pointer-types.sql');
    unlink('pointer-types.sql'); 
}

sub process_data_files {
    my $base  = shift;
    my $types = shift;

    # Open SQL files for each table we'll write
    open SYNSETSQL, '>synsets.sql' or die "Could not open synsets.sql";
    open WORDSQL, '>words.sql' or die "Could not open words.sql";
    open SYNSETPTRSSQL, '>synset-pointers.sql' 
	or die 'Could not open synset-pointers.sql';
    open WORDSYNSETSQL, '>words-synsets.sql' 
	or die 'Could not open words-synsets.sql';

    my $words_dedupe = {};
    foreach my $type ( @{$types} ) {
        my $file = "$base.$type";

        open DATA, "<$file" or die "Could not open $file";

	while ( my $line = <DATA> ) {
	    # Comments in the file start with spaces
	    next if ( $line =~ /^\s+/ );
            
	    chomp($line);
	    $line =~ s/'/''/g;
	    $line =~ s/\\/\\\\/g;

	    my ($data, $gloss) = split(/\s+\|\s+/, $line);

	    $gloss =~ s/\s+$//g;

	    my @fields = split(/\s+/, $data);
	    
	    # The use of byte offset as a key makes this file very hard to 
	    # change. If a new version comes out it's likely our primary keys 
	    # will all change unless only synsets are added at the end of 
	    # these files
	    my $synset_offset = shift(@fields); 
	    my $lex_filenum   = shift(@fields);  
	    my $synset_type   = shift(@fields);  
	    my $word_cnt      = hex(shift(@fields));

	    print SYNSETSQL "insert into synsets(synset_id,synset_type,lex_num,gloss) values ('$synset_offset','$synset_type','$lex_filenum','$gloss');\n";

	    # Each word has a lex id used for disambiguation. I'll hash those.
	    my %words;
	    for ( my $i=0; $i<$word_cnt; $i++ ) {
		my $word = shift(@fields);
		$word =~ s/_/ /g;
		my $lex_id = shift(@fields);
		$words{$word} = $lex_id;

		# Don't create duplicate inserts for words
		unless ( exists($words_dedupe->{$word}{$lex_id}) ) {
		    $words_dedupe->{$word}{$lex_id} = 1;
		    print WORDSQL "insert into words(word,lex_id,syntactic_category) values ('$word','$lex_id','$synset_type');\n";
		}
			 
		my $word_num = $i+1;
		print WORDSYNSETSQL "insert into words_synsets(word,lex_id,synset_id,synset_type,word_num) values ('$word','$lex_id','$synset_offset','$synset_type',$word_num);\n";
	    }

	    my $pointer_cnt = shift(@fields);
	    $pointer_cnt += 0;
	    my $pointers = [];
	    my $dedupe_ptrs = {};
	    for ( my $i=0; $i<$pointer_cnt; $i++ ) {
		my $pointer_symbol    = shift(@fields);
		my $ptr_synset_offset = shift(@fields);
		my $ptr_synset_type   = shift(@fields);
		my $source_target     = shift(@fields);
		
		unless ( exists $dedupe_ptrs->{$pointer_symbol}{$ptr_synset_offset}{$ptr_synset_type}{$source_target} ) {
		    $dedupe_ptrs->{$pointer_symbol}{$ptr_synset_offset}{$ptr_synset_type}{$source_target} = 1;
		    # If source_target equal '0000' it means a semantic 
		    # pointer, otherwise it is a lexical pointer between the 
		    # word number of the first two digits (in hex) to the 
		    # word number of the second two digits (in hex)
		    if ( $source_target eq '0000' ) {
			print SYNSETPTRSSQL "insert into synset_pointers (from_synset_id, from_synset_type, to_synset_id, to_synset_type, pointer_type_symbol, semantic_pointer,lexical_pointer,from_word_num,to_word_num) values ('$synset_offset','$synset_type','$ptr_synset_offset','$ptr_synset_type','$pointer_symbol',true,false,0,0);\n";
		    } else {
			my $from_word_num = hex(substr($source_target,0,2));
			my $to_word_num = hex(substr($source_target,2,2));
			print SYNSETPTRSSQL "insert into synset_pointers (from_synset_id, from_synset_type, to_synset_id, to_synset_type, pointer_type_symbol, semantic_pointer,lexical_pointer,from_word_num,to_word_num) values ('$synset_offset','$synset_type','$ptr_synset_offset','$ptr_synset_type','$pointer_symbol',false,true,$from_word_num,$to_word_num);\n";
		    }
		}
	    }
	    
	    if ($type eq 'verb' ) {
		# Process frames here
	    }
	}
    }

    my %sql_files = ('synsets.sql'         => 'synsets', 
		     'words.sql'           => 'words', 
		     'synset-pointers.sql' => 'synset_pointers', 
		     'words-synsets.sql'   => 'words_synsets');
    foreach my $sql_file ( keys %sql_files ) {
	my $table_name = $sql_files{$sql_file};
	# Delete the current data in the tables because we want the data in 
	# dolt to mirror the data in the WordNet file exactly. 
	run_command("dolt sql -q 'delete from $table_name'",
		    "Could not delete $table_name table");
	run_command("dolt sql < $sql_file", "Could not execute $sql_file");
	unlink($sql_file);
    }
    
    close SYNSETSQL;
    close WORDSQL;
    close SYNSETPTRSSQL;
    close WORDSYNSETSQL;
}

sub publish {
    my $url = shift;

    unless ( `dolt diff` ) {
	print "Nothing changed in import. Not generating a commit\n";
	exit 0;
    }

    run_command('dolt add .', 'dolt add command failed');

    my $datestring = gmtime();
    my $commit_message = 
	"Automated import of new data downloaded from $url at $datestring GMT";

    run_command('dolt commit -m "' . $commit_message . '"', 
		"dolt commit failed");

    run_command('dolt push origin master', 'dolt push failed');
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
