#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use XML::Simple;

# Configuration

# This script assumes you have downloaded a release from
# https://github.com/globalwordnet/english-wordnet/releases,
# unpacked it, and assembled it into a single XML file using merge.py
my $db_file = 'english-wordnet-3.1/wn31.xml';

my $organization = 'Liquidata';
my $repo         = 'english-wordnet';
my $clone_path   = "$organization/$repo"; 

my %expected_lemma_keys = (
    'writtenForm'  => 'writtenForm',
    'partOfSpeech' => 'partOfSpeech',
    );

my %expected_sense_keys = (
    'id' => 'SenseID',
    'synset' => 'SynsetID',
    'dc:identifier'  => 'dc_identifier',
    );

my %expected_sense_relation_keys = (
    'target' => 'TargetSenseID',
    'relType' => 'relType',
    );

my %expected_synset_keys = (
    'Definition' => 'Definition',
    'dc:subject' => 'dc_subject',
    'ili' => 'ili',
    'partOfSpeech' => 'partOfSpeech',
    'ILIDefinition' => 'ILIDefinition',
    );

my %expected_synset_relation_keys = (
    'target' => 'TargetSynsetID',
    'relType' => 'relType',
    );

import_data($db_file);

sub import_data {
    my $file = shift;

    my $data = XMLin($file);

    my $lemmas               = [];
    my $senses               = [];
    my $sense_relations      = [];
    my $syntactic_behaviours = [];

    my $lexical_entries = $data->{'Lexicon'}{'LexicalEntry'};

    process_lexical_entries($lexical_entries,
    			    $lemmas,
    			    $senses,
    			    $sense_relations,
    			    $syntactic_behaviours);
    
    import_table('Lemmas', $lemmas);
    import_table('Senses', $senses);
    import_table('SenseRelations', $sense_relations);
    import_table('SyntacticBehaviours', $syntactic_behaviours);

    my $synsets          = [];
    my $synset_relations = [];
    my $synset_examples  = [];

    my $synsets_in = $data->{'Lexicon'}{'Synset'};

    process_synsets($synsets_in,
		    $synsets,
		    $synset_relations,
		    $synset_examples);

    import_table('Synsets', $synsets);
    import_table('SynsetRelations', $synset_relations);
    import_table('SynsetExamples', $synset_examples);
}

sub process_lexical_entries {
    my $lexical_entries      = shift;
    my $lemmas               = shift;
    my $senses               = shift;
    my $sense_relations      = shift;
    my $syntactic_behaviours = shift;

    foreach my $lemma_id ( keys %{$lexical_entries} ) {
	# These contain a Lemma, one to many Senses, and 0 to many
	# SyntacticBehavious

	# Lemmas
	
	my $lemma_in = $lexical_entries->{$lemma_id}{'Lemma'};
	my %lemma_out = ( 'LemmaID' => $lemma_id );
	flatten_entry($lemma_in, \%lemma_out, \%expected_lemma_keys);
	push @{$lemmas}, \%lemma_out;

	# Senses
	
	my $senses_in = $lexical_entries->{$lemma_id}{'Sense'};

	# For a lemma with a single sense id is a key.
	# Otherwise, we have multiple senses for a lemma.
	my $sense_id;
	if ( $senses_in->{'id'} ) {
	    $sense_id = $senses_in->{'id'};
	    delete $senses_in->{'id'};
	    $senses_in = { $sense_id => $senses_in };
	}

	foreach my $id ( keys %{$senses_in} ) {
	    my $sense_in  = $senses_in->{$id};
	    my %sense_out = ( 'LemmaID' => $lemma_id, 'SenseID' => $id );

	    # There can be 0, 1 or many sense relations so we           
	    # see if there is 1 and insert it into a single element      
	    # array
	    if ( $sense_in->{'SenseRelation'} ) {
		my $sense_relations_in = $senses_in->{$id}{'SenseRelation'};
		if ( ref($sense_relations_in) eq 'HASH' ) {
		    $sense_relations_in = [$sense_relations_in];
		}

		# There are dupes in SenseRelations. Ugh.
		# Going to dedupe
		my $dedupe = {};
		foreach my $sense_relation_in ( @{$sense_relations_in} ){

		    # dedupe
		    my $synset_id = $sense_in->{'synset'};
		    my $target    = $sense_relation_in->{'target'};
		    my $type      = $sense_relation_in->{'relType'};
		    next if $dedupe->{$synset_id}{$id}{$target}{$type};
		    $dedupe->{$synset_id}{$id}{$target}{$type} = 1;

		    
		    # Use a hash to guarantee a new reference every time
		    my %sense_relation_out = (
			'SynsetID' => $synset_id,
			'SenseID'  => $id
		    );

		    flatten_entry($sense_relation_in,
				  \%sense_relation_out,
				  \%expected_sense_relation_keys);
		    push @{$sense_relations}, \%sense_relation_out;
		}
	    

		# Must delete the key so we have a clean entry
		delete $sense_in->{'SenseRelation'};
	    }
	    flatten_entry($sense_in,
			  \%sense_out,
			  \%expected_sense_keys);
	    push @{$senses}, \%sense_out;
	}

	# Syntactic Behaviours
	my $sbs_in = $lexical_entries->{$lemma_id}{'SyntacticBehaviour'};
	if ( ref($sbs_in)  eq 'HASH' ) {
	    $sbs_in = [$sbs_in];
	}

	# Syntactic Behaviours have dupes so I have to dedupe
	my $dedupe = {};
	foreach my $sb_in ( @{$sbs_in} ) {
	    # Senses come in a space separated list
	    my @senses = split /\s+/, $sb_in->{'senses'};
	    foreach my $sense ( @senses ) {
		my $frame = $sb_in->{'subcategorizationFrame'};
		next if $dedupe->{$lemma_id}{$sense}{$frame};
		$dedupe->{$lemma_id}{$sense}{$frame} = 1;
		    
		my %sb_out = ( 'LemmaID' => $lemma_id,
			       'SenseID' => $sense,
			       'subcategorizationFrame' => $frame );
		push @{$syntactic_behaviours}, \%sb_out;
	    }
	}
    }
}

sub process_synsets {
    my $synsets_in       = shift;
    my $synsets          = shift;
    my $synset_relations = shift;
    my $synset_examples  = shift;

    foreach my $id ( keys %{$synsets_in} ) {
	my $synset_in  = $synsets_in->{$id};
	my %synset_out = ( 'SynsetID' => $id );

	# There can be 0, 1 or many synset relations so we           
	# see if there is 1 and insert it into a single element      
	# array
	if ( $synset_in->{'SynsetRelation'} ) {
	    my $synset_relations_in = $synsets_in->{$id}{'SynsetRelation'};
	    if ( ref($synset_relations_in) eq 'HASH' ) {
		$synset_relations_in = [$synset_relations_in];
	    }
	    
	    foreach my $synset_relation_in ( @{$synset_relations_in} ){
		my %synset_relation_out = ( 'SynsetID' => $id );
		flatten_entry($synset_relation_in,
			      \%synset_relation_out,
			      \%expected_synset_relation_keys);
		push @{$synset_relations}, \%synset_relation_out;
	    }
	    
	    delete $synset_in->{'SynsetRelation'};
	}

	if ( $synset_in->{'Example'} ) {
	    my $examples_in = $synset_in->{'Example'};
	    unless ( ref($examples_in) ) {
		$examples_in = [ $examples_in ];
	    }

	    foreach my $example_in ( @{$examples_in} ) {
		my %example_out = ( 'SynsetID' => $id,
				    'Example' => $example_in );
		push @{$synset_examples}, \%example_out;
	    }
	    delete $synset_in->{'Example'};
	}
	
	flatten_entry($synsets_in->{$id},
		      \%synset_out,
		      \%expected_synset_keys);
	push @{$synsets}, \%synset_out;
    }	   
}

sub flatten_entry {
    my $input         = shift;
    my $output        = shift;
    my $key_transform = shift;

    foreach my $key ( keys %{$input} ) {
	transform_entry($input, $output, $key, $key_transform);
    }
}

sub transform_entry{
    my $input         = shift;
    my $output        = shift;
    my $key           = shift;
    my $key_transform = shift;

    my $transformed_key = $key_transform->{$key};
    die "Unexpected Key: $key" unless $transformed_key;
    $output->{$transformed_key} = $input->{$key};
}

sub import_table {
    my $name = shift;
    my $data = shift;

    open SQL, ">$name.sql" or die "Could not open $name.sql";

    print SQL "delete from $name;\n";

    foreach my $entry ( @{$data} ) {
	my $sql = "insert into $name (";
	my $columns = join ', ', sort keys %{$entry};
	$sql .= "$columns) values (";
	my $first = 1;
	foreach my $key ( sort keys %{$entry} ) {
	    $sql .= ', ' unless $first;
	    $first = 0;
	    my $value = escape_sql($entry->{$key});
	    $sql .= "\"$value\"";
	}
	$sql .= ");\n";
	print SQL $sql;
    }

    close SQL;
    
    run_command("dolt sql < $name.sql", "Could not run $name.sql");
}

sub escape_sql {
    my $string = shift;

    # die "Passed undefined string" unless $string; 

    # Looks like some of the quotes are already escaped
    $string =~ s/\\[^\\"]/\\\\/g;
    $string =~ s/^"/\\"/g;
    $string =~ s/[^\\"]"/\\"/g;
    

    return $string;
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
