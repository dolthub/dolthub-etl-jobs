#!/usr/bin/perl -w 

use strict;

use File::Path qw(rmtree);
use JSON::Parse qw(parse_json json_file_to_perl);

my $url_base = 'https://github.com/facebookresearch/Neural-Code-Search-Evaluation-Dataset/raw/master/data/';

my %files = (
    '287_android_questions.json' => 'd4e6db7c7faeb0d5645d28eeb3b70650',
    'android_repositories_download_links.txt' => '9ed809cc1d75f9a9f1873bd9382cf696',
    'score_sheet.csv' => '25b2811bb9efae625e6988b1b56bd36e',
    'search_corpus_1.tar.gz' => 'cbb0d6461284cd64fca0bf39d9c194a3',
    'search_corpus_2.tar.gz' => '29755fc02eadf98c5b86d8348099ff69',
    );

my $tmpdir = 'data';

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'neural-code-search-evaluation';
my $clone_path   = "$organization/$repo";
run_command("dolt clone $clone_path",
            "Could not clone repo $clone_path");

chdir($repo);

my @new = download_and_unpack($url_base, \%files, $tmpdir);

unless ( @new ) {
    print "No new files to import\n";
    exit;
}

import_files($tmpdir, @new);

publish($url_base);

rmtree($tmpdir);

sub download_and_unpack {
    my $url_base = shift;
    my $files    = shift;
    my $tmpdir   = shift;

    run_command("mkdir $tmpdir", "Could not create $tmpdir");
    chdir($tmpdir);
    
    my @new;
    foreach my $file ( keys %{$files} ) {
	my $current_md5 = $files->{$file};
	my $url         = "$url_base/$file";
	run_command("curl -L -O $url", "Could not download $url");

	my $md5 = `md5sum $file`;
	my @split_md5 = split(/\s+/, $md5);
	$md5 = $split_md5[0];
	
        next if ( $md5 eq $current_md5 );

	push @new, $file; 
	
	if ( $file =~ /\.tar\.gz/ ) {
	    run_command("tar -xzvf $file", "Could not unzip $file");
	}
    }
	
    chdir('..');

    return @new;
}

sub import_files {
    my $tmpdir = shift;
    my @files = @_;

    my $finished_search_corpus = 0;
    foreach my $file ( @files ) {
	if ( $file eq 'android_repositories_download_links.txt' ) {
	    import_download_links("$tmpdir/$file");
	} elsif ( $file eq 'score_sheet.csv' ) {
	    import_score_sheet("$tmpdir/$file");
	} elsif ( $file eq '287_android_questions.json' ) {
	    import_questions("$tmpdir/$file");
	} elsif ( $file =~ /search_corpus/ && !$finished_search_corpus ) {
	    import_search_corpus($tmpdir, 
				 'search_corpus_1.jsonl',
				 'search_corpus_2.jsonl');
	    $finished_search_corpus = 1;
	}
    }
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

sub import_download_links {
    my $path = shift;

    my $sql_file = 'links.sql';
    open FILE, "<$path" or die "Could not open $path";
    open SQL, ">$sql_file" or die "Could not open $sql_file"; 

    while ( my $line = <FILE> ) {
	chomp($line);
	$line = clean_input($line);
	my $sql = "insert into download_links (url) values ('$line');\n";
	print SQL $sql;
    }

    close FILE;
    close SQL;

    run_command("dolt sql -q 'delete from download_links'",
		"Could not delete download_links table");
    run_command("dolt sql < $sql_file", "Could not execute SQL in $sql_file");
    unlink($sql_file);
}

sub import_score_sheet {
    my $path = shift;

    my $sql_file = 'scores.sql';
    open FILE, "<$path" or die "Could not open $path";
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    my $skip = 1;
    while ( my $line = <FILE> ) {
	# Skip the header line
	if ( $skip ) {
	    $skip = 0;
	    next;
	}
	
        chomp($line);
	chop($line) if ($line =~ m/\r$/);
        
	$line = clean_input($line);

	my @fields = split(/,/, $line);
	
        my $sql = "insert into score_sheet (num,stackoverflow_id,ncs_frank,ncs_postrank_frank,unif_android_frank,unif_stackoverflow_frank) values (";
	
	foreach my $field ( @fields ) {
	    $field = 'NULL' if ( $field eq 'NF' );
	    
	    $sql .= "$field,";
	}
	$sql =~ s/,$//g;

	$sql .= ");\n";
        print SQL $sql;
    }

    close FILE;
    close SQL;

    run_command("dolt sql -q 'delete from score_sheet'",
		"Could not delete score_sheet table");
    run_command("dolt sql < $sql_file", "Could not execute SQL in $sql_file");
    unlink($sql_file);
}

sub import_questions {
    my $path = shift;

    my $question_sql_file = 'questions.sql';
    open QUESTIONSQL, ">$question_sql_file" 
	or die "Could not open $question_sql_file";
    binmode(QUESTIONSQL, ":utf8");

    my $example_sql_file = 'examples.sql';
    open EXAMPLESQL, ">$example_sql_file"
        or die "Could not open $example_sql_file";

    my $data = json_file_to_perl($path);

    my %fields = (
	'stackoverflow_id'    => 'int', 
	'question'            => 'string', 
	'question_url'        => 'string', 
	'question_author'     => 'string', 
	'question_author_url' => 'string',
	'answer'              => 'string',
        'answer_url'          => 'string',
        'answer_author'       => 'string',
        'answer_author_url'   => 'string',
	);

    my $dedupe = {}; 
    foreach my $record ( @{$data} ) {
	my $id = $record->{'stackoverflow_id'};
	if ( !exists($dedupe->{$id}) ) { 
	    $dedupe->{$id} = 1;
	} else {
	    next;
	}

	my $sql = generate_sql_from_json('stackoverflow_questions', 
					 $record, 
					 \%fields);
	print QUESTIONSQL $sql;

	my $i = 0;
	foreach my $example ( @{$record->{'examples'}} ) {
	    $sql = "insert into stackoverflow_questions_examples (stackoverflow_id,search_corpus_id,example_num,example_url) values (";
	    $sql .= "$id,$example,$i,";
	    my $url = $record->{'examples_url'}[$i];
	    $url = clean_input($url);
	    $sql .= "'$url');\n";
	    print EXAMPLESQL $sql;
	}
    }

    close QUESTIONSQL;
    close EXAMPLESQL;

    run_command("dolt sql -q 'delete from stackoverflow_questions'",
		"Could not delete stackoverflow_questions table");
    run_command("dolt sql < $question_sql_file", 
		"Could not execute SQL in $question_sql_file");
    unlink($question_sql_file);

    run_command("dolt sql -q 'delete from stackoverflow_questions_examples'",
		"Could not delete stackoverflow_questions_examples table");
    run_command("dolt sql < $example_sql_file", 
		"Could not execute SQL in $example_sql_file");
    unlink($example_sql_file);    
}

sub import_search_corpus {
    my $base  = shift;
    my @files = @_;

    my $sql_file = 'corpus.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";
    binmode(SQL, ":utf8");

    my %fields = (
	'id'          => 'int', 
	'url'         => 'string',
	'filepath'    => 'string',
	'start_line'  => 'int', 
	'end_line'    => 'int',
	'method_name' => 'string'
	);
    
    foreach my $file ( @files ) {
	open FILE, "<$base/$file" or die "Could not open $base/$file";
	while ( my $line = <FILE> ) {
	    chomp($line);
	    my $json = parse_json($line);

	    my $sql = generate_sql_from_json('search_corpus', $json, \%fields);
	    print SQL $sql;
	}
    }

    close SQL;

    run_command("dolt sql -q 'delete from search_corpus'",
                "Could not delete search_corpus table");
    run_command("dolt sql < $sql_file", "Could not execute SQL in $sql_file");
    unlink($sql_file);   
}

sub generate_sql_from_json {
    my $table_name = shift;
    my $json       = shift;
    my $fields     = shift;

    my $sql = "insert into $table_name (";
    $sql .= join(',', keys(%{$fields}));
    $sql .= ') values (';
    foreach my $field ( keys (%{$fields}) ) {
	my $value = $json->{$field};
	$value = clean_input($value);
	if ( $fields->{$field} eq 'int' ) {
	    $sql .= $value;
	} else {
	    $sql .= "'$value'";
	}
	$sql .= ',';
    }
    $sql =~ s/,$//g;
    $sql .= ");\n";
    
    return $sql;
}

sub clean_input {
    my $string = shift;

    $string =~ s/\\/\\\\/g;
    $string =~ s/'/\\'/g;

    return $string;
}


sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    die "$error\n" if ( $exitcode != 0 );
}
