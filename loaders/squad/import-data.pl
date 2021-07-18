#!/usr/bin/perl -w 

use strict;

use JSON::Parse qw(json_file_to_perl);

my $base_url = 'https://rajpurkar.github.io/SQuAD-explorer/dataset/';

my $train_file = 'train-v2.0.json';
my $dev_file   = 'dev-v2.0.json';

my %current_md5s = (
    'train-v2.0.json' => '62108c273c268d70893182d5cf8df740',
    'dev-v2.0.json'   => '246adae8b7002f8679c027697b0b7cf8'
);

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'squad';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($base_url, \%current_md5s, $train_file, $dev_file);

my $train_data = json_file_to_perl($train_file);
my $dev_data   = json_file_to_perl($dev_file);

my $paragraphs_sql = 'paragraphs.sql';
open PARAGRAPHS, ">$paragraphs_sql" or 
    die "Could not open $paragraphs_sql\n";
binmode(PARAGRAPHS, ":utf8");
print PARAGRAPHS "delete from paragraphs;\n";


my $questions_sql = 'questions.sql';
open QUESTIONS, ">$questions_sql" or
    die "Could not open $questions_sql\n";
binmode(QUESTIONS, ":utf8");
print QUESTIONS "delete from questions;\n";

my $answers_sql = 'answers.sql';
open ANSWERS, ">$answers_sql" or
    die "Could not open $answers_sql\n";
binmode(ANSWERS, ":utf8");
print ANSWERS "delete from answers;\n";

generate_sql($train_data, $dev_data);

close PARAGRAPHS;
close QUESTIONS;
close ANSWERS;

execute_sql($paragraphs_sql, $questions_sql, $answers_sql);

publish($base_url);

sub download_files {
    my $base         = shift;
    my $current_md5s = shift;
    my @files        = @_;

    my $is_changed = 0;
    foreach my $file ( @files ) {
        my $url = $base . $file;
        run_command("curl -L -O $url", "Could not download $url");

	my $md5 = `md5sum $file`;
	my @split_md5 = split(/\s+/, $md5);
	$md5 = $split_md5[0];

	$is_changed = 1 if ( $md5 ne $current_md5s->{$file} );
    }

    unless ( $is_changed ) {
	print "No new data in files. Exiting early...\n";
	exit 0;
    }
}

sub generate_sql {
    my $train_data = shift;
    my $dev_data   = shift;

    my $is_train = 1;
    my $is_dev   = 0;

    parse_file($train_data, $is_train, $is_dev);

    $is_train = 0;
    $is_dev   = 1;

    parse_file($dev_data, $is_train, $is_dev);
}

sub execute_sql {
    my @sql_files = @_;

    foreach my $sql_file ( @sql_files ) {
	run_command("dolt sql < $sql_file", "Could not execute $sql_file");
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


sub parse_file {
    my $data     = shift;
    my $is_train = shift;
    my $is_dev   = shift;

    foreach my $article ( @{$data->{data}} ) {
	parse_article($article, $is_train, $is_dev);
    }
}

sub parse_article {
    my $article  = shift;
    my $is_train = shift;
    my $is_dev   = shift;

    my $title = $article->{title};

    # There are some duplicate entries for paragraphs in this data. I'll 
    # depdupe using a hash. It's fine other for the questions and answers 
    # table becuase those entries will be merged.
    my %paragraph_dedupe;
    foreach my $paragraph ( @{$article->{paragraphs}} ) {
	my $paragraph_text = $paragraph->{context};

	next if ( $paragraph_dedupe{$paragraph_text} );
	$paragraph_dedupe{$paragraph_text} = 1;

	$title          = clean_input($title);
	$paragraph_text = clean_input($paragraph_text);

	print PARAGRAPHS "insert into paragraphs (title, paragraph, is_train, is_dev) values ('$title', '$paragraph_text', $is_train, $is_dev);\n";

	foreach my $qa ( @{$paragraph->{qas}} ) {
	    parse_qa($qa, $paragraph_text);
	}
    }
}

sub parse_qa {
    my $qa             = shift;
    my $paragraph_text = shift;

    my $id            = $qa->{id};
    my $question      = $qa->{question};
    my $is_impossible = $qa->{is_impossible} || 0;

    $question = clean_input($question);

    print QUESTIONS "insert into questions (id, paragraph, question, is_impossible) values ('$id', '$paragraph_text', '$question', $is_impossible);\n";

    # Define these as empty arrays if they don't exist
    $qa->{answers}           ||= [];
    $qa->{plausible_answers} ||= [];
    parse_answers($id, $qa->{answers}, $qa->{plausible_answers});
}

sub parse_answers {
    my $question_id = shift;
    my $answers     = shift;
    my $plausibles  = shift;

    if ( scalar @{$answers} > 0  && scalar @{$plausibles} > 0 ) {
	die "$question_id has both answers and plausible answers\n";
    }
    
    my $is_plausible = scalar @{$plausibles} > 0 ? 1 : 0;
    
    my @all_answers = (@{$answers}, @{$plausibles});

    # We need to count the answers before we generate the insert
    my $counts = {};
    foreach my $answer ( @all_answers ) {
	my $answer_text  = $answer->{text};
        my $answer_start = $answer->{answer_start};

	if ( $counts->{$answer_text}{$answer_start} ) {
	    $counts->{$answer_text}{$answer_start}++;
	} else {
	    $counts->{$answer_text}{$answer_start} = 1;
	}
    }

    # Only insert an answer once
    my $dedupe = {};
    foreach my $answer ( @all_answers ) {
	my $answer_text  = $answer->{text};
	my $answer_start = $answer->{answer_start};
	
	next if ( $dedupe->{$answer_text}{$answer_start} );
        $dedupe->{$answer_text}{$answer_start} = 1;

	die "No count for answer" 
	    unless $counts->{$answer_text}{$answer_start};
	my $count = $counts->{$answer_text}{$answer_start};

	$answer_text = clean_input($answer_text);
	print ANSWERS "insert into answers (id, answer_start, answer, is_plausible, answer_count) values ('$question_id', $answer_start, '$answer_text', $is_plausible, $count);\n";
    }
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

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
