#!/usr/bin/perl -w

use strict;

# Configuration
my $sentences_url    = 
    'http://downloads.tatoeba.org/exports/sentences_detailed.tar.bz2';
my $translations_url = 'http://downloads.tatoeba.org/exports/links.tar.bz2';

my $sentences_csv    = 'sentences_detailed.csv';
my $translations_csv = 'links.csv';

my $sentences_header    = 
    'SentenceID,Language,Text,Username,DateAdded,DateLastModified';
my $translations_header = 'SentenceID,TranslationID';

my $sentences_table    = 'sentences';
my $translations_table = 'translations';

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'tatoeba-sentence-translations';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
	    "Could not clone repo $clone_path");

chdir($repo);

# Download and import the data 
import_new_data($sentences_url, 
		$sentences_csv, 
		$sentences_header, 
		$sentences_table);

import_new_data($translations_url,
		$translations_csv,
		$translations_header, 
		$translations_table);

# Bail here if the data hasn't changed. We could use a way to store some state
# between jobs. That way I could md5 the zip files instead of doing the import
die "No changes to repository" unless `dolt diff`;

# Commit the new data to dolt
run_command('dolt add .', 'Could not add tables');

my $datestring = gmtime();
my $commit_message = "Automated import of new data downloaded from http://downloads.tatoeba.org/exports/ at $datestring GMT";
run_command('dolt commit -m "' . $commit_message . '"', "dolt commit failed");

# Put the new data on DoltHub
run_command('dolt push origin master', 'dolt push failed');

sub import_new_data {
    my $input_url    = shift;
    my $expected_csv = shift;
    my $header_row   = shift;
    my $dolt_table   = shift;
    
    my $tar_input = 'downloaded.tar.bz2';

    my $tmp_input  = $expected_csv;
    my $tmp_output = 'output.csv';

    run_command("curl -L -o $tar_input $input_url",
		"Could not download $input_url");

    run_command("tar -xvf $tar_input", 
		"Could not extract from tar ball $tar_input");

    open INPUT, "<$tmp_input" or die "Could not open $tmp_input\n";
    open OUTPUT, ">$tmp_output" or die "Could not open $tmp_output\n";;

    print OUTPUT "$header_row\n";

    while ( my $line = <INPUT> ) {
	chomp $line;

	# Escape quotes
	$line =~ s/"/""/g;

	# Convert tabs to commas and quote values
	my @fields = split "\t", $line;

	my $fixed_line = '';
	my $i = 0;
	foreach my $field ( @fields ){
	    if ( $field ne '\N' ) {
		$fixed_line .= '"' . $field . '"';
	    }

	    if ( $i == $#fields ) {
		$fixed_line .= "\n";
	    } else {
		$fixed_line .= ",";
	    }
	    $i++;
	}

	print OUTPUT $fixed_line;
    }	 
    
    close INPUT;
    close OUTPUT;
	
    run_command("dolt table import -r $dolt_table $tmp_output",
		"dolt table import failed of $dolt_table");

    unlink($tar_input, $tmp_input, $tmp_output);
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    die "$error\n" unless ( $exitcode == 0 );
}
