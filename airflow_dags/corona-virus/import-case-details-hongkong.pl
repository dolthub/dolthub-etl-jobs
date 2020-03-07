#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use File::Basename;

my $url       = 'https://wars.vote4.hk/en/cases';
my $html_file = 'hk-cases.html';

my $data = {};

my $debug = 0;

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_cases_html($url, $html_file);

extract_data($html_file, $data);

import_data($url, $data);

publish($url);

sub download_cases_html {
    my $url  = shift;
    my $file = shift;

    my $dirname = dirname(__FILE__);

    run_command('npm install puppeteer',
		'Cannot install puppeteer');

    run_command("cp $dirname/scroll-to-bottom-scraper/scroll-to-bottom-scrape.js .",
		"Could not copy node script");
    
    my $script = "$dirname/scroll-to-bottom-scraper/scroll-to-bottom-scrape.js";
    
    run_command("node scroll-to-bottom-scrape.js $url $file",
		"Could not execute node infinite scroll scraper");
}

sub extract_data {
    my $html_file = shift;
    my $data = shift;
    
    open HTML, "<$html_file" or die "Could not open $html_file";

    # Slurp the whole thing into a string
    local $/;
    my $html_string = <HTML>;

    # Remove comments
    $html_string =~ s/<!--.*?-->//sg;

    die "Could not remove all comments" if ( $html_string =~ /<!--.*?-->/sg );
    
    my $found = 1;
    my $i = 1;
    while ( $found ) {
	my $regex = qr/(#$i\s+\((Confirmed|Probable)\).+?Confirmed date.{17})/s;
	if ( $html_string =~ $regex ) {
	    print "Found case $i\n";
	    print $1 . "\n" if $debug;
	    my $snippet = $1;

	    my $case_id = $i;
	    my $age;
	    my $sex;
	    my $onset_date;
	    my $confirmed_date;
	    my $status;

	    if ( $snippet =~ />Age\s+(\d+)\s+(\w+|dashboard\.gender_)?</ ) {
		$age = $1;
		$sex = $2;

		if ( $sex eq 'dashboard.gender_' ) {
		    print "Error detected on website. Ignore sex for $case_id\n";
		    $sex = undef;
		}

		print "Age: $age Sex: $sex\n" if $debug;
		$data->{$case_id}{'age'} = $age if $age;
		$data->{$case_id}{'sex'} = substr($sex,0,1) if $sex;
	    } else {
		die "Could not find age or sex for case $case_id";
	    }

	    if ( $snippet =~ /Onset\sdate<\/p><b>(\d{4}-\d{2}-\d{2})<\/b>/ ) {
		$onset_date = "$1 00:00:00";
		print "Onset date: $onset_date\n" if $debug;
		$data->{$case_id}{'symptomatic_date'} = $onset_date;
	    } else {
		print "No onset date for case: $case_id\n" if $debug;
	    }

	    if ( $snippet =~ /Confirmed\sdate<\/p><b>(\d{4}-\d{2}-\d{2})$/ ) {
                $confirmed_date = "$1 00:00:00";
		print "Confirmed date: $confirmed_date\n" if $debug;
		$data->{$case_id}{'confirmed_date'} = $confirmed_date;
            } else {
                print "No confirmed date for case: $case_id\n" if $debug;
            }

	    $status = 'In hospital'
		if ( $snippet =~ /(Hospitalised|Critical|Serious)/ );
	    $status = 'Recovered' if ( $snippet =~ /Discharged/ );
	    $status = 'Deceased' if ( $snippet =~ /Deceased/ );
	    die "No status found" unless $status;
	    print "Status: $status\n" if $debug;
	    
	    $data->{$case_id}{'current_status'} = $status;

	} else {
	    print "Could not find case $i. Stopping search\n";
	    $found = 0;
	}
	$i++;
    }
}
    
sub import_data {
    my $url  = shift;
    my $data = shift;

    my $sql_file = 'case-details-hk.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    print SQL "delete from case_details where source='$url';\n";

    foreach my $case_id ( keys %{$data} ) {
        my $sql = "insert into case_details (source, place_id, case_id";
        foreach my $column_name ( keys %{$data->{$case_id}} ) {
            $sql .= ", $column_name";
        }
        $sql .= ") values ('$url', 32, $case_id";
        foreach my $column_name ( keys %{$data->{$case_id}} ) {
            my $value = $data->{$case_id}{$column_name};
            if ( $value =~ /^\d+$/ ) {
                $sql .= ", $value";
            } elsif ( $value eq '') {
                $sql .= ", NULL";
            } else {
                $sql .= ", '$value'";
            }
        }
        $sql .= ");\n";
        print SQL $sql;
    }

    close SQL;

    run_command("dolt sql < $sql_file", "Could not execute generated SQL");
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
        "Automated import of case_details table downloaded from $url at $datestring GMT";

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
