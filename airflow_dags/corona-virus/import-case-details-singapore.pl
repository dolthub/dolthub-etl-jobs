#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use HTML::TableExtract;

my $url  = 'https://www.wuhanvirus.sg/cases/search';
my $html = 'cases-search.html'; 

# Assume cases_id is the first column and use it to key the data hash
my @columns = (
    'case_name',
    'age',
    'sex',
    'nationality',
    'current_status',
    'case_type',
    'country_of_origin',
    'symptomatic_to_confirmed_days',
    'confirmed_to_recover_days',
    'symptomatic_date',
    'confirmed_date',
    'recovered_date',
    'displayed_symptoms',
    );

my @import_columns = (
    'case_name',
    'age',
    'sex',
    'nationality',
    'current_status',
    'symptomatic_date',
    'confirmed_date',
    'recovered_date',
    );

my $data = {};

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_search_results($url, $html);

$data = extract_data($html, \@columns, $data);

import_data($url, $data, \@import_columns);

publish($url);

sub download_search_results {
    my $url  = shift;
    my $html = shift;

    run_command("curl -f -L -o $html '$url'",
		"Could not download $url");
}

sub extract_data {
    my $html    = shift;
    my $columns = shift;
    my $data    = shift;

    open HTML, "<$html" or die "Could not open $html";

    # Slurp the whole thing into a string
    local $/;
    my $html_string = <HTML>;

    my $cases_table;
    if ( $html_string =~ /(<table\s+.*id="casesTable">.*<\/table>)/sg ) {
	$cases_table = $1;
    } else {
	die "Did not match cases table\n"
    }

    my $table_extract = HTML::TableExtract->new();
    $table_extract->parse($cases_table);

    my $table = $table_extract->table(0, 0);
    
    my $header = 1;
    foreach my $row ( $table->rows() ) {
	if ( $header ) {
	    $header = 0;
	    next;
	}
	my $i = 0;
	my $case_id = shift @{$row};
	foreach my $col ( @{$row} ) {
	    #Remove leading and traling space
	    $col =~ s/^\s+//g;
	    $col =~ s/\s+$//g;

	    $col = '' if ( $col eq '-' );

	    die "Number of columns does not match website"
		unless $columns->[$i];
	    
	    if ( $columns->[$i] eq 'sex' ) {
		$col = substr($col, 0, 1);
	    }

	    if ( $col && $columns->[$i] =~ /_date/ ) {
		$col = parse_date($col);
	    }
	    
	    $data->{$case_id}{$columns->[$i]} = $col;
	    $i++;
	}
    }

    return $data;
}

sub parse_date {
    my $date = shift;

    my %months = (
	'Jan' => '01',
	'Feb' => '02',
	'Mar' => '03',
	'Apr' => '04',
	'May' => '05',
	'Jun' => '06',
	'Jul' => '07',
	'Aug' => '08',
	'Sep' => '09',
	'Oct' => '10',
	'Nov' => '11',
	'Dec' => '12'
	);

    my ($day, $month, $year);
    if ( $date =~ /^(\d+)\w+\,\s+(\w+)\s+(\d{4})/ ) {
	$day   = $1;
	$month = $months{$2} or die "Could not match month: $2";
	$year  = $3;

	$day = "0$day" if $day < 10;
    } else {
	die "Date does not match assumed format: $date";
    }

    return "$year-$month-$day 00:00:00";
}

sub import_data {
    my $url  = shift;
    my $data = shift;
    my $cols = shift;

    my $sql_file = 'case-details.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    print SQL "delete from case_details where source='$url';\n";

    foreach my $case_id ( keys %{$data} ) {
	my $sql = "insert into case_details (source, place_id, case_id";
	foreach my $column_name ( @{$cols} ) {
	    $sql .= ", $column_name";
	}
	$sql .= ") values ('$url', 29, $case_id";
	foreach my $column_name ( @{$cols} ) {
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

    # Grab the latest just in case something changed while we were running
    run_command('dolt pull', 'dolt pull failed');
    
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
