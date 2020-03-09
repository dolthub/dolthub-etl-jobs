#!/usr/bin/perl -w 

use strict;

use Text::CSV qw( csv );

my $url_base = 'https://raw.githubusercontent.com/jihoo-kim/Coronavirus-Dataset/master/';
my $csv = 'patient.csv';

my %interesting_cols = (
    'id' => 'case_id',
    'sex' => 'sex',
    'birth_year' => 'age',
    'country' => 'nationality',
    'state' => 'current_status',
    'confirmed_date' => 'confirmed_date',
    'released_date' => 'recovered_date'
    );

my $data = {};
    
# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($url_base, $csv);

extract_data($csv, \%interesting_cols, $data);

import_data($url_base . $csv, $data);

publish($url_base . $csv);

sub download_files {
    my $url_base = shift;
    my $csv      = shift;

    my $url = $url_base . $csv;
    run_command("curl -f -L -o $csv '$url'", "Could not download $url");
}

sub extract_data {
    my $csv          = shift;
    my $col_interest = shift;
    my $data         = shift;

    my $csv_array = csv(in => $csv);

    die "No data in $csv" unless @{$csv_array};

    my @header = @{shift @{$csv_array}};

    my $col_map;
    my $i = 0;
    foreach my $field ( @header ) {
	if ( $col_interest->{$field} ) {
	    $col_map->{$i} = $col_interest->{$field};
	}
	$i++;
    }

    foreach my $row ( @{$csv_array} ) {
	my $row_data = {};
	foreach my $col_num ( sort keys $col_map ) {
	    my $col_name = $col_map->{$col_num};

	    my $datapoint = $row->[$col_num];

	    if ( $datapoint && $datapoint !~ /\s+/) {
		if ( $col_name eq 'age' ) {
		    $datapoint = 2020 - $datapoint;
		} elsif ( $col_name =~ /_date$/ ) {
		    $datapoint .= ' 00:00:00';
		} elsif ( $col_name eq 'sex' ) {
		    $datapoint = uc(substr($datapoint, 0,1));
		}
		$row_data->{$col_name} = $datapoint;
	    }
	}

	my $case_id = $row_data->{'case_id'};
	delete $row_data->{'case_id'};
	$data->{$case_id} = $row_data;
    }
}

sub import_data {
    my $url  = shift;
    my $data = shift;

    my $sql_file = 'case-details-southkorea.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    print SQL "delete from case_details where source='$url';\n";

    foreach my $case_id ( keys %{$data} ) {
        my $sql = "insert into case_details (source, place_id, case_id";
        foreach my $column_name ( keys %{$data->{$case_id}} ) {
            $sql .= ", $column_name";
        }
        $sql .= ") values ('$url', 33, $case_id";
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
 	"Automated import of cases and places tables downloaded from $url at $datestring GMT";

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
