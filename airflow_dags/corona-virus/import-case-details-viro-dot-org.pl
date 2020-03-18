#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use Text::CSV qw( csv );

my $sheet_id = '1itaohdPiAeniCXNlntNztZ_oRvjh0HsGuJXUJWET008';

my $url = "https://docs.google.com/spreadsheets/d/$sheet_id/";

my @sheets = (
    'Hubei',
    'outside_Hubei'
    );

my %needed_columns = (
    'ID' => 'case_id',
    'source' => 'case_name',
    'sex' => 'sex',
    'age' => 'age',
    'country' => 'nationality',
    'outcome' => 'current_status',
    'date_onset_symptoms' => 'symptomatic_date',
    # Doing this prefers date_confirmation over date_admission_hospital
    # But allows us to use both to populate confirmed_date
    'date_admission_hospital' => 'confirmed_date',
    'date_confirmation' => 'confirmed_date',
    'date_death_or_discharge' => 'recovered_date', 
    # Not used directly but will extract anyway so we can get place_id
    'country' => 'country',
    'province' => 'state'
);


my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $branch       = 'case_details_virological_dot_org';

prepare_import($organization, $repo, $branch);

my $place_id_map = {};
create_place_id_map($place_id_map);

download_files($sheet_id, @sheets);

my $case_details = {};
extract_data($case_details, \%needed_columns, $place_id_map, @sheets);

import_data($url, $case_details);

publish($url, $branch);

sub prepare_import {
    my $org = shift;
    my $repo = shift;
    my $branch = shift;

    my $clone_path = "$org/$repo";
    
    run_command("dolt clone $clone_path", "Could not clone repo $clone_path");

    chdir($repo);
    
    run_command("dolt checkout $branch", "Could not checkout $branch");

    run_command("dolt merge master",
                "Could not merge master branch into $branch");
}

sub create_place_id_map {
    my $place_id_map = shift;

    my $query = "select place_id, country_region as country, province_state as state from places";
    my $place_id_csv =
	run_command("dolt sql -r csv -q \"$query\" > places.csv",
		    "Could not get places from dolt");

    my $place_id_arr = csv(in => 'places.csv',
			   headers => 'auto');

    foreach my $row ( @{$place_id_arr} ) {
	my $place_id = $row->{'place_id'};
	my $country = $row->{'country'};
	my $state = $row->{'state'};

	$place_id_map->{$country}{$state} = $place_id;
    }
}

sub download_files {
    my $sheet_id = shift;
    my @sheets    = @_;

    # To download a CSV of a sheet, you take the base, add in the sheet id,
    # then put the postfix, and finally add the sheet name
    my $google_base      = 'https://docs.google.com/spreadsheets/d/';
    my $download_postfix = '/gviz/tq?tqx=out:csv&sheet=';

    foreach my $sheet ( @sheets ) {
	my $url = $google_base . $sheet_id . $download_postfix . $sheet;
	run_command("curl -m 30 -L -o $sheet.csv '$url'",
		    "Could not download $url");
    }
}

sub extract_data {
    my $case_details = shift;
    my $col_interest = shift;
    my $place_id_map = shift;
    my @sheets = @_;

    foreach my $sheet ( @sheets ) {
	my $csv = "$sheet.csv";

	my $csv_array = csv(in => $csv);
	
	die "No data in $csv" unless @{$csv_array};

	my @header = @{shift @{$csv_array}};

	my %col_map;
	my $i = 0;
	foreach my $field ( @header ) {
	    if ( $col_interest->{$field} ) {
		$col_map{$i} = $col_interest->{$field};
	    }
	    $i++;
	}

	foreach my $row ( @{$csv_array} ) {
	    my $row_data = {};
	    foreach my $col_num ( sort keys %col_map ) {
		my $col_name = $col_map{$col_num};
		
		my $datapoint = $row->[$col_num];

		$datapoint = '' if ( $datapoint eq 'NA' );
		
		if ( $datapoint && $datapoint !~ /\s+/) {
		    if ( $col_name =~ /_date$/ ) {
			if ( $datapoint =~ /^(\d{2})\.(\d{2})\.(\d{4})$/ ) {
			    my $day = $1;
			    my $month = $2;
			    my $year = $3;
			    next if ( $month > 12 || $day > 31 ); 
			    $datapoint = "$year-$month-$day 00:00:00";
			} else {
			    next;
			}

		    } elsif ( $col_name eq 'sex' ) {
			$datapoint = uc(substr($datapoint, 0,1));

		    } elsif ( $col_name eq 'age' ) {
			if ( $datapoint =~ /(\d+)|(\d+)-(\d+)/ ) {
			    $datapoint = $1;
			} else {
			    die "Improperly formatted age: $datapoint";
			}
			
		    } elsif ( $col_name eq 'current_status' ) {
			if ( lc($datapoint) =~ /discharge|recovered/) {
			    $datapoint = 'released';
			} elsif ( lc($datapoint) =~ /died|death/ ) {
			    $datapoint = 'deceased';
			}
		    }
		    
		    $row_data->{$col_name} = $datapoint;
		}
	    }

	    # Delete reovered_date if case resulted in death
	    if ( $row_data->{'current_status'} &&
		 $row_data->{'current_status'} eq 'deceased' ) {
		delete $row_data->{'recovered_date'};
	    }

	    # Must estimate place_id somehow
	    if ( my $place_id = estimate_place_id($place_id_map,
						  $row_data->{'country'},
						  $row_data->{'state'}) ) {
		$row_data->{'place_id'} = $place_id;
	    }

	    delete $row_data->{'country'};
	    delete $row_data->{'state'};

	    # Remove Singapore, Hong Kong, and South Korea because we
	    # have better data
	    next if ( $row_data->{'place_id'} && 
		      ($row_data->{'place_id'} == 33 ||
		       $row_data->{'place_id'} == 32 ||
		       $row_data->{'place_id'} == 29) );

	    next unless $row_data->{'case_id'};
	    
	    my $case_id =
		$row_data->{'case_id'};
	    delete $row_data->{'case_id'};
	    $case_details->{$sheet}{$case_id} = $row_data;
	}
    }
}
    
sub import_data {
    my $url  = shift;
    my $data = shift;

    my $sql_file = 'case-details-viro-dot-org.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    foreach my $sheet ( sort keys %{$data} ) {
	print SQL "delete from case_details where source='$url$sheet';\n";
	foreach my $case_id ( sort keys %{$data->{$sheet}} ) {
	    my $sql = "insert into case_details (source, case_id";
	    foreach my $column_name ( sort keys %{$data->{$sheet}{$case_id}} ) {
		$sql .= ", $column_name";
	    }
	    $sql .= ") values ('$url$sheet', $case_id";
	    foreach my $column_name ( sort keys %{$data->{$sheet}{$case_id}} ) {
		my $value = $data->{$sheet}{$case_id}{$column_name};
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
    }
	
    close SQL;
    
    run_command("dolt sql < $sql_file", "Could not execute generated SQL");
}

sub publish {
    my $url = shift;
    my $branch = shift;

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

    run_command("dolt push origin $branch", 'dolt push failed');
}

sub estimate_place_id {
    my $place_id_map = shift;
    my $country = shift;
    my $region  = shift;

    my $place_id;
    if ( $country && $region ) {
	$place_id = $place_id_map->{$country}{$region};
	unless ( $place_id ) {
	    $place_id = $place_id_map->{$country}{''};
	}
	unless ( $place_id ) {
	    $place_id = $place_id_map->{$country}{$country};
	}
    }

    if ( $country ) {
	$place_id = $place_id_map->{$country}{''};
	unless ( $place_id ) {
	    $place_id = $place_id_map->{$country}{$country};
	}
    }    
    
    return $place_id;
}

sub escape_sql {
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
