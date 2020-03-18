#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use JSON::Parse 'json_file_to_perl';

my $url = 'https://coronavirus-ph-api.now.sh/cases';

my $json_file = 'phillipines-cases.json'; 

my $fields = {
    'case_no' => 'case_id',
    'other_information' => 'case_name',
    'age' => 'age',
    'gender' => 'sex',
    'nationality' => 'nationality',
    'status' => 'current_status',
    'date' => 'confirmed_date',
};

my $data = {};
    
# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($url, $json_file);

extract_data($json_file, $fields, $data);

import_data($url, $data);

publish($url);

sub download_files {
    my $url       = shift;
    my $json_file = shift;

    run_command("curl -m 30 -f -L -o $json_file '$url'",
		"Could not download $url");
}

sub extract_data {
    my $json_file    = shift;
    my $interest     = shift;
    my $data         = shift;

    my $json_array = json_file_to_perl($json_file);

    die "No data in $json_file" unless @{$json_array};

    foreach my $case ( @{$json_array} ) {
	my $case_data = {};
	foreach my $in_field ( keys %{$interest} ) {
	    my $datapoint = $case->{$in_field};

	    my $out_field = $interest->{$in_field};
	    if ( $datapoint && $datapoint !~ /^\s+$/ && $datapoint ne 'TBA') {
		if ( $in_field eq 'status' ) {
		    $datapoint = 'Deceased' if ( lc($datapoint) eq 'died' );
		}
		
		$case_data->{$out_field} = $datapoint;
	    }
	}

	my $case_id = $case_data->{'case_id'} or die "Could not find case id";
	delete $case_data->{'case_id'};
	$data->{$case_id} = $case_data;
    }
}
    
sub import_data {
    my $url  = shift;
    my $data = shift;

    my $sql_file = 'case-details-phillipines.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    print SQL "delete from case_details where source='$url';\n";

    foreach my $case_id ( sort keys %{$data} ) {
        my $sql = "insert into case_details (source, place_id, case_id";
        foreach my $column_name ( keys %{$data->{$case_id}} ) {
            $sql .= ", $column_name";
        }
        $sql .= ") values ('$url', 48, $case_id";
        foreach my $column_name ( keys %{$data->{$case_id}} ) {
            my $value = escape_sql($data->{$case_id}{$column_name});
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
