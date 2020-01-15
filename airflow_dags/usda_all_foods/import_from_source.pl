#!/usr/bin/perl -w

use strict;

use File::Path 'rmtree';

# Declare corrections here
my $corrections = {
    'downloaded_data/branded_food.csv' => {
 	347334 => {
 	    'match' => '[\n\r]+',
 	    'sub'   => '',
 	},
 	347217 => {
             'match' => '[\n\r]+',
             'sub'   => '',
 	},
 	347360 => {
             'match' => '[\n\r]+',
             'sub'   => '',
 	},
 	347686 => {
             'match' => '[\n\r]+',
             'sub'   => '',
 	},
	611348 => {
	    'match' => '[\n\r]+',
	    'sub'   => '',
	},
	606994 => {
            'match' => '[\n\r]+',
	    'sub'   => '',
        },
	607218 => {
            'match' => '[\n\r]+',
            'sub'   => '',
        },
	607260 => {
            'match' => '[\n\r]+',
            'sub'   => '',
	},
	607840 => {
            'match' => '[\n\r]+',
	    'sub'   => '',
        },
    },
    'downloaded_data/food.csv' => {
 	321829 => {
	    'match' => '[\n\r]+',
	    'sub'   => '',
	},
    },
    'downloaded_data/food_update_log_entry.csv' => {
        321829 => {
            'match' => '[\n\r]+',
            'sub'   => '',
        },
    },
};

# Configuration
my $url_base = 'https://fdc.nal.usda.gov';
my $info_url = "$url_base/download-datasets.html";
my $csv_search = '/fdc-datasets/FoodData_Central_csv';

my $current_url = '/fdc-datasets/FoodData_Central_csv_2019-12-17.zip';

my $zip_dir  = 'downloaded_data';
my $zip_file = "$zip_dir.zip";

# There is no static link to the latest database so I'll download the info page
# and find the link I'm looking for
my $download_url = find_download_url($info_url, $csv_search);

if ( $download_url eq $current_url ) {
    print "We already have imported the most current data: $download_url";
    exit 0;
}

$download_url = "$url_base$download_url";

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'usda-all-foods';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
	    "Could not clone repo $clone_path");

chdir($repo);

get_data($download_url, $zip_dir, $zip_file);

import_data($zip_dir);

publish($download_url);

cleanup($zip_dir, $zip_file);

sub find_download_url {
    my $info_url      = shift;
    my $search_string = shift;

    my $info_html = `curl $info_url | grep $search_string`;

    $info_html =~ m/<a\shref="($search_string.+?\.zip)">/;
    
    my $file = $1 or die "Could find the file path from $search_string";

    return $file;
}

sub get_data {
    my $input_url = shift;
    my $zip_dir   = shift;
    my $zip_file  = shift;

    run_command("curl -o $zip_file $input_url",
                "Could not download $input_url");

    run_command("unzip -o -d $zip_dir $zip_file",
		"Could not unzip $zip_file");
}

sub import_data {
    my $data_dir = shift;

    my @files = `ls -1 $data_dir | grep csv`;

    foreach my $file ( @files ) {
	chomp $file;

	my $path = "$data_dir/$file";
	
	# Construct table names from files
	my $table = $file;
	$table =~ s/\.csv//g;

	my $fixed = correct_csv($path);

	my $created_tables = `dolt ls`;

	my $import_command = ( $created_tables =~ /\b$table\b/ ) ? 
	    "dolt table import -r $table $fixed" :
	    "dolt table import -c $table $fixed";

	run_command($import_command, "Could not import $table");

	unlink($fixed);
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

sub cleanup {
    my $zip_dir = shift;
    my $zip_file = shift;

    rmtree($zip_dir);
    unlink($zip_file);
}

sub correct_csv {
    my $csv_file = shift;

    my $fixed_csv = $csv_file;
    $fixed_csv =~ s/\.csv/\.fixed\.csv/g;

    open INPUT, "<$csv_file";
    open OUTPUT, ">$fixed_csv";

    while ( my $line = <INPUT> ) {
	# NULL Columns are "" in this dataset. Make empty columns.
	my $old_line = '';
	while ( $line ne $old_line ) {
	    $old_line = $line;
	    $line =~ s/,"",/,,/g;
	}
	$line =~ s/,""\n/,\n/g;

	$line = correct_data($csv_file, $line);

	print OUTPUT $line;
    }

    close INPUT;
    close OUTPUT;

    return $fixed_csv;
}

sub correct_data {
    my $file = shift;
    my $line = shift;

    # Short circuit if we have no corrections to process in the file
    return $line unless $corrections->{$file};

    # The first column in all these files is the id
    my @fields = split ',', $line;
    my $id = $fields[0];
    $id =~ s/"//g;

    my $regex = $corrections->{$file}{$id};

    if ( $regex ) {
	my $match = $regex->{'match'};
	my $subst = $regex->{'sub'};
	$line =~ s/$match/$subst/g;
    }
    
    return $line;
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    die "$error\n" unless ( $exitcode == 0 );
}
