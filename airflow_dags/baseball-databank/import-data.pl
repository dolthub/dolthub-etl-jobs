#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use JSON::Parse qw(parse_json);

my $base_url = 'https://github.com/chadwickbureau/baseballdatabank/archive/';

my $zip_file    = 'master.zip';
my $current_md5 = 'b43879ed1e40bdc5decabe76d36d143d';

my $root = 'baseballdatabank-master';

my $pk_override = {
    'AwardsPlayers' => ['playerID', 'awardID', 'yearID'],
    'Teams'         => ['yearID', 'lgID', 'teamID', 'franchID'],
    'AllstarFull'   => ['playerID', 'yearID' , 'teamID', 'lgID'],
};

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'baseball-databank';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($base_url, $current_md5, $zip_file);

import_csvs($root, $pk_override);

publish("$base_url$zip_file");

sub download_files {
    my $base         = shift;
    my $current_md5s = shift;
    my $zip_file     = shift;

    my $is_changed = 0;
    my $url = $base . $zip_file;
    run_command("curl -L -O $url", "Could not download $url");

    my $md5 = `md5sum $zip_file`;
    my @split_md5 = split(/\s+/, $md5);
    $md5 = $split_md5[0];

    $is_changed = 1 if ( $md5 ne $current_md5 );

    unless ( $is_changed ) {
	print "No new data in file. Exiting early...\n";
	exit 0;
    }

    run_command("unzip $zip_file", "Could not unzip $zip_file");
}

sub import_csvs {
    my $root        = shift;
    my $pk_override = shift;

    my @csvs = `find baseballdatabank-master -name '*.csv'`;

    foreach my $csv ( @csvs ) {
	chomp $csv;
	
	$csv =~ /\/(\w+)\.csv$/;
	my $table_name = $1;

	print "TABLE: $table_name\n";
	
	my @primary_keys = determine_pks($table_name, $csv, $pk_override);
	my $pk_string    = join ',', @primary_keys;

	run_command("dolt schema import -r -pks $pk_string $table_name $csv",
                    "Could not create schema for $table_name from $csv");
	
	run_command("dolt table import -r -pk $pk_string $table_name $csv",
		    "Could not create $table_name from $csv");
    }
}

sub determine_pks {
    my $table_name  = shift;
    my $csv         = shift;
    my $pk_override = shift;

    if ( $pk_override->{$table_name} ) {
	return @{$pk_override->{$table_name}};
    }
    

    my $header = `head -1 $csv`;
    chomp $header;
    my @fields = split /,/, $header;
    
    my @primary_keys;
    foreach my $field ( @fields ) {
	# Primary keys mostly have postfix ID or .key
	next unless ( $field =~ /\w+ID$/ or $field =~ /\w+\.key$/ );

	# Ignore retroID or bbrefID
	next if ( $field eq 'retroID' );
	next if ( $field eq 'bbrefID' );

	push @primary_keys, $field;
	print "$field\n";
    }

    die "No primary_keys found for $table_name"
	unless ( scalar(@primary_keys) > 0 );

    return @primary_keys;
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

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
