#!/usr/bin/perl -w 

use strict;

use Data::Dumper;

my $base_url = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/';

my $files = {
    'airports'  => 'airports.dat',
    'airlines'  => 'airlines.dat',
    'routes'    => 'routes.dat',
    'planes'    => 'planes.dat',
    'countries' => 'countries.dat',
};

my $pks = {
    'airports'  => ['airports_id'],
    'airlines'  => ['airline_id'],
    'routes'    => ['airline_id', 'source_airport_id', 'destination_airport_id'],
    'planes'	=> ['name'],
    'countries'	=> ['name'],
};

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'open-flights';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($base_url, $files);

import_csvs($files, $pks);

publish($base_url);

sub download_files {
    my $base  = shift;
    my $files = shift;

    foreach my $filename ( keys %{$files} ) { 
	my $csv = $filename . '.csv';
	my $url = $base . $files->{$filename};
	run_command("curl -m 30 -L -o $csv '$url'", "Could not download $url");
    }
}

sub import_csvs {
    my $files = shift;
    my $pks   = shift;

    foreach my $table ( keys %{$files} ) {
	my $csv = "$table.csv";
	my $pk_string = join(',', @{$pks->{$table}});

	my $header_csv = $table . '_header.csv';

	run_command("sed -i '' -E -e 's/\\\\N//g' $csv",
		    "Could not replace NULL value string in $csv");

	run_command("dolt sql -r csv -q 'select * from $table limit 0' > $header_csv",
		    "Could not build header for $table");
	
	run_command("cat $header_csv $csv | dolt table import -r -pk $pk_string $table";,
		    "Could not import $table from  $csv");
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

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
