#!/usr/bin/perl -w

use strict;

my $url_base =
    'https://raw.githubusercontent.com/guga31bb/nflfastR-data/master/data';

my $year = 2000;
my $first = 1;
while ( $year < 2020 ) {
    my $file      = "play_by_play_$year.csv";
    my $gzip_file = "$file.gz";
    my $url       = "$url_base/$gzip_file?raw=True";

    run_command("curl -f -L -o $gzip_file '$url'",
		"Could not download $url");

    run_command("gunzip -f $gzip_file");

    unlink($gzip_file);
    
    run_command("sed 's/,NA/,/g' $file > fixed.csv",
		"Bad sed");

    unlink($file);
    
    if  ($first) {
	run_command("mv fixed.csv all.csv", "Can't mv file");
	$first = 0;
    } else {
	run_command('tail -n +2 fixed.csv >> all.csv', 'Append failed');
    }

    $year++;
}

run_command("dolt schema import -r --pks play_id,game_id plays all.csv",
	    "Could not import to dolt");

run_command("dolt table import -u --pk play_id,game_id plays all.csv",
	    "Could not import to dolt");

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
