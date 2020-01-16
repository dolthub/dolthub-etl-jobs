#!/usr/bin/perl -w 

use strict;

my $base_url = 'https://www.ssa.gov/oact/babynames/';

my $national_zip = 'names.zip';
my $state_zip    = 'state/namesbystate.zip';

my $files = {
    national => {
	md5 => 'f6a44cac6976da98c075aa55dde9b930',
	path => 'names.zip',
	name => 'names.zip',
    },
    state => {
	md5 => 'ae1878e6c419dc5850d5f268f5952bc1',
	path => 'state/namesbystate.zip',
	name => 'namesbystate.zip',
    },
};

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'us-baby-names';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($base_url, $files);

parse_files();

publish($base_url);

sub download_files {
    my $base  = shift;
    my $files = shift;
    
    my $is_changed = 0;

    foreach my $type ( keys %{$files} ) {
	my $path        = $files->{$type}{path};
	my $current_md5 = $files->{$type}{md5};
	my $zip_file    = $files->{$type}{name};

	run_command("mkdir $type", "Could not create directory $type");

	chdir($type);
	
	my $url = $base . $path;
	run_command("curl -L -O $url", "Could not download $url");

	my $md5 = `md5sum $zip_file`;
	my @split_md5 = split(/\s+/, $md5);
	$md5 = $split_md5[0];

	print "$type: $md5\n";

	$is_changed = 1 if ( $md5 ne $current_md5 );

	run_command("unzip $zip_file", "Could not unzip $zip_file");

	chdir('..')
    }

    unless ( $is_changed ) {
	print "No new data in file. Exiting early...\n";
	exit 0;
    }
}

sub parse_files {
    open SQL, ">import.sql" or die "Could not open import.sql";
    print SQL "delete from names;\n";


    my @national_files = `ls -1 national/*.txt`;
    foreach my $file ( @national_files ) {
	chomp $file;

	open YEAR, "<$file" or die "Could not open $file for read";

	$file =~ /national\/yob(\d{4})\.txt/;
	my $year = $1;
	
	while ( my $line = <YEAR> ) {
	    chomp $line;
	    chop($line) if ($line =~ m/\r$/);
	    my ($name, $sex, $count) = split ',', $line;

	    print SQL "insert into names(name, birth_year, state, sex, count) values('$name', $year, 'US', '$sex', $count);\n";
	}
    }

    my @state_files = `ls -1 state/*.TXT`;
    foreach my $file ( @state_files ) {
	chomp $file;
	
        open STATE, "<$file" or die "Could not open $file for read";
	while ( my $line = <STATE> ) {
	    chomp $line;
            chop($line) if ($line =~ m/\r$/);

	    my ($state, $sex, $year, $name, $count) = split ',', $line;

	    print SQL "insert into names(name, birth_year, state, sex, count) values('$name', $year, '$state', '$sex', $count);\n";
        }
    }
	
    run_command("dolt sql < import.sql",
		"Could not run import.sql using dolt sql");

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

sub clean_input {
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
