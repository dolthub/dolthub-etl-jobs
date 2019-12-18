#!/usr/bin/perl -w

use strict;

use Data::Dumper;
use Text::CSV qw(csv);

# Configuration 
my $base_url = 'https://s3.amazonaws.com/google-landmark/metadata/';

my $files = {
    'train' => {
	'.csv' => '8ae4a4e9e30d76eca31da9f902dc5e9f',
	'_attribution.csv' => '1f3b832f8b7f74e174e046719b7890f8',
	'_label_to_category.csv' => '7a83d7e79655b4da4dec3bceba2ab2e2'
    },
    'index' => {
	'.csv' => 'ff79f2322f4acbde496ecce4949d51be',
	'_image_to_landmark.csv' => '2fe216e4e45cbd18420089f88f84e3be',
	'_label_to_category.csv' => 'de049caf07dcd7923b61cc5939e62483'
    },
    'test' => {
	'.csv' => 'b6579e313d98647bd51b7caa189b5c84'
    }
};

# Begin Import job
# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'google-landmarks';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($base_url, $files);
process_train_files();
process_index_files();

publish($base_url);

sub download_files {
    my $base  = shift;
    my $files = shift;

    my $is_changed = 0;
    foreach my $set ( keys %{$files} ) {
	foreach my $file ( keys %{$files->{$set}} ) {
	    my $url = $base . $set . $file;
	    run_command("curl -L -O $url", "Could not download $url");

	    my $filename = $set . $file;
	    my $md5 = `md5sum $filename`;
	    my @split_md5 = split(/\s+/, $md5);
	    $md5 = $split_md5[0];

	    print "New MD5: $md5\n";
	    print "Old MD5: $files->{$set}{$file}\n";
	    $is_changed = 1 if ( $md5 ne $files->{$set}{$file} );
	}
    }

    unless ( $is_changed ) {
        print "No new data in files. Exiting early...\n";
        exit 0;
    }
}

sub process_train_files {
    my $csv = Text::CSV->new({
	auto_diag => 1, 
	binary => 1, 
	decode_utf8 => 0});
    
    # I want to combine this data into one table so I'm going to create a 
    # hash of all the rows on id and then dump it out into a set of SQL
    # inserts
    my $data = {};
    
    open TRAIN_CSV, "<train.csv" or die "Could not open train.csv\n";
    my $header = 1;
    while ( my $line = $csv->getline(*TRAIN_CSV) ) {
	if ( $header ) {
	    $header = 0;
	    next;
	}

	my $id = $line->[0];
	$data->{$id} = {};

	# Skip id because we keyed that
	my @columns = ('id','url','landmark_id');
	for ( my $i = 1; $i < scalar(@columns); $i++ ) {
	    $data->{$id}{$columns[$i]} = $line->[$i];
	}
    }
    close TRAIN_CSV;

    open ATTR_CSV, "<train_attribution.csv" 
	or die "Could not open train_attribution.csv\n";
    $header = 1;
    while ( my $line = $csv->getline(*ATTR_CSV) ) {
        if ( $header ) {
            $header = 0;
            next;
        }
	
	my $id = $line->[0];

	# Skip id and url because we already have those
	my @columns = ('id','url','author','license','title');
	for ( my $i = 2; $i < scalar(@columns); $i++ ) {
            $data->{$id}{$columns[$i]} = $line->[$i];
        }
    }
    close ATTR_CSV;

    open SQL, '>train.sql' or die "Could not open train.sql";
    print SQL "delete from train_images;\n";
    foreach my $id ( keys %{$data} ) {
	print SQL "insert into train_images (id";
	foreach my $key ( sort keys %{$data->{$id}} ) {
	    print SQL ",$key";
	}
	print SQL ") values ('$id'";
	foreach my $key ( sort keys %{$data->{$id}} ) {
	    my $value = clean_input($data->{$id}{$key});
	    print SQL ",'$value'";
	}
	print SQL ");\n";
    }
    close SQL;

    run_command('dolt sql < train.sql', 'Could not execute train.sql in dolt');

    process_train_label_to_category();
}

sub process_train_label_to_category {
    my $csv = 'train_label_to_category.csv';

    run_command("dolt table import -u train_landmarks $csv",
		"Could not import $csv into dolt\n");
}

sub process_index_files {
    my $csv = 'index_image_to_landmark.csv';
    run_command("dolt table import -u index_images $csv",
		"Could not import $csv into dolt\n");

    $csv = 'index_label_to_category.csv';
    run_command("dolt table import -u index_landmarks $csv",
		"Could not import $csv into dolt\n");
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

# Helpers

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
