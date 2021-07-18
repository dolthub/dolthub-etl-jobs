#!/usr/bin/perl -w 

use strict;

use JSON::Parse qw(parse_json);

my $base_url = 'https://nlp.stanford.edu/projects/snli/';

my $zip_file    = 'snli_1.0.zip';
my $current_md5 = '981c3df556bbaea3f17f752456d0088c';

my %json_files = (train => 'snli_1.0/snli_1.0_train.jsonl',
		  dev   => 'snli_1.0/snli_1.0_dev.jsonl',
		  test  => 'snli_1.0/snli_1.0_test.jsonl');

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'snli';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($base_url, $current_md5, $zip_file);

my $inf_sql = 'inference.sql';
open INF_SQL, ">$inf_sql" or die "Could not open $inf_sql";
print INF_SQL "delete from inferences;\n";

my $label_sql = 'labels.sql';
open LABEL_SQL, ">$label_sql" or die "Could not open $label_sql";
print LABEL_SQL "delete from annotator_labels;\n";
    
parse_files(%json_files);

close INF_SQL;
close LABEL_SQL;

run_command("dolt sql < $inf_sql",
	    "Could not process inferences SQL file");
run_command("dolt sql < $label_sql",
	    "Could not process annotator_labels SQL file");

publish($base_url);

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

sub parse_files {
    my %files = @_;

    foreach my $set ( keys %files ) {
	open JSONL, "<$files{$set}" or die "Could not open $files{$set}";

	while ( my $line = <JSONL> ) {
	    my $data = parse_json($line);
	    parse_line($data, $set);
	}

	close JSONL;
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

sub parse_line {
    my $line = shift;
    my $set  = shift;

    my $inf_sql = 'insert into inferences(train_dev_test, ';

    my $first = 1;
    foreach my $key ( sort keys %{$line} ) {
	next if ( $key eq 'annotator_labels' );

	$inf_sql .= ', ' if !$first;
	$first = 0;
	
	$inf_sql .= $key;
    }

    $inf_sql .= ") values ('$set', ";

    $first = 1;
    foreach my $key ( sort keys %{$line} ) {
	next if	( $key eq 'annotator_labels' );

	$inf_sql .= ', ' if !$first;
	$first = 0;	

	my $value = clean_input($line->{$key});
	
        $inf_sql .= "'$value'";
    }

    $inf_sql .= ");\n";

    print INF_SQL $inf_sql;

    my $label_sql_base =
	'insert into annotator_labels(pairID, label_num, label) values (';

    my $pairID = $line->{'pairID'};
    
    my $i = 1;
    foreach my $label ( @{$line->{'annotator_labels'}} ) {
	my $label_sql = $label_sql_base;
	$label_sql .= "'$pairID', $i, '$label');\n";
	print LABEL_SQL $label_sql;
	$i++;
    }    
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
