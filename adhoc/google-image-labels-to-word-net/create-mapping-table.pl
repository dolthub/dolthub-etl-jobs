#!/usr/bin/perl -w

use strict;

use File::Path qw(rmtree);

# In order to install perl DBI I had to:
# brew install openssl mysql-connector-c
# 
# Then: 
# cpan install DBI
# cpan install DBD:mysql
use DBI;

my $this_repo       = 'google-image-labels-to-word-net'; 
my $imagenet_repo   = 'image-net';
my $openimages_repo = 'open-images';

my $imagenet_port    = 6666;
my $openimages_port = 6667;

chdir("../$imagenet_repo");
run_command("dolt sql-server -P $imagenet_port &", 
	    "Could not start $imagenet_repo sql server");

chdir("../$openimages_repo");
run_command("dolt sql-server -P $openimages_port &",
	    "Could not start $openimages_repo sql server");
chdir("../$this_repo");

# Give the SQL servers a chance to start
sleep(3);

my $dsn_base = "DBI:mysql:database=dolt;host=127.0.0.1;port="; 

my %attr = (
    'PrintWarn' => 0,
    'PrintError' => 0
    );

my $imagenet_dsn = "$dsn_base$imagenet_port";
my $imagenet_dbh = DBI->connect($imagenet_dsn, 'root', '', \%attr);

my $openimages_dsn = "$dsn_base$openimages_port";
my $openimages_dbh = DBI->connect($openimages_dsn, 'root', '', \%attr);

# Build words mapping table
my $words = run_query($imagenet_dbh, 'select * from words_synsets');

my $words_map = {};
foreach my $row ( @{$words} ){
    my ($word, $lex_id, $synset_id, $synset_type, $word_num) = @{$row};

    $word = lc($word);

    my $synset_map = {
	'id' => $synset_id,
	'type' => $synset_type
    };

    if ( $words_map->{$word} ) {
	push @{$words_map->{$word}}, $synset_map;
    } else {
	$words_map->{$word} = [$synset_map];
    }
}

# Build hypernym map
my $hyponyms = run_query($imagenet_dbh, 
			 "select * from synset_pointers where pointer_type_symbol='~'");

my $hyponym_map = {};
foreach my $row ( @{$hyponyms} ) {
    my $from_id   = $row->[0];
    my $from_type = $row->[1];
    my $to_id     = $row->[2];
    my $to_type   = $row->[3];

    my $from = $from_type . $from_id;
    my $to   = $to_type . $to_id;

    if ( $hyponym_map->{$from} ) {
        push @{$hyponym_map->{$from}}, $to;
    } else {
        $hyponym_map->{$from} = [$to];
    }
}

my $labels = run_query($openimages_dbh, 'select * from label_dictionary');

my $google_label_count = scalar @{$labels}; 
my $processed    = 0;
my $match_count  = 0;
my $mapped_count = 0;

open SQL, '>sql-inserts.sql' or die "Could not open sql-inserts.sql\n";    

foreach my $row ( @{$labels} ) {
    my $google_id = shift(@{$row});
    my $label     = lc(shift(@{$row}));

    my $synsets_data = $words_map->{$label} || [];

    if ( @{$synsets_data} ) {
	$match_count++;
	print "Matched $label\n"; 
	# If we match multiple synsets, write them all
	my $dedupe = {};
	foreach my $synset ( @{$synsets_data} ) {
	    my $synset_id   = $synset->{'id'};
	    my $synset_type = $synset->{'type'};

	    my $from = $synset_type . $synset_id;

	    my $hyponyms = $hyponym_map->{$from} || [];
		
	    # Find leaf nodes (or deepest lexically) in synset 
	    # graph here and insert all of them
	    if ( @{$hyponyms} && @{$synsets_data} > 1) {
		print "Skipped synset for $label because it is not a leaf and we have multiple synsets\n";
	    } else {
		if ( $dedupe->{$label}{$from} ) {
		    print "Found dupe sysnet for $label";
		    next;
		}
		$mapped_count++;
		print "Found leaf synset for $label\n";
		print SQL "insert into google_image_labels_synsets (google_image_label,synset_type,synset_id) values ('$google_id','$synset_type','$synset_id');\n";
		$dedupe->{$label}{$from} = 1;
	    }
	}
    } else {
	print "No synsets for $label\n";
    }

    $processed++;
    print "Processed $processed of $google_label_count\n";


}

close SQL;

print "Matched $match_count and mapped $mapped_count wordnet synsets of $google_label_count google labels\n";

run_command('pkill dolt',
	    "Could not kill dolt sql-server instances"); 

exit 0;

sub run_query {
    my $dbh   = shift;
    my $query = shift;
    my @vars  = @_;

    my $sth = $dbh->prepare($query) 
	or die "prepare statement failed: $dbh->errstr()";
    $sth->execute(@vars) or die "execution failed: $dbh->errstr()";
    my $ref = $sth->fetchall_arrayref();
    $sth->finish;

    return $ref;
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
