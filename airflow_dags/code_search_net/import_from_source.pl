#!/usr/bin/perl -w 

use strict;

use File::Path qw(rmtree);
use JSON::Parse 'parse_json';

my $url_base = 'https://s3.amazonaws.com/code-search-net/CodeSearchNet/v2/';

my @languages = ('python', 
		 'java',
		 'go',
		 'php',
		 'ruby');

my %md5s = (
    'python' => '07b49dd01fbac894fbdae22da6462e4f',
    'java'   => 'fea180077275d8f98f42a3386f492837',
    'go'     => 'c0288db91f067c95bb952577949e7b13',
    'php'    => '62373f85cfae2f5d7422dc1e55fbbb50',
    'ruby'   => '6847c0149666334cc937909b0e2297ae');

my $path_base= '/final/jsonl/';
my @paths = ('train/',
	     'test/',
	     'valid/');

my @keys = ('url', 
	    'repo', 
	    'path', 
	    'func_name', 
	    'language', 
	    'original_string',
	    'code',
	    'docstring',
	    'partition');

my $tmpdir = 'data/';

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'code-search-net';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
	    "Could not clone repo $clone_path");

chdir($repo);

my @new = 
    download_and_unpack($url_base, \@languages, $path_base, \@paths, $tmpdir);

unless ( @new ) {
    print "No new data to process\n";
    rmtree($tmpdir);
    exit;
}

import_files($tmpdir, \@new, $path_base, \@paths);

publish($url_base);

rmtree($tmpdir);

sub download_and_unpack {
    my $url_base  = shift;
    my $languages = shift;
    my $path_base = shift;
    my $paths     = shift;
    my $tmpdir    = shift;

    run_command("mkdir $tmpdir", "Could not create $tmpdir");
    chdir($tmpdir);

    my @new;
    foreach my $language ( @{$languages} ) {
	my $file = "$language.zip";
	my $url  = "$url_base$file";

	run_command("curl -O $url", "Could not download $url");

	my $md5 = `md5sum $file`;
	my @split_md5 = split(/\s+/, $md5);
	$md5 = $split_md5[0];

	next if ( $md5 eq $md5s{$language} );
	push @new, $language;
	run_command("unzip -o $file", "Could not unzip $file");
	
	foreach my $path ( @{$paths} ) {
	    my $dir = "$language$path_base$path";
	    foreach my $gzip ( `ls -1 $dir*.gz` ) {
		chomp $gzip;
		run_command("gunzip -f $gzip", "Could not unzip $gzip");
	    }
	}
    }

    return @new;

    chdir('..');
}

sub import_files {
    my $tmpdir    = shift;
    my $languages = shift;
    my $path_base = shift;
    my $paths     = shift;

    foreach my $language ( @{$languages} ) {
	foreach my $path ( @{$paths} ) {
	    foreach my $json ( `ls -1 $tmpdir$language$path_base$path*.jsonl` ) {
		chomp $json;

		print "Processing file: $json\n";

		open JSON, "<$json";

		open CODESQL, '>code.sql';
		binmode(CODESQL, ":utf8");

		open CODETOKENSQL, '>code-token.sql';
		binmode(CODETOKENSQL, ":utf8");

		open DOCTOKENSQL, '>doc-token.sql';
		binmode(DOCTOKENSQL, ":utf8");

		while ( my $line = <JSON> ) {
		    my $data = parse_json($line);
		    create_code_sql($data, \*CODESQL);
		    create_token_sql($data, 'code_tokens', \*CODETOKENSQL); 
		    create_token_sql($data, 'docstring_tokens',\*DOCTOKENSQL);
		}

		close JSON;
		close CODESQL;
		close CODETOKENSQL;
		close DOCTOKENSQL;

		run_command("dolt sql < code.sql", 
			    "Could not execute SQL in code.sql");
		run_command("dolt sql < code-token.sql",
			    "Could not execute SQL in code-token.sql");
		run_command("dolt sql < doc-token.sql",
			    "Could not execute SQL in code-token.sql");
		unlink('code.sql', 'code-token.sql', 'doc-token.sql');
	    }
	}
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

sub create_code_sql {
    my $data = shift;
    my $fh   = shift;

    my $key_string = '';
    my $value_string = '';
    my $insert_string = '';
    foreach my $key ( @keys ) {
	my $value = $data->{$key};
	die "Cannot find value for key: $key" unless $value;
	$value = clean_input($value);
	$insert_string .= "'$value',";
	# There is an edge case where the string ends with a \\ and 
	# the parser can't handle it so we add a space after.
	$insert_string =~ s/\\\\',$/\\\\\\\\',/g; 
	$key_string .= "$key,";
    }
    $insert_string =~ s/,$//g;
    $key_string =~ s/,$//g;
    
    my $sql = 
	"INSERT INTO code ($key_string) VALUES ($insert_string);\n";
    
    print $fh $sql;
}

sub create_token_sql {
    my $data = shift;
    my $key  = shift; 
    my $fh   = shift;

    my $url    = clean_input($data->{'url'}); 
    my $tokens = $data->{$key};

    my $i = 0;
    foreach my $token ( @{$tokens} ) {
	my $sql = "INSERT INTO $key (url,sequence_num,token) VALUES ";
	$token = clean_input($token);
	$sql .= "('$url', $i, '$token');";
	$sql =~ s/\\\\'\);$/\\\\\\\\'\);/g;
	print $fh "$sql;\n";
	$i++;
    }
}

sub create_doc_token_sql {
    my $data       = shift;
    my $fh         = shift;
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

    die "$error\n" if ( $exitcode != 0 );
}
