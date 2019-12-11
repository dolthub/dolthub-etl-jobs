#!/usr/bin/perl -w

use strict;

my $url_base = 'http://nlpgrid.seas.upenn.edu/PPDB/eng/';
my $file     = 'ppdb-2.0-xxxl-all.gz';

my $current_md5 = 'c3585ad2458f6a034dc8066abc15b4eb';

my $url = $url_base . $file;

run_command("curl -L -O $url", "Could not download $url");

my $md5 = `md5sum $file`;
my @split_md5 = split(/\s+/, $md5);
$md5 = $split_md5[0];
        
exit 0 if ( $md5 eq $current_md5 );

# Fail the job if we have a new ppdb
exit 1;

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    die "$error\n" if ( $exitcode != 0 );
}
