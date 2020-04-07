#!/usr/bin/perl -w 

use strict;

# The source for this data has moved and changed formats, we'll continue to
# merge in master into this branch.
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $branch       = 'case_details_virological_dot_org';

prepare_import($organization, $repo, $branch);

sub prepare_import {
    my $org = shift;
    my $repo = shift;
    my $branch = shift;

    my $clone_path = "$org/$repo";
    
    run_command("dolt clone $clone_path", "Could not clone repo $clone_path");

    chdir($repo);
    
    run_command("dolt checkout $branch", "Could not checkout $branch");

    run_command("dolt merge master",
                "Could not merge master branch into $branch");

    print "Attempting to commit...\n";
    if ( system("dolt commit -m 'Merged master branch'") eq 0 ) {
	print "Commit Succeeded\n";
	run_command("dolt push origin $branch", 'dolt push failed');
    } else {
	print "Nothing to commit\n";
    }
}

sub run_command {
    my $command = shift;
    my $error   = shift;

    print "Running: $command\n";

    my $exitcode = system($command);

    print "\n";

    die "$error\n" if ( $exitcode != 0 );
}
