#!/usr/bin/perl -w 

use strict;

use JSON::Parse qw(parse_json);
use Getopt::Long;

$| = 1;

my $base_url = 'https://data.gharchive.org';

my $date_str;
my $push;
my $cleanup; 
GetOptions(
    "date|d=s" => \$date_str,
    "push|p" => \$push,
    "cleanup|c" => \$cleanup,
) or die("Error in command line arguments. Supported flags are --date, --push, and --cleanup.");

die "Must specify a date string YYYY-MM-DD with --date or -d" unless $date_str;

my $organization = 'Liquidata';
my $repo         = 'github-repos';
my $clone_path   = "$organization/$repo"; 

if ( ! -e $repo ) {
    clone_repo();
}

chdir($repo);

download_files($base_url, $date_str);

my $events = get_pull_request_events();

generate_inserts($events);
execute_inserts();
commit($date_str);
push_origin() if $push;
cleanup() if $cleanup;

exit 0;

sub clone_repo {
    run_command("dolt clone $clone_path", 
                "Could not clone repo $clone_path");
}

sub download_files {
    my $base     = shift;
    my $date_str = shift;

    # https://data.gharchive.org/2020-01-10-{0..23}.json.gz
    foreach my $hour ( 0..23 ) { 
        my $url = "$base/$date_str-$hour.json.gz";
        download($url, 3);
    }
    run_command("gunzip *.gz", "Could not unzip files");
}

sub get_pull_request_events {
    my @files = `ls *.json`;
    my @entries;
    @files = sort by_hour @files;
    foreach my $file ( @files ) {
        chomp($file);
        print "processing $file\n";
        open(F, '<', $file);
        while (<F>) {
            chomp();
            my $entry = parse_json($_);
            if ( $entry->{'type'} eq 'PullRequestEvent' ) {
                push @entries, $entry;
            }
        }
        close(F);
    }

    return \@entries;
}

sub by_hour {
    return 0 unless ( $a =~ m/\d{4}-\d{2}-\d{2}-(\d+)/ );
    my $aHour = $1;
    
    return 0 unless ( $b =~ m/\d{4}-\d{2}-\d{2}-(\d+)/ );
    my $bHour = $1;
    
    return $aHour <=> $bHour;
}

sub generate_inserts {
    my $entries = shift;

    open(SQL, '>', "updates.sql") or die $!;
    
    foreach my $entry ( @$entries ) {
        my $base = $entry->{'payload'}{'pull_request'}{'base'}{'repo'};
        unless ( $base ) {
            warn "No base repo found, skipping";
            next;
        }
        
        die "no url name found for repo" unless $base->{'url'};

        # Some fields changed their format name in 2015, so we look
        # for multiple matches. The full_name property for repos isn't
        # available until 2015 data, so we extract if from the URL in
        # all cases.
        my $name = quote_string(get_repo_name_from_url($base->{'url'}));
        my $id = $base->{'id'};
        my $description = quote_string($base->{'description'});
        my $owner = quote_string($base->{'owner'}{'login'});
        my $owner_type = quote_string($base->{'owner'}{'type'});
        my $created_at = quote_string($base->{'created_at'});
        my $updated_at = quote_string($base->{'updated_at'});
        my $fork = $base->{'fork'} ? 'TRUE' : 'FALSE';
        my $size = $base->{'size'} // 'NULL';
        my $star_count = $base-> {'watchers'} // $base->{'stargazers_count'} // 'NULL';
        my $fork_count = $base->{'forks'} // $base->{'forks_count'} // 'NULL';;
        my $open_issues_count = $base->{'open_issues'} // $base->{'open_issues_count'} // 'NULL';
        my $repo_language = quote_string($base->{'language'});
        my $has_issues = $base->{'has_issues'} ? 'TRUE' : 'FALSE';
        my $has_downloads = $base->{'has_downloads'} ? 'TRUE' : 'FALSE';
        my $has_wiki = $base->{'has_wiki'} ? 'TRUE' : 'FALSE';
        my $has_pages = $base->{'has_pages'} ? 'TRUE' : 'FALSE';
        my $default_branch = quote_string($base->{'default_branch'});

        print SQL <<"END";
            REPLACE INTO repos 
               (name, id, description, owner, owner_type, created_at, 
                updated_at, fork,  size, star_count, fork_count, open_issues_count,
                repo_language, has_issues, has_downloads, has_wiki, 
                has_pages, default_branch)
              VALUES ($name, $id, $description, $owner, $owner_type, $created_at, 
                      $updated_at, $fork, $size, $star_count, $fork_count, $open_issues_count,
                      $repo_language, $has_issues, $has_downloads, $has_wiki, 
                      $has_pages, $default_branch);
END
    }

    close(SQL)
}

sub get_repo_name_from_url {
    my $url = shift;
    if ( $url =~ m|/([^/]+)/([^/]+)$| ) {
        return "$1/$2";
    }
    return undef
}

sub quote_string {
    my $s = shift;
    return 'NULL' unless defined $s;
    $s =~ s/'/''/g;
    $s =~ s/\\/\\\\/g;
    return "'$s'";
}

sub execute_inserts {
    run_command('dolt sql < updates.sql', 
                "Failed to update dolt repository");
}

sub commit {
    my $datestring = shift;
    
    unless ( `dolt diff` ) {
        print "Nothing changed in import. Not generating a commit\n";
        return;
    }
    
    run_command('dolt add .', 'dolt add command failed');

    my $commit_message = 
        "Automated import of repos with pull request events on $datestring GMT";

    $datestring .= "T23:59:59Z";
    run_command("dolt commit -m '$commit_message' --date $datestring", 
                "dolt commit failed");
}

sub push_origin {
    run_command('dolt push origin master', 'dolt push failed');
}

sub cleanup {
    run_command('rm *.json *.sql')
}

sub run_command {
    my $command = shift;
    my $error   = shift;
    my $retries = shift;

    $retries = 1 unless $retries;

    do {
        print "Running: $command\n";
        my $exitcode = system($command);
        return if ( $exitcode == 0 );
        die "$error\n" if ( $retries <= 1 );
    } while ( --$retries > 0 );
}

sub download {
    my $url = shift;
    my $retries = shift;

    my $cmd = 'curl --silent -L -O --write-out \'%{http_code}\' ' . $url;

    do {
        print "Running: $cmd\n";
        my $http_code = `$cmd`;
        return if ( $http_code == 200 );
        if ( $http_code == 404 ) {
            print "404 response, skipping $url\n";
            # The response won't be a proper GZIP file, so remove it
            $url =~ m|.*/(.*)|;
            if ( -e $1 ) {
                run_command("rm $1", "Couldn't remove bad gzip file");
            }
            return;
        }
        die "could not download $url\n" if ( $retries <= 1 );
    } while ( --$retries > 0 );
}
