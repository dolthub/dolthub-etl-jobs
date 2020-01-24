#!/usr/bin/perl -w 

use strict;

use JSON::Parse qw(parse_json);
use Data::Dumper;

$| = 1;

my $base_url = 'https://data.gharchive.org';

# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'github-repos';
my $clone_path   = "$organization/$repo"; 
#run_command("dolt clone $clone_path", 
#            "Could not clone repo $clone_path");

chdir($repo);

my $date_str = '2019-09-01';

download_files($base_url, $date_str);

my $events = get_pull_request_events();
generate_inserts($events);

# my $train_data = json_file_to_perl($train_file);
# my $dev_data   = json_file_to_perl($dev_file);

# my $paragraphs_sql = 'paragraphs.sql';
# open PARAGRAPHS, ">$paragraphs_sql" or 
#     die "Could not open $paragraphs_sql\n";
# binmode(PARAGRAPHS, ":utf8");
# print PARAGRAPHS "delete from paragraphs;\n";


# my $questions_sql = 'questions.sql';
# open QUESTIONS, ">$questions_sql" or
#     die "Could not open $questions_sql\n";
# binmode(QUESTIONS, ":utf8");
# print QUESTIONS "delete from questions;\n";

# my $answers_sql = 'answers.sql';
# open ANSWERS, ">$answers_sql" or
#     die "Could not open $answers_sql\n";
# binmode(ANSWERS, ":utf8");
# print ANSWERS "delete from answers;\n";

# generate_sql($train_data, $dev_data);

# close PARAGRAPHS;
# close QUESTIONS;
# close ANSWERS;

# execute_sql($paragraphs_sql, $questions_sql, $answers_sql);

# publish($base_url);

sub download_files {
    my $base     = shift;
    my $date_str = shift;

    # https://data.gharchive.org/2020-01-10-{0..23}.json.gz
    foreach my $hour ( 0..23 ) { 
        my $url = "$base/$date_str-$hour.json.gz";
        run_command("wget $url", "Could not download $url");
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
#        last;
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
        die "No base repo found" unless $base;
        die "no full name found for repo" unless $base->{'full_name'};
        my $name = quote_string($base->{'full_name'});
        my $id = $base->{'id'};
        my $description = quote_string($base->{'description'});
        my $owner = quote_string($base->{'owner'}{'login'});
        my $owner_type = quote_string($base->{'owner'}{'type'});
        my $created_at = quote_string($base->{'created_at'});
        my $updated_at = quote_string($base->{'updated_at'});
        my $fork = $base->{'fork'} ? 'TRUE' : 'FALSE';
        my $size = $base->{'size'} or 'NULL';
        my $star_count = $base->{'stargazers_count'} or 'NULL';
        my $fork_count = $base->{'forks_count'} or 'NULL';;
        my $open_issues_count = $base->{'open_issues_count'} or 'NULL';
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

sub quote_string {
    my $s = shift;
    return 'NULL' unless defined $s;
    $s =~ s/'/''/g;
    return "'$s'";
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
