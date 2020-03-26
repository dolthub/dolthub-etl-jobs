#!/usr/bin/perl

use warnings;
use strict;

use Parse::CSV;
use Data::Dumper::Simple;
use Getopt::Long;

my $spread = 0.5;
GetOptions ("spread|s=f" => \$spread) or die ("Error in command line args");

open(my $fh, "<-:encoding(UTF-8)") or die $!;

my $num_sims = 10000;

my $csv = Parse::CSV->new(
    handle => $fh,
    names      => 1,
    );

my @reps;
while ( my $line = $csv->fetch ) {
    $line->{deaths} = 0;
    push @reps, $line;
}

my %deaths = (
    'Republican' => [],
    'Democratic' => [],
    'Independent' => [],
    );

foreach my $i ( 1..$num_sims ) {
    my @deaths;
    foreach my $rep ( @reps ) {
        # Roll the dice twice: once to see if they contract the disease, and again to see if they die
        if ( rand() < $spread && rand() < $rep->{mortality_rate} ) {
            $rep->{deaths} += 1;
            push @deaths, $rep;
        }
    }

    while ( my ($party, $death_count) = each %deaths ) {
        my @party_deaths = grep { $_->{party} eq $party } @deaths;
        push @$death_count, scalar(@party_deaths);
    }
}

foreach my $rep ( @reps ) {
    print "$rep->{last_name} died $rep->{deaths} times\n";
}

while ( my ($party, $death_count) = each %deaths ) {
    my @sorted_deaths = sort @$death_count;
    my $median_deaths = $sorted_deaths[$num_sims / 2];
    print "$party party lost a median $median_deaths representatives\n";
}
