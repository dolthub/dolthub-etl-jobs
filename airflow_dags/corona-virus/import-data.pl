#!/usr/bin/perl -w 

use strict;

use Text::CSV qw( csv );

my $url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/';

my $csv_prefix = 'time_series_19-covid-';

my %sheets = (
    'Confirmed' => 'confirmed',
    'Recovered' => 'recovered',
    'Deaths'    => 'death'
    );

my $place_id_map = {
    'Mainland China' => {
	'Hubei'          => 1,
	'Guangdong'      => 2,
	'Henan'          => 3,
	'Hunan'          => 4,
	'Jiangxi'        => 5,
	'Anhui'          => 6,
	'Chongqing'      => 7,
	'Jiangsu'        => 8,
	'Shandong'       => 9,
	'Sichuan'        => 10,
	'Beijing'        => 11,
	'Shanghai'       => 12,
	'Fujian'         => 13,
	'Heilongjiang'   => 14,
	'Shaanxi'        => 15,
	'Guangxi'        => 16,
	'Hebei'          => 17,
	'Yunnan'         => 18,
	'Hainan'         => 19,
	'Liaoning'       => 20,
	'Shanxi'         => 21,
	'Tianjin'        => 22,
	'Guizhou'        => 23,
	'Gansu'          => 24,
	'Jilin'          => 25,
	'Inner Mongolia' => 26,
	'Ningxia'        => 27,
	'Xinjiang'       => 28,
	'Qinghai'        => 34,
	'Tibet'          => 59,
	'Zhejiang'       => 68,   
    },
    'Singapore' => {
	'' => 29,
    },
    'Thailand' => {
	'' => 30,
    },
    'Japan' => {
	'' => 31,
    },
    'Hong Kong' => {
	'Hong Kong' => 32,
    },
    'South Korea' => {
	'' => 33,
    },
    'Germany' => {
	'' => 35,
	'Bavaria' => 65,
    },
    'Malaysia' => {
	'' => 36,
    },
    'Taiwan' => {
	'Taiwan' => 37,
    },
    'Macau' => {
	'Macau' => 38,
    },
    'Vietnam' => {
	'' => 39,
    },
    'France' => {
	'' => 40,
    },
    'Australia' => {
	'New South Wales' => 41,
	'Victoria' => 42,
	'Queensland' => 43,
	'South Australia' => 45,
	'From Diamond Princess' => 81,
	'Western Australia' => 117,
	'Tasmania' => 134,    
	'Northern Territory' => 160,
    },
    'India' => {
	'' => 44,
    },
    'Canada' => {
	'Toronto, ON' => 46,
	'British Columbia' => 56,
	'London, ON' => 57,
	'Montreal, QC' => 109,    
    },
    'Italy' => {
	'' => 47,
    },
    'Philippines' => {
	'' => 48,
    },
    'Russia' => {
	'' => 49,
    },
    'UK' => {
	'' => 50,
    },
    'US' => {
	'Illinois'    => 51,
	'San Benito, CA' => 52,
	'Orange County, CA'  => 53,
	'Los Angeles, CA' => 0,
	'Santa Clara, CA' => 65,
	'Boston, MA'  => 63,
	'Washington'  => 66,
	'Arizona'     => 67,
	'Madison, WI' => 69,
	'King County, WA' => 70,
	'Cook County, IL' => 71,
	'Tempe, AZ'   => 72,
	'San Diego County, CA' => 75,
	'San Antonio, TX' => 76,
	'Omaha, NE (From Diamond Princess)' => 78,
	'Travis, CA (From Diamond Princess)' => 80,
	'Lackland, TX (From Diamond Princess)' => 82,
	'Humboldt County, CA' => 85,
	'Sacramento County, CA' => 86,
	'Unassigned Location (From Diamond Princess)' => 88,
	'Snohomish County, WA' => 123,
	'Providence, RI' => 129,   
	'Grafton County, NH' => 139,
	'Hillsborough, FL' => 140,
	'New York City, NY' => 141,
	'Placer County, CA' => 142,
	'San Mateo, CA' => 143,
	'Sarasota, FL' => 144,
	'Sonoma County, CA' => 145,
	'Umatilla, OR' => 146,
	'Fulton County, GA' => 147,
	'Washington County, OR' => 122,
	'Norfolk County, MA' => 152,
	'Berkeley, CA' => 153,
	'Maricopa County, AZ' => 154,
	'Wake County, NC' => 155,
	'Westchester County, NY' => 156,
	'Contra Costa County, CA' => 166,
	'Bergen County, NJ' => 170,
	'Harris County, TX' => 171,
	'San Francisco County, CA' => 172,
	'Clark County, NV' => 174,
	'Fort Bend County, TX' => 175,
	'Grant County, WA' => 176,
	'Queens County, NY' => 177,
	'Santa Rosa County, FL' => 178,
	'Williamson County, TN' => 179,    
    },
    'Belgium' => {
	'' => 54,
    },
    'Cambodia' => {
	'' => 55,
    },
    'Finland' => {
	'' => 58,
    },
    'Nepal' => {
	'' => 59,
    },
    'Spain' => {
	'' => 60,
    },
    'Sri Lanka' => {
	'' => 61,
    },
    'Sweden' => {
	'' => 62,
    },
    'United Arab Emirates' => {
	'' => 64,
    },
    'Others' => {
    	'Diamond Princess cruise ship' => 73,
	'' => 74,
    },
    'Egypt' => {
	'' => 77,
    },
    'Iran' => {
        '' => 79,
    },
    'Israel' => {
	'' => 83,
    },
    'Lebanon' => {
	'' => 84,
    },
    'Iraq' => {
	'' => 87,
    },
    'Oman' => {
	'' => 89, 
    },
    'Afghanistan' => {
	'' => 90,
    },
    'Bahrain' => {
	'' => 91,
    },
    'Kuwait' => {
	'' => 92,
    },
    'Austria' => {
	'' => 93,
    },
    'Algeria' => {
	'' => 94,
    },
    'Croatia' => {
	'' => 95,
    },
    'Switzerland' => {
	'' => 96,
    },
    'Pakistan' => {
        '' => 97,
    },
    'Brazil' => {
        '' => 98,
    },
    'Georgia' => {
        '' => 99,
    },
    'Greece' => {
        '' => 100,
    },
    'North Macedonia' => {
        '' => 101,
    },
    'Norway' => {
        '' => 102,
    },
    'Romania' => {
        '' => 103,
    },
    'Denmark' => {
	'' => 104,
    },
    'Estonia' => {
        '' => 105,
    },
    'Netherlands' => {
        '' => 106,
    },
    'San Marino' => {
        '' => 107,
    },  
    'Azerbaijan' => {
        '' => 108,
    },
    'Belarus' => {
	'' => 109,
    },
    'Iceland' => {
        '' => 111,
    },
    'Lithuania' => {
        '' => 112,
    },
    'Mexico' => {
        '' => 113,
    },
    'New Zealand' => {
        '' => 114,
    },
    'Nigeria' => {
        '' => 115,
    },
    'North Ireland' => {
        '' => 116,
    },
    'Ireland' => {
	'' => 118,
    },
    'Luxembourg' => {
	'' => 119,
    },
    'Monaco' => {
	'' => 120,
    },
    'Qatar' =>	{
	'' => 121,
    },
    'Ecuador' =>  {
        '' => 125,
    },
    'Czech Republic' =>  {
        '' => 126,
    },
    'Armenia' =>  {
        '' => 127,
    },
    'Dominican Republic' =>  {
        '' => 128,
    },
    'Indonesia' => {
	'' => 131,
    },
    'Portugal' => {
	'' => 132,
    },
    'Andorra' => {
	'' => 133,
    },
    'Latvia' => {
	'' => 135,
    },
    'Morocco' =>	{
        '' => 136,
    },
    'Saudi Arabia' =>	{
        '' => 137,
    },
    'Senegal' => {
	'' => 138,
    },
    'Argentina' => {
        '' => 149,
    },
    'Chile' => {
        '' => 150,
    },
    'Jordan' => {
        '' => 151,
    },
    'Ukraine' => {
	'' => 157,
    },
    'Saint Barthelemy' => {
	'' => 158,
    },
    'Hungary' => {
        '' => 159,
    },
    'Faroe Islands' => {
	'' => 161,
    },
    'Gibraltar' => {
	'' => 162,
    },
    'Liechtenstein' => {
	'' => 163,
    },
    'Poland' => {
	'' => 164,
    },
    'Tunisia' => {
	'' => 165,
    },
    'Palestine' => {
        '' => 167,
    },
    'Bosnia and Herzegovina' => {
        '' => 168,
    },
    'Slovenia' => {
        '' => 169,
    },
    'South Africa' => {
        '' => 173,
    },
};
    
# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'corona-virus';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
           "Could not clone repo $clone_path");

chdir($repo);

download_files($url, $csv_prefix, %sheets);

my ($places, $observations) = extract_data(%sheets);

import_data($places, $observations);

publish($url);

sub download_files {
    my $url_base   = shift;
    my $csv_prefix = shift;
    my %sheets     = @_;


    foreach my $sheet ( keys %sheets ) {
	my $url = $url_base . $csv_prefix . $sheet . '.csv';
	run_command("curl -f -L -o $sheet.csv '$url'",
		    "Could not download $url");
    }
}

sub extract_data {
    my %sheets = @_;

    my $places = {};
    my $observations = {};
    
    foreach my $sheet ( keys %sheets ) {
	my $csv = "$sheet.csv";

	my $data = csv(in => $csv);

	die "No data in $csv" unless @{$data};

	my @header = @{shift @{$data}};
	# Everything after column 4 is observations, so we'll grab those
	# as timestamps. This used to be column 5 but they removed first
	# confirmed date.
	my @timestamps = splice(@header, 4);

	# Build the places and $observations hashes from the data
	foreach my $row ( @{$data} ) {
	    # Remove leading spaces
	    $row->[0] =~ s/^\s+//g;
	    $row->[1] =~ s/^\s+//g;
	    
	    my $place_id = calculate_place_id($row->[1], $row->[0]);
	    $places->{$place_id}{'country'} = $row->[1];
	    $places->{$place_id}{'state'}   = $row->[0];
	    $places->{$place_id}{'lat'}     = $row->[2];
	    $places->{$place_id}{'long'}    = $row->[3];

	    my $current = '';
	    my $i = 0; # Use this to look up the timestamp
	    foreach my $observation ( splice @{$row}, 4 ) {
		if ( $observation eq '' or $observation eq $current) {
		    $i++;
		    next;
		}
		$current = $observation;
		die "Empty string in $current for $sheet, $place_id, $timestamps[$i]" if ( $current eq '' );
		my $date_time = convert_timestamp($timestamps[$i]);
		$observations->{$place_id}{$date_time}{lc($sheets{$sheet})} =
		    $observation;
		$i++;
	    }
	}
    }

    return ($places, $observations);
}

sub calculate_place_id {
    my $country = shift;
    my $state   = shift;
    
    # I think I just use a map here with country and state as keys.
    # If I get back undefined, error becuase I'm not sure if they found cases
    # in a new place or whether they updated an existing place.
    my $place_id = $place_id_map->{$country}{$state};

    die "Could not determine place_id for country: '$country' and state: '$state'" unless $place_id;

    return $place_id;
}

sub import_data {
    my $places       = shift;
    my $observations = shift;

    import_places($places);
    import_observations($observations);
}

sub import_places {
    my $places = shift;

    open PLACES_SQL, '>places.sql' or die "Could not open places.sql\n";
    print PLACES_SQL "delete from places;\n";
    
    foreach my $place_id ( keys %{$places} ) {
	my $country = $places->{$place_id}{'country'};
	my $state   = $places->{$place_id}{'state'};
	my $lat     = $places->{$place_id}{'lat'};
	my $long    = $places->{$place_id}{'long'};

	my $sql =
	    "insert into places (place_id, country_region, province_state, latitude, longitude) values ($place_id, '$country', '$state', $lat, $long);\n";
	print PLACES_SQL $sql;
    }
    close PLACES_SQL;
    
    run_command("dolt sql < places.sql", "Could not execute places.sql");
}

sub import_observations {
    my $observations = shift;

    open OBS_SQL, '>obs.sql' or die "Could not open obs.sql\n";
    print OBS_SQL "delete from cases;\n";

    foreach my $place_id ( keys %{$observations} ) {
	foreach my $date ( keys %{$observations->{$place_id}} ) {
	    my $sql =
		"insert into cases (place_id, observation_time) values ($place_id, '$date');\n";
	    print OBS_SQL $sql;
	    
	    my $cases = $observations->{$place_id}{$date};
	    die "No cases found for $place_id, $date" if ( !%{$cases} );
	    foreach my $type ( keys %{$cases} ) {
		my $count = $cases->{$type};
		my $column_name = $type . "_count";
		$sql = "update cases set $column_name = $count where place_id=$place_id and observation_time='$date';\n";
		print OBS_SQL $sql;
	    }
	}
    }
    close OBS_SQL;

    run_command("dolt sql < obs.sql", "Could not execute obs.sql");
}

sub convert_timestamp {
    my $timestamp = shift;

    my $month;
    my $day;
    my $year;
    if ( $timestamp =~ /^(\d+)\/(\d+)\/(\d+)$/ ) {
	$month  = $1;
	$day    = $2;
	$year   = $3;
    } else {
	die "Timestamp does not match expected format";
    }
	
    $day   = "0$day" if ( $day < 10 and $day =~ /^\d$/);
    $month = "0$month" if ( $month < 10 and $month =~ /^\d$/);
    $year  = "20$year" if ( $year == 20 );
    
    my $date_time = "$year-$month-$day 00:00:00";

    return $date_time;
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
 	"Automated import of cases and places tables downloaded from $url at $datestring GMT";

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
