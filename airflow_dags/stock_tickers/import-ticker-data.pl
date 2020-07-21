#!/usr/bin/perl -w 

use strict;

use Data::Dumper;
use POSIX qw/strftime/;
use Text::CSV qw( csv );

my $url_base = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&render=download&exchange=';

my @exchanges = (
    'nyse',
    'nasdaq',
    'amex',
    );

my %symbols_column_map = (
    'Symbol'   => 'symbol',
    'Name'     => 'name',
    'IPOyear'  => 'ipo_year',
    'Sector'   => 'sector',
    'industry' => 'industry',
    );

my %prices_column_map = (
    'Symbol'    => 'symbol',
    'LastSale'  => 'last_sale',
    'MarketCap' => 'market_cap',
    );

my $symbol_data = {};
my $prices_data = {};
    
# Clone the repository
my $organization = 'Liquidata';
my $repo         = 'stock-tickers';
my $clone_path   = "$organization/$repo"; 
run_command("dolt clone $clone_path", 
            "Could not clone repo $clone_path");

chdir($repo);

download_files($url_base, @exchanges);

extract_data($symbol_data,
	     $prices_data,
	     \@exchanges,
	     \%symbols_column_map,
	     \%prices_column_map);

market_caps_to_ints($prices_data);

import_data($symbol_data, $prices_data);

publish($url_base);

sub download_files {
    my $url_base  = shift;
    my @exchanges = @_;

    foreach my $exchange ( @exchanges ) {
	my $url        = $url_base . $exchange;
	my $file       = $exchange . '.csv';
	my $user_agent = "-H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'";
	run_command("curl -m 30 -f -L -o $file $user_agent '$url'",
		    "Could not download $url");
    }
}

sub extract_data {
    my $symbol_data     = shift;
    my $prices_data     = shift;
    my $exchanges       = shift;
    my $symbols_col_map = shift;
    my $prices_col_map  = shift;

    foreach my $exchange ( @{$exchanges} ) {
	process_csv($symbol_data, $exchange, $symbols_col_map, 'symbol');
	process_csv($prices_data, $exchange, $prices_col_map, 'symbol'); 
    }
}

sub process_csv {
    my $data     = shift;
    my $exchange = shift;
    my $col_map  = shift;
    my $key_name = shift;

    my $csv_array = csv(in => $exchange . '.csv');

    die "No data in $exchange.csv" unless @{$csv_array};

    my @header = @{shift @{$csv_array}};

    my %output_map;
    my $i = 0;
    foreach my $field ( @header ) {
        $output_map{$i} = $col_map->{$field}
            if ( $col_map->{$field} );
        $i++;
    }

    foreach my $row ( @{$csv_array} ) {
        my $row_data = {};
        foreach my $col_num ( sort keys %output_map ) {
            my $col_name = $output_map{$col_num};

            my $datapoint = $row->[$col_num];

            if ( $datapoint && $datapoint !~ /n\/a/) {
                $row_data->{$col_name} = $datapoint;
            }
        }

        my $key = $row_data->{$key_name}
            or die "Could not find $key_name";
        delete $row_data->{$key_name};
        $data->{$exchange}{$key} = $row_data;
    }
}

sub market_caps_to_ints {
    my $prices_data =shift;

    foreach my $exchange ( keys %{$prices_data} ) {
	foreach my $symbol ( keys %{$prices_data->{$exchange}} ) {
	    my $market_cap = $prices_data->{$exchange}{$symbol}{'market_cap'};
	    next unless $market_cap;
	    $market_cap =~ s/\$//g;

	    my $number;
	    my $scale;
	    if ( $market_cap =~ /(\d+\.\d+)([MBT]?)/ ) {
		$number = $1;
		$scale  = $2;
	    } elsif ( $market_cap =~ /(\d+)([MBT]?)/ ) {
		$number = $1;
                $scale  = $2;
	    } else {
		die "market_cap: $market_cap for $exchange, $symbol does not match expected format\n"
	    }

	    $number *= 1000000 if ( $scale eq 'M' );
	    $number *= 1000000000 if ( $scale eq 'B' );
	    $number *= 1000000000000 if ( $scale eq 'T' );

	    $prices_data->{$exchange}{$symbol}{'market_cap'} = $number;
	}
    }		     
}

sub import_data {
    my $symbol_data = shift;
    my $prices_data = shift;

    import_symbols($symbol_data);
    import_prices($prices_data);
}

sub import_symbols {
    my $data = shift;
    
    my $sql_file = 'symbols.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    print SQL "delete from symbols;\n";

    foreach my $exchange ( keys %{$data} ) {
	foreach my $symbol ( keys %{$data->{$exchange}} ) {
	    my $sql = "insert into symbols (exchange, symbol";
	    foreach my $column_name ( sort keys %{$data->{$exchange}{$symbol}} ) {
		$sql .= ", $column_name";
	    }
	    $sql .= ") values ('$exchange', '$symbol'";
	    foreach my $column_name ( sort keys %{$data->{$exchange}{$symbol}} ) {
		my $value = $data->{$exchange}{$symbol}{$column_name};
		if ( $value eq '' ) {
		    $sql .= ", NULL";
		} else {
		    $sql .= ", '$value'";
		}
	    }
	    $sql .= ");\n";
	    print SQL $sql;
	}
    }
    close SQL;

    run_command("dolt sql < $sql_file", "Could not execute generated SQL");
}

sub import_prices {
    my $data = shift;
    
    my $sql_file = 'prices.sql';
    open SQL, ">$sql_file" or die "Could not open $sql_file";

    my $time = strftime("%Y-%m-%d %H:%M:%S", gmtime());

    foreach my $exchange ( keys %{$data} ) {
        foreach my $symbol ( keys %{$data->{$exchange}} ) {
            my $sql = "insert into prices (exchange, symbol, input_date";
            foreach my $column_name ( sort keys %{$data->{$exchange}{$symbol}} ) {
                $sql .= ", $column_name";
            }
            $sql .= ") values ('$exchange', '$symbol', '$time'";
            foreach my $column_name ( sort keys %{$data->{$exchange}{$symbol}} ) {
                my $value = $data->{$exchange}{$symbol}{$column_name};
                if ( $value eq '' ) {
                    $sql .= ", NULL";
                } else {
                    $sql .= ", $value";
                }
            }
            $sql .= ");\n";
            print SQL $sql;
        }
    }
    close SQL;

    run_command("dolt sql < $sql_file", "Could not execute generated SQL");
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
