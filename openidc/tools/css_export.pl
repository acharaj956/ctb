#!/usr/bin/perl
#
# css_export.pl — export the `arrival` relation from Postgres to a CSS 3.0
# flat file on stdout.
#
# This mirrors the IDC's proc2css / css2proc conversion utilities (part of the
# NDC-in-a-Box toolset): the database holds the processing results, and analysts
# and legacy tools exchange them as fixed-width CSS flat files.
#
# Usage (inside the perl-tools container, which has DBD::Pg and the PG* env set):
#   perl css_export.pl                 # all arrivals
#   perl css_export.pl --sta ARCES     # one station
#   perl css_export.pl --limit 100
#
use strict;
use warnings;
use DBI;
use Getopt::Long;

my ($sta, $limit);
GetOptions("sta=s" => \$sta, "limit=i" => \$limit) or die "bad arguments\n";

my $host = $ENV{PGHOST}     // 'postgres';
my $port = $ENV{PGPORT}     // '5432';
my $db   = $ENV{PGDATABASE} // 'openidc';
my $user = $ENV{PGUSER}     // 'idc';
my $pass = $ENV{PGPASSWORD} // 'idc';

my $dbh = DBI->connect(
    "dbi:Pg:dbname=$db;host=$host;port=$port",
    $user, $pass,
    { RaiseError => 1, AutoCommit => 1, PrintError => 0 },
) or die "cannot connect to Postgres: $DBI::errstr\n";

# Build the query safely with a bind parameter for the optional station filter.
my $sql = "SELECT sta, chan, time, arid, jdate, iphase, snr, auth FROM arrival";
my @bind;
if (defined $sta) {
    $sql .= " WHERE sta = ?";
    push @bind, $sta;
}
$sql .= " ORDER BY time";
$sql .= " LIMIT " . int($limit) if defined $limit;   # int() guards against injection

my $sth = $dbh->prepare($sql);
$sth->execute(@bind);

# CSS 3.0 `arrival` flat-file layout (subset): fixed-width, space-separated.
# sta(6) chan(8) time(17.5) arid(9) jdate(8) iphase(8) snr(10.2) auth(15)
my $n = 0;
while (my $r = $sth->fetchrow_hashref) {
    printf "%-6s %-8s %17.5f %9d %8d %-8s %10.2f %-15s\n",
        $r->{sta}, $r->{chan}, $r->{time}, $r->{arid},
        ($r->{jdate} // 0), ($r->{iphase} // '-'),
        ($r->{snr} // -1), ($r->{auth} // '-');
    $n++;
}

$sth->finish;
$dbh->disconnect;
print STDERR "css_export: wrote $n arrival rows in CSS 3.0 flat-file format\n";
