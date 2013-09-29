#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Date::Parse;
use Date::Language;
use POSIX qw(strftime);
use Getopt::Long;
use Cwd 'cwd';
use Data::Dumper;
use RRD::Simple;

our ($DBH, $SV, $SERVER, $DATABASE, $USERNAME, $PASSWORD, $VERBOSE, $SERVERNAME, $VHOST);
our ($STARTTIME, $ENDTIME, $LIMIT, $HELP);
$DATABASE = 'apache';
$USERNAME = 'apache';
$SERVER = 'localhost';
$VERBOSE = 0;
$STARTTIME = time() - (24*60*60);
$ENDTIME = time();

# TODO: this should be the general size of an apache process that is not
# performing some custom php operation, like a file download, and should 
# be in the same units as %{mod_php_memory_usage}n (bytes)
my $DEFAULTMEM = 0;

my $result = GetOptions (
	"database=s" => \$DATABASE,
	"username=s" => \$USERNAME,
	"password=s" => \$PASSWORD,
	"server=s"   => \$SERVER,
	"servername=s"   => \$SERVERNAME,
	"start=s"    => \$STARTTIME, # string
	"end=s"      => \$ENDTIME, # string
	"vhost=s"    => \$VHOST, # string
	"limit=i"    => \$LIMIT, # integer
	"help"			 => \$HELP,
	"verbose+"   => \$VERBOSE); # flag

if ($HELP  || !$VHOST || !$PASSWORD) {
	usage();
	exit();
}

# parse the dates if they were passed in
my $lang = Date::Language->new('English');
if ($STARTTIME !~ /^\d+$/) {
  $STARTTIME = $lang->str2time($STARTTIME);
}
if ($ENDTIME !~ /^\d+$/) {
  $ENDTIME = $lang->str2time($ENDTIME);
}

$DBH = DBI->connect("DBI:mysql:$DATABASE;host=$SERVER", $USERNAME, $PASSWORD)
		|| die "Could not connect to database: $DBI::errstr";

# Build the query
my $query = 'SELECT memory FROM logentries';

$query .= 'LEFT JOIN server ON (logentries.id = server.logentry_id)'
	if ($SERVERNAME);

$query .= ' WHERE time > ? and time < ? ';

if ($VHOST != 'ALL') {
  $query .= 'AND server_name = ?';
}

$query .= " AND server.name = '$SERVERNAME'"
	if ($SERVERNAME);

$query .= ' limit '. $LIMIT
	if ($LIMIT);

my $sth = $DBH->prepare($query);

my $fname = "$$.$VHOST.";
if ($SERVERNAME) {
  $fname = "$SERVERNAME.$VHOST.";
} 

my $rrdMem = RRD::Simple->new( file => $fname ."memory.rrd" );
$rrdMem->create( Memory => "GAUGE");

my $rrdReq = RRD::Simple->new( file => $fname ."requests.rrd" );
$rrdReq->create( Requests => "GAUGE");

my $curtime = $STARTTIME;

while ($curtime < $ENDTIME) {

	# we are working on 5 minute intervals
	my $endinterval = $curtime + 60*5;
	my $requests = 0;
	my $memory = 0;

	my $sCurtime = strftime("%F %H:%M:%S", localtime($curtime));
	my $sEndinterval = strftime("%F %H:%M:%S", localtime($endinterval));
	print "$query => ($sCurtime, $sEndinterval, $VHOST)\n"
		if ($VERBOSE > 1);
        if ($VHOST == 'ALL') {
          $sth->execute($sCurtime, $sEndinterval);
        } else {
          $sth->execute($sCurtime, $sEndinterval, $VHOST);
        }

	while (my $le = $sth->fetchrow_hashref()) {
		$requests++;
		print "$requests: ". $le->{memory} ."\n"
			if ($VERBOSE > 1);
		$memory += defined($le->{memory}) ? $le->{memory} : $DEFAULTMEM;
	}

	print "Update [". strftime("%F %H:%M:%S", localtime($endinterval)) 
		."] => Requests: $requests, Memory: $memory\n"
		if $VERBOSE;
	$rrdMem->update($fname .'memory.rrd', $endinterval, Memory => $memory);
	$rrdReq->update($fname .'requests.rrd', $endinterval, Requests => $requests);

	$curtime = $endinterval;
}

#TODO: this needs to be configured to use the time frame specified on the command line
# Generate graphs:
# /var/tmp/myfile-daily.png, /var/tmp/myfile-weekly.png
# /var/tmp/myfile-monthly.png, /var/tmp/myfile-annual.png
my %rtn = $rrdMem->graph(
	destination => cwd(),
	title => "Memory usage for ". $VHOST . ($SERVERNAME?" on $SERVERNAME":''),
	vertical_label => "Bytes",
        periods => [ qw(week) ],
	interlaced => ""
);
printf("Created %s\n",join(", ",map { $rtn{$_}->[0] } keys %rtn))
	if $VERBOSE;

%rtn = $rrdReq->graph(
	destination => cwd(),
	title => "Requests for ". $VHOST . ($SERVERNAME?" on $SERVERNAME":''),
	vertical_label => "Count",
        periods => [ qw(week) ],
	interlaced => ""
);
printf("Created %s\n",join(", ",map { $rtn{$_}->[0] } keys %rtn))
	if $VERBOSE;

sub usage {
print <<END;
$0 --vhost <vhostname> --password <db_password> [<options>]
Generate an RRD file containing memory and number of requests in 5 min
intervals from the apache database (populated by apache_to_mysql.pl).

--servername        : The hostname of the server that the vhost is on
--vhost             : The name of the vhost we are interested in
--database          : Name of the database to use [default: apache]
--username          : The username to connect to the database with [default:
                    apache]
--password          : The password to use when connecting to the database
--server            : The server where the DB is hosted [default: localhost]
--start             : The start time of logentries you are interested in
                    [default: 24 hours ago]
--end               : The end time of logentries you are interested in 
                    [default: now]
--limit             : Do not include more then this many entries from the DB 
--verbose           : Increase verbosity of output.  This can be repeated.
--help              : Print this help message

END
}
