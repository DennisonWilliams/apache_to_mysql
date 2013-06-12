#!/usr/bin/perl
# TODO: create instalation and usage instructions in pod format

# INSTALLATION INSTRUCTIONS

# Create a database to stoore the log entries
# CREATE USER 'apache'@'localhost' IDENTIFIED BY '***';
# GRANT USAGE ON * . * TO 'apache'@'localhost' IDENTIFIED BY '***' WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0 ;
# CREATE DATABASE IF NOT EXISTS `apache` ;
# GRANT ALL PRIVILEGES ON `apache` . * TO 'apache'@'localhost';

# Update the apache configuration with the following configuraion
use strict;
use warnings;
use DBI;
use Data::Dumper;
use Date::Parse;
use Date::Language;
use POSIX qw(strftime);
use Getopt::Long;

# global variables
our ($DBH, $SV, $SERVER, $DATABASE, $USERNAME, $PASSWORD, $VERBOSE, $INTERVAL);
$DATABASE = 'apache';
$USERNAME = 'apache';
$VERBOSE = 1;

# Interval is the number of minutes in which we will be sending reports
# and is used by the report function
$INTERVAL = 5;

our ($QUERY, $STARTTIME, $ENDTIME, $VHOST, $LIMIT, $REPORT);
my $result = GetOptions (
	"database=s" => \$DATABASE,
	"username=s" => \$USERNAME,
	"password=s" => \$PASSWORD,
	"server=s"   => \$SERVER,
	"query=s"    => \$QUERY, # string
	"start=s"    => \$STARTTIME, # string
	"end=s"      => \$ENDTIME, # string
	"vhost=s"    => \$VHOST, # string
	"limit=i"    => \$LIMIT, # integer
	"report"		 => \$REPORT,
	"verbose"    => \$VERBOSE); # flag

# Schema version.  Increment this each time there is a change to the DB schema,
# include the appropriate update code in the update method, and update the 
# install method.
$SV = 1;

# TODO: make this an all purpose script that can also generate reports from 
# the database
initDB();

# If a query argument is specified then we are generating a report not adding
# log entries to the database
if ($QUERY) {
	query();
	exit;
}
elsif ($REPORT) {
	report();
	exit;
}

# A statement handler to add log entries to the database
# We will be using this log directive:
# LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" ::: %{mod_php_memory_usage}n %D %O %v %U %q" combined-php
# 127.0.0.1 - - [14/Dec/2012:14:56:58 -0500] "OPTIONS * HTTP/1.0" 200 - "-" "Apache/2.4.3 (Unix) OpenSSL/1.0.1c PHP/5.4.7 (internal dummy connection)" ::: - 1256 148 ushahidi-fresh.march-hare.org *
# %h - Remote Host
# %u - Remote user (from auth; may be bogus if return status (%s) is 401)
# %t - Time the request was received (standard english format)
# "%r" - First line of request
# %>s - Status. For requests that got internally redirected, this is the status of the *original* request --- %>s for the last.
# %b - Size of response in bytes, excluding HTTP headers. In CLF format, i.e. a '-' rather than a 0 when no bytes are sent.
# %D - The time taken to serve the request, in microseconds.
# %O - Bytes sent, including headers, cannot be zero. You need to enable mod_logio to use this.
# %v - vhost
# %U - url
# %q - query string
my $sth = $DBH->prepare('INSERT INTO logentries(remote_host, time, server_name, tts, size, url, params, memory) values(?, ?, ?, ?, ?, ?, ?, ?)')
    || die "$DBI::errstr";

# Process logs from STDIN
my $lang = Date::Language->new('English');
my ($remote_host, $time, $servername, $tts, $size, $url, $params, $memory);
while (<STDIN>) {
	# pull in fields of interest from a RE
	/
		([^\s]+)						# %h - Remote Host
		\s
		(?:[^\s]+\s){2}			# %l %u
		\[([^\]]+)\]				# %t - Time the request was recieved
		\s
		"[^\"]+"						# %r - First line of request
		\s
		(?:[^\s]+)					# %>s - Status
		\s
		([^\s]+)						# %b - Size of response
		.*\s:::							# Everything up to the ':::' divider
		
		(?:
			\s
			([^\s]+)					# %{mod_php_memory_usage}n
			\s
			([^\s]+)					# %D - tts
			\s
			([^\s]+)					# %O - bytes sent
			\s
			([^\s]+)					# %v - vhost
			\s
			([^\s]+)					# %U - url
			(?:
				\s
				([^\s]+)				# %q - params
			)?
		)?								# This whole section may not exist
	/x or next;
	$remote_host = $1;
	$time = strftime "%F %H:%M:%S", localtime($lang->str2time($2));
	$memory = ($4 eq "-")?'':$4;
	$tts = $5;
	$size = ($6 && ($6 ne "-"))?$6:$3;
	$servername = $7;
	$url = ($8 && ($8 ne '*'))?$8:'';
	$params = $9;
	print "$_\n\$remote_host =>$remote_host\n\$time => $time\n\$servername => $servername\n\$tts => $tts\n\$size => $size\n\$url => $url\n\$params => $params\n\$memory => $memory\n\n";
	$sth->execute($remote_host, $time, $servername, $tts, $size, $url, $params, $memory);
}

# Close the database connection
$sth->finish();
$DBH->disconnect();

# TODO: create functionality to install the DB schema if it dooes not exist
# TODO: create a custom error handler that sends an alert to nagios
# TODO: is there a limit for the duration that we can keep a connection to the
# DB open for?
sub initDB{
	my ($sth, @data);
	
	# Connect to the database
	$DBH = DBI->connect("DBI:mysql:$DATABASE;host=$SERVER", $USERNAME, $PASSWORD)
			|| die "Could not connect to database: $DBI::errstr";

	# TODO: this will generate a error if the schema has not been installed yet,
	# but will not fail.
	$sth = $DBH->prepare( 
		"SELECT value FROM variables WHERE `key`='schema_version'"
	);
	$sth->execute();
	my $ref = $sth->fetchrow_hashref();
	$sth->finish();
	my $schema_version = $ref->{'value'};

	if (!defined($schema_version)) {
			install();
			$sth = $DBH->prepare("INSERT INTO variables(`key`, `value`) values(?, ?)");
			$sth->execute('schema_version', 1);
			$schema_version = 1;
	}

	# Upgrade starts here
	if ($schema_version == 1) {
			$sth = $DBH->prepare(
				"CREATE TABLE reporting (
					`` VARCHAR(255) NOT NULL,
					`time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
					PRIMARY KEY (`vhost`)
			)");
			$sth->execute();
			$sth->finish();

			$sth = $DBH->prepare("UPDATE variables SET `value`=? where `key`=?");
			$sth->execute(2, 'schema_version');
			$schema_version = 2;
	}
}

sub install {
	my $sth;
	$sth = $DBH->prepare(
		"CREATE TABLE variables (
			`key` VARCHAR(255) NOT NULL,
			`value` VARCHAR(255),
			PRIMARY KEY (`key`)
	)");
	$sth->execute();
	$sth->finish();

	$sth = $DBH->prepare(
		"CREATE TABLE logentries (
			id INT NOT NULL AUTO_INCREMENT,
			time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			remote_host VARCHAR(255) NOT NULL,
			server_name VARCHAR(255),
			tts INT,
			size INT NOT NULL,
			url VARCHAR(255),
			params VARCHAR(255),
			memory INT,

			PRIMARY KEY(id),
			INDEX(remote_host, server_name, url)			
	)");
	$sth->execute();
	$sth->finish();
}

sub query {
	# Build the query
	my $where = 0;
	my $query = "SELECT time,server_name,url,params,tts,size,memory FROM logentries";

	# The filter set is defined in command line arguments
	if ($VHOST) {
		if (!$where) {
			$query .= " WHERE ";
			$where++;
		}
		$query .= "server_name='$VHOST'";
	}

	if ($STARTTIME) {
		if (!$where) {
			$query .= " WHERE ";
		  $where++;
		}
		else {
			$query .= ' AND ';
		}

		$query .= "time > '$STARTTIME'";
	}

	if ($ENDTIME) {
		if (!$where) {
			$query .= " WHERE ";
		  $where++;
		}
		else {
			$query .= ' AND ';
		}

		$query .= "time < '$ENDTIME'";
	}

	# TODO: clean this up so that users can't accidentally fuck the schema
	$query .= " ORDER BY $QUERY DESC";

	if ($LIMIT) {
		$query .= " LIMIT $LIMIT";
	}

	print "$query\n" if $VERBOSE;

	my $sth = $DBH->prepare($query) ||
    die "$DBI::errstr";
	$sth->execute();
	while (my $row = $sth->fetchrow_hashref()) {
		#print Dumper($row) ."\n";
		print $row->{time} 
			."\t". $row->{$QUERY} ."\thttp://". $row->{server_name} . $row->{url} . $row->{params} ."\n";
		
	}
}

# TODO: count, bytes sent, memory used, tts
sub report {
	my ($sth, $time);
	$sth = $DBH->prepare(
		'SELECT server_name,SUM(tts) as tts,SUM(size) as size,SUM(memory) as memory FROM logentries 
		WHERE time>? 
		GROUP by server_name'
	);
	$sth->execute(strftime("%F %H:%M:%S", localtime(time()-$INTERVAL*60)));
	while (my $row = $sth->fetchrow_hashref()) {
		print $row->{server_name}
			."\ttts: ". $row->{tts} ."\tsize: ". $row->{size} ."\tmemory: ". $row->{memory} ."\n";
	}
}
