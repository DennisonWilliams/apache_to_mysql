#!/usr/bin/perl
# TODO: create instalation and usage instructions in pod format

# INSTALLATION INSTRUCTIONS

# Create a database to stoore the log entries
# CREATE USER 'apache'@'localhost' IDENTIFIED BY '***';
# GRANT USAGE ON * . * TO 'apache'@'localhost' IDENTIFIED BY '***' WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0 ;
# CREATE DATABASE IF NOT EXISTS `apache` ;
# GRANT ALL PRIVILEGES ON `apache` . * TO 'apache'@'localhost';

# USAGE INSTRUCTIONS

# Import a log file into the database.  Check against the different examples
# below to see what log file formatr to specify in the --format argument
# $ zcat all-domains*5*| \
#   ./apache_to_mysql.pl --password='apache' \
#   --format=combined-php-forensics-ee

# mysql> select server_name,count(*) as ct from logentries where
#   time>='2015-06-18 13:00' and time<='2015-06-18 14:00' group by server_name order

use strict;
use warnings;
use DBI;
use Data::Dumper;
use Date::Parse;
use Date::Language;
use POSIX qw(strftime);
use Getopt::Long;

# global variables
our ($DBH, $SV, $SERVER, $DATABASE, $USERNAME, $PASSWORD, $VERBOSE, $INTERVAL, $FORMAT);
$DATABASE = 'apache';
$USERNAME = 'apache';
$SERVER = 'localhost';
$VERBOSE = 0;
$FORMAT = 'combined-php';

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
	"report"     => \$REPORT,
  "format=s"   => \$FORMAT,
	"verbose+"    => \$VERBOSE); # flag

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
my $sth = $DBH->prepare('INSERT INTO logentries(remote_host, time, server_name, tts, size, url, params, memory) values(?, ?, ?, ?, ?, ?, ?, ?)')
    || die "$DBI::errstr";

# Process logs from STDIN
my $lang = Date::Language->new('English');
my ($remote_host, $time, $servername, $tts, $size, $url, $params, $memory);
while (<STDIN>) {
  my ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params);
  if ($FORMAT eq 'combined') {
    ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params) = parse_combined($_);
  } elsif ($FORMAT eq 'combined-ee') {
    ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params) = parse_combined_ee($_);
  } elsif ($FORMAT eq 'combined-php') {
    ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params) = parse_combined_php($_);
  } elsif ($FORMAT eq 'combined-php-forensics') {
    ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params) = parse_combined_php_forensics($_);
		next if ($VERBOSE > 1);
  } elsif ($FORMAT eq 'combined-php-forensics-ee') {
    ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params) = parse_combined_php_forensics_ee($_);
		next if ($VERBOSE > 1);
  }
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
					`vhost` VARCHAR(255) NOT NULL,
					`time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
					PRIMARY KEY (`vhost`)
			)");
			$sth->execute();
			$sth->finish();

			$sth = $DBH->prepare("UPDATE variables SET `value`=? where `key`=?");
			$sth->execute(2, 'schema_version');
			$schema_version = 2;
	} elsif ($schema_version == 2) {
          $sth = $DBH->prepare( "ALTER TABLE logentries DROP KEY remote_host" );
          $sth->execute();
          $sth->finish();

          $sth = $DBH->prepare( "alter table logentries add key remote_host (remote_host)" );
          $sth->execute();
          $sth->finish();

          $sth = $DBH->prepare( "alter table logentries add key time (time)" );
          $sth->execute();
          $sth->finish();

          $sth = $DBH->prepare( "alter table logentries add key url (url)" );
          $sth->execute();
          $sth->finish();

          $sth = $DBH->prepare("UPDATE variables SET `value`=? where `key`=?");
          $sth->execute(3, 'schema_version');
          $schema_version = 3;
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
			INDEX(remote_host, server_name, url, time)			
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
sub parse_combined_php {
  my ($line) = @_;
  # pull in fields of interest from a RE
  $line =~ /
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
  print "$_\n\$remote_host =>$remote_host\n\$time => $time\n".
    "\$servername => $servername\n\$tts => $tts\n\$size => $size\n".
    "\$url => $url\n\$params => $params\n\$memory => $memory\n\n"
    if $VERBOSE;
  return ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params);
}

# We will be using this log directive:
# LogFormat "%h %l %u %v %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
# 50.116.20.205 - - teamsters856.org [16/Jun/2015:00:05:43 -0700] "GET / HTTP/1.1" 200 33340 "-" "WWW-Mechanize/1.71"
# %h - Remote Host
# %l - Remote logname (from identd, if supplied). This will return a dash unless mod_ident is present and IdentityCheck is set On.
# %u - Remote user (from auth; may be bogus if return status (%s) is 401)
# %v - Vhost name
# %t - Time the request was received (standard english format)
# "%r" - First line of request
# %>s - Status. For requests that got internally redirected, this is the status of the *original* request --- %>s for the last.
# %b - Size of response in bytes, excluding HTTP headers. In CLF format, i.e. a '-' rather than a 0 when no bytes are sent.
# %{Referer}i - The Referer header from the client
# %{User-Agent}i - The User-Agent header ffrom the client
sub parse_combined_ee {
  my ($line) = @_;
  # pull in fields of interest from a RE
  $line =~ /
    ([^\s]+)				# %h - Remote Host
    \s
    (?:[^\s]+\s){2}			# %l %u
    ([^\s]+)				# %v - VHost name
    \s
    \[([^\]]+)\]			# %t - Time the request was recieved
    \s
    "\w+\s([^ ]+)\s[^"]+"				# %r - First line of request
    \s
    (?:[^\s]+)				# %>s - Status
    \s
    ([^\s]+)				# %b - Size of response
  /x;

  $remote_host = $1;
  $time = strftime "%F %H:%M:%S", localtime($lang->str2time($3));
  $memory = '';
  $tts = '';
  $size = $5;
  # TODO: this does not make a whole lot of sense
  $servername = $2;
  $url = $4;
  $params = '';
  if (!$remote_host || !$time) {
    print "MATCH FAIL: ". $line;
    return;
  } else {
    print "$_\n\$remote_host =>$remote_host\n\$time => $time\n".
      "\$servername => $servername\n\$tts => $tts\n\$size => $size\n".
      "\$url => $url\n\$params => $params\n\$memory => $memory\n\n"
      if $VERBOSE;
  }
  return ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params);
}

# We will be using this log directive:
# LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" ::: %{forensic-id}n %{mod_php_memory_usage}n %D %O http://%v%U%q" combined-php-forensics
# 67.207.151.21 - - [13/Sep/2013:10:19:36 -0700] "GET / HTTP/1.1" 200 4057 "http://208.78.97.143" "WWW-Mechanize/1.64" ::: 24489:52334928:5c 786432 1746 4276 http://compare50.org/index.php
# %h - Remote Host
# %l - Remote logname (from identd, if supplied). This will return a dash unless mod_ident is present and IdentityCheck is set On.
# %u - Remote user (from auth; may be bogus if return status (%s) is 401)
# %t - Time the request was received (standard english format)
# "%r" - First line of request
# %>s - Status. For requests that got internally redirected, this is the status of the *original* request --- %>s for the last.
# %b - Size of response in bytes, excluding HTTP headers. In CLF format, i.e. a '-' rather than a 0 when no bytes are sent.
# %{Referer}i - The Referer header from the client
# %{User-Agent}i - The User-Agent header ffrom the client
# %{forensic-id}n - The unique forensics id assigned by mod_forensics
# %{mod_php_memory_usage}n - The amount of memory used by php
# %D - The time taken to serve the request, in microseconds.
# %O - Bytes sent, including headers, cannot be zero. You need to enable mod_logio to use this.
# http://%v%U%q - A string representing the requested url where:
#   %v - vhost
#   %U - url
#   %q - query string
sub parse_combined_php_forensics {
  my ($line) = @_;
  # pull in fields of interest from a RE
  $line =~ /
          ([^\s]+)						# %h - Remote Host, $1
          \s
          (?:[^\s]+\s){2}			# %l %u
          \[([^\]]+)\]				# %t - Time the request was recieved, $2
          \s
          "[^\"]+"						# %r - First line of request
          \s
          (?:[^\s]+)					# %>s - Status
          \s
          ([^\s]+)						# %b - Size of response, $3
          .*\s:::							# Everything up to the ':::' divider
          
          (?:
                  \s
                  [^\s]+					# %{forensic-id}n
                  \s
                  ([^\s]+)					# %{mod_php_memory_usage}n, $4
                  \s
                  ([^\s]+)					# %D - tts, $5
                  \s
                  ([^\s]+)					# %O - bytes sent, $6
                  \s
                  ([^\s]+)					# http:FOO, $7
          )?								# This whole section may not exist
  /x or next;
  my $remote_host = $1;
  my $time = strftime "%F %H:%M:%S", localtime($lang->str2time($2));
  my $memory = ($4 eq "-")?'':$4;
  my $tts = $5;
  my $size = ($6 && ($6 ne "-"))?$6:$3;
  my $url = $7;
  my $servername = '';
  my $params = '';
  if ($url =~ /http:\/\/([^\/]*)(?:\/([^?]*)(?:\?(.*))?)?/) {
    $servername = $1;
    $url = $2;
    $params = $3;
  }
  print "$_\n\$remote_host =>$remote_host\n\$time => $time\n".
    "\$servername => $servername\n\$tts => $tts\n\$size => $size\n".
    "\$url => $url\n\$params => $params\n\$memory => $memory\n\n"
    if $VERBOSE;
  return ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params);
}

# We will be using this log directive:
# LogFormat "%h %l %u %v %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" ::: %{forensic-id}n %{mod_php_memory_usage}n %D %O http://%v%U%q" combined-php
# 157.55.34.29 - - speakoutnow.org [19/Sep/2013:00:00:09 -0700] "GET /melanie-demore-performs-at-911-memorial-concert HTTP/1.1" 302 - "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" ::: Ujqg@dBa12IAAQSVEL0AAAAF 15466496 163644 364 http://speakoutnow.org/melanie-demore-performs-at-911-memorial-concert?q_url=melanie-demore-performs-at-911-memorial-concert
# %h - Remote Host
# %l - Remote logname (from identd, if supplied). This will return a dash unless mod_ident is present and IdentityCheck is set On.
# %u - Remote user (from auth; may be bogus if return status (%s) is 401)
# %v - Vhost name
# %t - Time the request was received (standard english format)
# "%r" - First line of request
# %>s - Status. For requests that got internally redirected, this is the status of the *original* request --- %>s for the last.
# %b - Size of response in bytes, excluding HTTP headers. In CLF format, i.e. a '-' rather than a 0 when no bytes are sent.
# %{Referer}i - The Referer header from the client
# %{User-Agent}i - The User-Agent header ffrom the client
# %{forensic-id}n - The unique forensics id assigned by mod_forensics
# %{mod_php_memory_usage}n - The amount of memory used by php
# %D - The time taken to serve the request, in microseconds.
# %O - Bytes sent, including headers, cannot be zero. You need to enable mod_logio to use this.
# http://%v%U%q - A string representing the requested url where:
#   %v - vhost
#   %U - url
#   %q - query string
sub parse_combined_php_forensics_ee {
  my ($line) = @_;
  # pull in fields of interest from a RE
  $line =~ /
          ([^\s]+)						# %h - Remote Host, $1
          \s
          (?:[^\s]+\s){2}			# %l %u
					([^\s]+)            # %v $2
					\s
          \[([^\]]+)\]				# %t - Time the request was recieved, $3
          \s
          "[^\"]+"						# %r - First line of request
          \s
          (?:[^\s]+)					# %>s - Status
          \s
          ([^\s]+)						# %b - Size of response, $4
          .*\s:::							# Everything up to the ':::' divider
          
          (?:
                  \s
                  [^\s]+					# %{forensic-id}n
                  \s
                  ([^\s]+)					# %{mod_php_memory_usage}n, $5
                  \s
                  ([^\s]+)					# %D - tts, $6
                  \s
                  ([^\s]+)					# %O - bytes sent, $7
                  \s
                  ([^\s]+)					# http:FOO, $8
          )?								# This whole section may not exist
  /x or next;
  my $remote_host = $1;
  my $time = strftime "%F %H:%M:%S", localtime($lang->str2time($3));
  my $memory = ($5 eq "-")?'':$5;
  my $tts = $6;
  my $size = ($7 && ($7 ne "-"))?$7:$4;
  my $url = $8;
  my $servername = $2;
  my $params = '';
  if ($url =~ /http:\/\/([^\/]*)(?:\/([^?]*)(?:\?(.*))?)?/) {
    #$servername = $1;
    $url = $2;
    $params = $3;
  }
  print "$_\n\$remote_host =>$remote_host\n\$time => $time\n".
    "\$servername => $servername\n\$tts => $tts\n\$size => $size\n".
    "\$url => $url\n\$params => $params\n\$memory => $memory\n\n"
    if $VERBOSE;
  return ($remote_host, $time, $memory, $tts, $size, $servername, $url, $params);
}
