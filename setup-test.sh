#!/bin/bash

# Usage:
#
# # Using default PostgreSQL version
# # Installs tests in the /tmp/barman-tests directory
# ./setup-test.sh
#
# # Override PostgreSQL version
# export PATH=/usr/pgsql-9.0/bin:$PATH
# ./setup-test.sh
#
# Requirements:
#
# This script requires SSH identity exchanged with
# ssh-copy-id ~/.ssh/id_rsa.pub localhost

# Test directory (main)
BARMANTEST_DIR=${BARMANTEST_DIR:-/tmp/barman-tests}

SEARCH_PATH=(
    /usr/lib/postgresql/{9.1,9.0,8.4}/bin
    /usr/pgsql-{9.1,9.0,8.4}/bin
    /Library/PostgreSQL/{9.1,9.0,8.4}/bin
)

######################################################
# 1 - Detect PostgreSQL version
######################################################

INITDB=$(which initdb)
if [ -z "$INITDB" ]
then
    for dir in "${SEARCH_PATH[@]}"
    do
	if [ -x "$dir/initdb" ]
	then
	    INITDB="$dir/initdb"
	    echo "PostgreSQL directory is not in your default PATH."
	    echo "Please add $dir to your PATH environment variable as follow:"
	    echo
	    echo "	export PATH=\"$dir:\$PATH\""
	    echo
	    export PATH="$dir:$PATH"
	    break
	fi
    done
fi

if [ -z "$INITDB" ]
then
    echo "Cannot find initb"
    exit -1
fi

INITDB_VERSION=($(initdb -V | sed -e 's/^.* //; s/\([0-9][0-9]*\.[0-9][0-9]*\)\.*\([^ ]*\).*/\1 \2/'))

if [ "${#INITDB_VERSION[@]}" != 2 ]
then
	echo "Cannot detect initdb's version"
	exit -1
fi

MAJOR_RELEASE="${INITDB_VERSION[0]}"
MICRO_RELEASE="${INITDB_VERSION[1]}"
PROPOSED_PORT="54${MAJOR_RELEASE/./}"
PGPORT=${PGPORT:-$PROPOSED_PORT}
echo "Detected PostgreSQL $MAJOR_RELEASE (minor $MICRO_RELEASE)"

######################################################
# 2 - Check ssh key exchange
######################################################

ssh -o BatchMode=yes -o StrictHostKeyChecking=no localhost true
if [ $? -ne 0 ]
then
        echo "Error: ssh cannot access localhost without password"
	echo "HINT: check ~/.ssh/authorized_keys"
        exit -1
fi


######################################################
# 3 - Setup barman tests directory
######################################################

if [ ! -d $BARMANTEST_DIR ]
then
	mkdir -p $BARMANTEST_DIR
fi

TEST_DIR=$BARMANTEST_DIR/$MAJOR_RELEASE
if [ -d $TEST_DIR ]
then
	echo "Directory $TEST_DIR already exists. Cannot continue"
	exit -1
fi

DR_DIR=$TEST_DIR/backup-node
ARCHIVE_DIR=$DR_DIR/wals
BASEBACKUP_DIR=$DR_DIR/base
MASTER_DIR=$TEST_DIR/master-node
PGDATA=$MASTER_DIR/data

mkdir -p $PGDATA
mkdir -p $ARCHIVE_DIR
mkdir -p $BASEBACKUP_DIR

echo "Server will be installed in $PGDATA and running on port $PGPORT"

export PGDATA
export PGPORT

##########################
# Initialise test server
##########################
initdb -U postgres &> /dev/null

if [ $? -ne 0 ]
then
	echo "Error initialising cluster in $PGDATA"
	exit -1
fi

cat >> $PGDATA/postgresql.conf <<EOF
port=$PGPORT
unix_socket_directory='/tmp'	#workaround for Debian/Ubuntu defaults
archive_mode=on
archive_command='rsync -z %p localhost:$ARCHIVE_DIR/%f'
EOF

if [ "${MAJOR_RELEASE/./}" -ge 90 ]
then
    echo "wal_level=archive" >> $PGDATA/postgresql.conf
fi

pg_ctl -D $PGDATA -l $MASTER_DIR/postgresql.log start

if [ $? -ne 0 ]
then
	echo "Error starting cluster in $PGDATA"
	exit -1
fi

cat <<EOF
barman test server with PostgreSQL $MAJOR_RELEASE.$MICRO_RELEASE installed

You can now connect as follows:

    psql -h /tmp -p $PGPORT -U postgres

Remember to stop the server after testing:

    pg_ctl -D $PGDATA stop

You can now simulate pgbench activity with:

    pgbench -iv -h /tmp -p $PGPORT -U postgres

EOF
