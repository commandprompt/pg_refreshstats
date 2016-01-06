#!/bin/bash
#########################################################################################################
# Copyright (c) 2016 COMMAND PROMPT, INC.
#
# Name: pg_refreshstats.sh
#
# Description:
# This script executes vacuum analyze or just analyze commands based on input parameters
#
# Inputs: all fields are optional except database and type.
# -h <hostname or IP address> -d <database> -n <schema> -p <PORT> -t <type> -u <db user> 
# -l <load threshold> -w <max rows> -c [vacuum analyze] -r [dry run] -v [verbose output]
# 
# TYPE values:
#    * EXTENSIVE (all user tables in the database or schema will be refreshed)
#    * SMART (only tables that need to be refreshed will be refreshed)
# 
# Examples:
# -- vacuum analyze for all user tables in the database but only if load is less than 20% and rows < 1 mil
# ./pg_refreshstats.sh -h localhost -d test -p 5433 -t extensive -u postgres -l 20 -w 1000000 -c -v
# 
# -- smart analyze for all user tables in specific schema, but only if load is less than 40% and rows < 1 mil
# ./pg_refreshstats.sh -h localhost -d test -n public -p 5433 -t smart -u postgres -l 40 -w 1000000 -v
# 
# Assumptions:
# 1. db user defaults to postgres if not provided as parameter.
# 2. Max rows defaults to 10 million if not provided as parameter 
# 3. Password must be in local .pgpass file
# 4. psql must be in the user's path
# 5. action defaults to analyze unless option, -c, is specified
# 6. Load detection assumes that you are running this script from the database host.
# 7. SMART type will only consider tables whose pg_class.reltuples value is greater than zero. 
#    This value can be zero even if a few rows are in the table, because pg_class.reltuples is also a close estimate.
#
# SMART TYPE dictates a filter algorithm to determine what tables will qualify for the stats refresh.
# 1. Refresh tables with no recent analyze or autovacuum_analyze in the last 30 days.
# 2. Refresh tables where pg_stat_user_tables.n_live_tup is less than half of pg_class.reltuples
# 
# Tables with over MAXROWS rows are not refreshed and are output in file, /tmp/PROGRAMPID_refreshstats_deferred.sql
# 
# # TODOS:
#    1.  None at this time
#
# History
# 2016-01-04 Original Coding    The Author: Michael Vitale
# 2016-01-05 Enhancements/fixes Modifier:   Michael Vitale
#########################################################################################################
#PGBIN="/usr/pgsql-9.4/bin"
VERSION=1.0
PROG="refreshstats"
MYPID=`echo $$`
WORKDIR=/tmp
WORKFILE=$WORKDIR/${MYPID}_refreshstats.sql
WORKFILE_DEFERRED=$WORKDIR/${MYPID}_refreshstats_deferred.sql

usage()
{
cat << EOF
usage: $0 options

pg_refreshstats Version ${VERSION}.  This script refreshes user table statistics using vacuum analyze or just plain analyze.

OPTIONS:
   -h      server name or Ip address, optional
   -d      database name, required
   -p      port, optional
   -u      db user, optional
   -n      schema name (optional, excluded implies database-wide refresh)   
   -t      type  (EXTENSIVE or SMART)
   -l      load threshold (optional) 
   -w      max rows, defaults to 10 million, optional
   -c      vacuum analyze, instead of default, analyze (optional)
   -r      dry run, optional
   -v      Verbose, optional
EOF
}

###############
# ENTRY POINT #
###############
SERVER=
DATABASE=
SCHEMA=
USER=
PORT=
TYPE=
VACUUM_ANALYZE=0
DRYRUN=0
LOWLOAD=-1
MAXROWS=10000000
VERBOSE=0
#while getopts "hs:d:n:p:t:l:w:u:crv" OPTION
while getopts "h:d:n:p:t:l:w:u:crv" OPTION
do
     case $OPTION in
         h)
             SERVER=$OPTARG
             ;;             
         d)
             DATABASE=$OPTARG
             ;;
         n)
             SCHEMA=$OPTARG
             ;;
         p)
             PORT=$OPTARG
             ;;
         t)
             TYPE=$OPTARG
             ;;             
         u)
             USER=$OPTARG
             ;;                          
         l)
             LOWLOAD=$OPTARG
             ;;             
         w)
             MAXROWS=$OPTARG
             ;;             
         c)
             VACUUM_ANALYZE=1
             ;;                                       
         r)
             DRYRUN=1
             ;;                          
         v)
             VERBOSE=1
             ;;
         ?)
             usage
             exit 1
             ;;
     esac
done

if [[ -z $DATABASE ]] || [[ -z $TYPE ]]
then
     usage
     exit 1
fi
echo "INFO  `date`: *** ${PROG} started. PID=$MYPID SERVER=$SERVER DATABASE=$DATABASE SCHEMA=$SCHEMA PORT=$PORT USER=$USER TYPE=$TYPE LOWLOAD=$LOWLOAD MAXROWS=$MAXROWS VACUUM_ANALYZE=$VACUUM_ANALYZE DRYRUN=$DRYRUN VERBOSE=$VERBOSE"

TODAY=`date +"%Y_%m_%d"`
exec 2>&1

TYPEU=`echo $TYPE | tr [a-z] [A-Z]`
# setup connect clause
if [ -z $SERVER ]; then
    SERVER_CLAUSE=" "
else
    SERVER_CLAUSE=" -h ${SERVER} "
fi

if [ -z $USER ]; then
    USER_CLAUSE=" "
else
    USER_CLAUSE=" -U ${USER} "
fi

if [ -z $PORT ]; then
    PORT_CLAUSE=" "
else
    PORT_CLAUSE=" -p ${PORT} "
fi

###################
### VALIDATIONS ###
###################
RC1=`psql ${SERVER_CLAUSE} ${USER_CLAUSE} ${PORT_CLAUSE} -t -d $DATABASE -w -c "select version()"`
RC2=$?
if [[ ${VERBOSE} = 1 ]] ; then
    #echo "DEBUG `date`: *** Source DB: RC2=$RC2  RC1=$RC1"
    :
fi

if [[ ${RC2} -ne 0 ]] ; then
    echo "ERROR `date`: *** Unable to connect to Database, RC2=$RC2"
    exit 1
fi

if [ "$TYPEU" = "EXTENSIVE" ] || [ "$TYPEU" = "SMART" ]; then    
    # nothing
    :
else
    echo "ERROR `date`: *** Invalid type: $TYPE    Valid values are: EXTENSIVE or SMART"
    exit 1
fi

# delete any previous workload files
RC1=`rm -f ${WORKDIR}/*_refreshstats*.sql`
RC2=$?
if [[ ${RC2} -ne 0 ]] ; then
    echo "WARN  `date`: *** Unable to delete old workfile(s). RC2=$RC2"
fi            

if [[ $VACUUM_ANALYZE -eq 0 ]]; then
    STATM="ANALYZE"
else
    STATM="VACUUM ANALYZE"
fi

if [ -z $SCHEMA ]; then
    SCHEMA_CLAUSE=" "
else
    SCHEMA_CLAUSE=" and n.nspname = '${SCHEMA}' "
fi

#####################
### CONSIDER LOAD ###
#####################

# first see if user wants to only execute this during low load times. Use 15 minute interval for evaluating
if [[ ${LOWLOAD} -ne -1 ]] ; then
    CPUS=`cat /proc/cpuinfo | grep processor | wc -l`
    LOAD15=`uptime | grep -ohe 'load average[s:][: ].*' | awk '{ print $5 }'`
    LOAD=`echo "$LOAD15 $CPUS" | awk '{printf "%.2f \n", $1/$2*100}'`
    # round it
    LOADR=`echo ${LOAD} | awk '{printf("%d\n",$1 + 0.5)}'`
   
    if [[ ${VERBOSE} = 1 ]] ; then
        echo "INFO  `date`: *** LOAD15=$LOAD15 CPUS=$CPUS LOAD=$LOADR."
    fi

    if [ "$LOADR" -lt "$LOWLOAD" ] ; then    
        #echo "INFO  `date`: *** LOAD15=$LOAD15 CPUS=$CPUS LOAD=$LOAD."
        echo "INFO  `date`: *** Low Load: ${LOADR}% is less than Load Threshold, ${LOWLOAD}%.  Proceeding with refreshstats commands..."
    else
        echo "INFO  `date`: *** High Load: ${LOADR}% is greater than Load Threshold, ${LOWLOAD}%.  Aborting refreshstats commands..."    
        exit 1
    fi
fi

###################
### DO THE WORK ###
###################

# Show user what tables will not be refreshed because their size exceeds Max rows.
SQL="select n.nspname || '.' || c.relname from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname ${SCHEMA_CLAUSE} and n.nspname not in ('information_schema','pg_catalog') and c.reltuples > ${MAXROWS} order by n.nspname, c.relname"        
RC1=`psql ${SERVER_CLAUSE} ${USER_CLAUSE} ${PORT_CLAUSE} -d ${DATABASE} -t -c "${SQL}" > ${WORKFILE_DEFERRED}`
RC2=$?
if [[ ${RC2} -ne 0 ]] ; then
    echo "ERROR `date`: *** Unable to extract refresh commands from database: $HOST $DATABASE RC2=$RC2"
    exit 1
fi        
RC1=`grep -c ^ ${WORKFILE_DEFERRED}`
COUNT=`expr $RC1 - 1`
if [[ ${COUNT} -gt 0 ]] ; then
    echo "INFO  `date`: *** $COUNT table(s) are deferred since rowcounts > ${MAXROWS}. Details --> $WORKFILE_DEFERRED"    
fi

if [ "$TYPEU" = "EXTENSIVE" ]; then
    if [ -z $SCHEMA ]; then
        echo "INFO  `date`: *** Extensive database refresh in progress...."            
    else
        echo "INFO  `date`: *** Extensive schema refresh in progress...."    
    fi

    SQL="select '${STATM} VERBOSE ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname ${SCHEMA_CLAUSE} and n.nspname not in ('information_schema','pg_catalog') and c.reltuples between 1 and ${MAXROWS} order by n.nspname, c.relname"  
    if [[ $VERBOSE -eq 1 ]]; then
        echo "DEBUG `date`: *** SQL--> $SQL"
    fi
    
    RC1=`psql ${SERVER_CLAUSE} ${USER_CLAUSE} ${PORT_CLAUSE} -d ${DATABASE} -t -c "${SQL}" > ${WORKFILE}`
    RC2=$?
    if [[ ${RC2} -ne 0 ]] ; then
        echo "ERROR `date`: *** Unable to extract refresh commands from database: $HOST $DATABASE RC2=$RC2"
        exit 1
    fi            
    RC1=`grep -c ^ ${WORKFILE}`
    COUNT=`expr $RC1 - 1`
    if [[ ${COUNT} -eq 0 ]] ; then
        echo "INFO  `date`: *** No tables will be analyzed."
    else
        echo "INFO  `date`: *** $COUNT table(s) will be analyzed. Details --> $WORKFILE"    
        if [ "$DRYRUN" -eq 1 ] ; then        
           echo "INFO  `date`: *** Dry Run ended."            
           exit 0
        fi
        # now execute the analyze commands in the file
        echo ""
        RC1=`psql ${SERVER_CLAUSE} ${USER_CLAUSE} ${PORT_CLAUSE} -d ${DATABASE} < ${WORKFILE}`
        RC2=$?
        echo ""
        if [[ ${RC2} -ne 0 ]] ; then
            echo "ERROR `date`: *** Unable to execute refresh commands: $HOST $DATABASE RC2=$RC2"
            exit 1
        fi                
    fi    
    
elif [ "$TYPEU" = "SMART" ]; then    
    if [ -z $SCHEMA ]; then
    	echo "INFO  `date`: *** Smart database refresh in progress..."
    else
        echo "INFO  `date`: *** Smart schema refresh in progress..."                
    fi
    
    SQL="select '${STATM} ' || n.nspname || '.' || c.relname || ';' as ddl from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where t.schemaname = n.nspname and t.tablename = c.relname and c.relname = u.relname ${SCHEMA_CLAUSE} and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples between 1 and ${MAXROWS} and round((u.n_live_tup / c.reltuples) * 100) < 50)) OR ((last_analyze is null and last_autoanalyze is null) or (now()::date  - last_analyze::date > 30 OR now()::date - last_autoanalyze::date > 30))) order by n.nspname, c.relname"
    if [[ $VERBOSE -eq 1 ]]; then
        echo "DEBUG `date`: *** SQL--> $SQL"
    fi
    RC1=`psql ${SERVER_CLAUSE} ${USER_CLAUSE} ${PORT_CLAUSE} -d ${DATABASE} -t -c "${SQL}" > ${WORKFILE}`
    RC2=$?
    if [[ ${RC2} -ne 0 ]] ; then
        echo "ERROR `date`: *** Unable to extract refresh commands from database: $HOST $DATABASE RC2=$RC2"
        exit 1
    fi        
    RC1=`grep -c ^ ${WORKFILE}`
    COUNT=`expr $RC1 - 1`
    if [[ ${COUNT} -eq 0 ]] ; then
        echo "INFO  `date`: *** No tables qualify to be analyzed."
    else
        echo "INFO  `date`: *** $COUNT table(s) will be analyzed. Details --> $WORKFILE"    
        if [ "$DRYRUN" -eq 1 ] ; then        
           echo "INFO  `date`: *** Dry Run ended."            
           exit 0
        fi        
        # now execute the analyze commands in the file
        echo ""
        RC1=`psql ${SERVER_CLAUSE} ${USER_CLAUSE} ${PORT_CLAUSE} -d ${DATABASE} < ${WORKFILE}`
        RC2=$?
        echo ""
        if [[ ${RC2} -ne 0 ]] ; then
            echo "ERROR `date`: *** Unable to execute refresh commands: $HOST $DATABASE RC2=$RC2"
            exit 1
        fi                
    fi
fi

echo "INFO  `date`: *** ${PROG} ended successfully."
exit 0


