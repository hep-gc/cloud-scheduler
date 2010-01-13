#!/bin/sh
date
echo Simple wait script for job testing
echo -

echo Machine name and address
echo Hostname: `hostname`
hostname --long
hostname -i
echo -

echo User: `whoami`
echo -

echo Memory
free -m
echo -

echo CPU
echo Total cores: `cat /proc/cpuinfo | grep processor | wc -l`
echo Specifics
cat /proc/cpuinfo | grep 'cpu cores'
cat /proc/cpuinfo | grep 'processor'
cat /proc/cpuinfo | grep 'model name'
echo -

echo Disk
df -h
echo -

echo VM Type as given in condor_config: `cat $CONDOR_CONFIG | grep ^VMType`
echo -

echo Registering with Condor pool: `cat /opt/condor/central.manager`
echo -

echo Wait for $1 seconds
sleep $1
echo -

echo Job finished at `date`
