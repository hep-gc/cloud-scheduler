#!/bin/bash

# A simple script to clean log, link, and temp files created by the cloud
# scheduler.
# Usage: ./cs_clean.sh [-i : confirm all file deletions]

rm $1 *.pyc
rm $1 tmp_*
rm $1 cloudscheduler/*.pyc
mv *.epr ./eprs

