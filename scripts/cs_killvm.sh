#!/bin/sh

echo Sending destroy...
workspace -e $1 --destroy
#mv $1 ./eprs
#rm $1
#echo Destroy complete, epr moved to ./eprs
echo Destroy complete
