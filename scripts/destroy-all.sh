#!/bin/bash

echo "Running workspace destroy on all passed in files..."

shopt -s nullglob
for f in $@; do
    echo 
    echo "$f :"
    workspace -e $f --destroy
done

echo "All available VMs destroyed."

