#!/bin/bash

INPUT_DIR=/home/fg/aw/data/input
fns=""

find $INPUT_DIR -type f -name "*.csv" |(while read fn
do
   #echo $fn
   dir=$(dirname $fn)
   donefn=${fn}.done
   if [ -f $donefn ]; then
      echo "$donefn exists, add $fn for processing."
      fns="$fns ${fn}"
   else
      echo "$donefn does not exists, ignore."
   fi
done

echo $fns)

if [ -z $fns ]; then
   echo "File list is empty, nothing to process"
fi

