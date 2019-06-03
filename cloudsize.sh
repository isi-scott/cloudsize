#!/bin/bash

if [ -z "$1" ]
then
echo "You must supply a path as an argument"
exit 1
elif [ -e "$1" ]
then
searchdir=$1
else
echo "Please provide a valid path"
exit 1
fi

total=0

slist=($(find $searchdir -flags +ssmartlinked -ls | awk '{print $7}'))

for x in ${slist[*]}
do total=$(($total + $x))
done

echo $total


