#!/bin/bash

IFS=$'\n'
k=$'\t'
delim='\\ '
sp=' '

trim()
{
	trimmed=$1
	trimmed=${trimmed%% }
	trimmed=${trimmed## }
	trimmed=${trimmed//$delim/$sp}
	echo $trimmed
}
for i in $(fdupes -r1 ./);
do
	h=${i//\.\//$k}
	IFS=$k
	count=0
	for j in $h;
	do
		if [ $count -eq 0 ] ; then
			actual=$(trim $j)
		else
			link=$(trim $j)
			echo rm "${link}"
			rm "${link}"
			echo ln -s $PWD/$actual $PWD/$link
			ln -s $PWD/$actual $PWD/$link
		fi
		let "count += 1"
	done
	IFS=$'\n'
done
