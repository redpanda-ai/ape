#!/bin/bash

IFS=$'\n'
k=$'\t'

trim()
{
        trimmed=$1
        trimmed=${trimmed%% }
        trimmed=${trimmed## }

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
                        source=$j
                else
                        link=$(trim $j)
                        actual=$(trim $source)
                        rm ${link}
                        ln -s $PWD/$actual $PWD/$link
                fi
                let "count += 1"
        done
        IFS=$'\n'
done

