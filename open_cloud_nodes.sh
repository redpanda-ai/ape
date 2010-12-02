#array of hosts to ssh into
array=( `ec2-describe-instances | grep 'ec2-' | sed -e 's:.*\(ec2-[^\t]\+\)\t.*:\1:g'` )

fg=( red orange magenta cyan purple green yellow blue )

#array=( localhost localhost localhost localhost localhost localhost localhost )

#secret key, so that there is no prompting
ssh_key=./keys/french_toast

#define the horizontal window size, and figure out other points
h_size="550"
h_reso=`xrandr | grep 'current' | sed -e 's:.*current \(\S\+\).*:\1:g'`
h_windows=$(($h_reso/$h_size))
h_pad=$((($h_reso-($h_windows*$h_size))/($h_windows+1)))

#define the vertical window, and figure out other points
v_size="400"
v_reso=`xrandr | grep 'current' | sed -e 's:.*current \S\+ x \(\S\+\),.*:\1:g'`
v_windows=$(($v_reso/$v_size))
v_pad=$((($v_reso-($v_windows*$v_size))/($v_windows+1)))

#count the number of array items
cnt=${#array[@]}
fg_count=${#fg[@]}

#create a terminal for each array item, nicely arranged
export node1=${array[0]}
for (( i = 0 ; i < cnt; i=i+1 ))
do
	h=$(($h_pad+i%$h_windows*($h_size+$h_pad)))
	v=$(($v_pad+i/$v_windows*($v_size+$v_pad)))
	#/usr/bin/xterm -bg black -fg ${fg[i%fg_count]} -fs 12 -geometry 80x24+$h+$v -e "ssh - $ssh_key root@${array[i]}; bash" &> /dev/null &
	/usr/bin/xterm -bg black -fg ${fg[i%fg_count]} -fs 12 -fn -*-fixed-medium-r-*-*-14-*-*-*-*-*-iso8859-* -geometry 80x24+$h+$v -e \
	"ssh -i $ssh_key root@${array[i]}; bash" &> /dev/null &

done

