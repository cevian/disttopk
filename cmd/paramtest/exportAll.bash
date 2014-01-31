#!/bin/bash
# make sure you always put $f in double quotes to avoid any nasty surprises i.e. "$f"
first=true
for f in $@
do
    #echo "Processing $f file... $first"
    if [ "$first" == true ]; then
      bash getExport.bash "$f"
      first=false
    else
      bash getExport.bash "$f" | grep -v Zip
    fi
done
