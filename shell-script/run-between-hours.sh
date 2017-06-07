#!/bin/bash
# "20161112 11:00:00" "20161212 11:00:00"


current=$1
end=$(date -d "$2 1 hour" +"%Y%m%d %H:%M:%S")

echo $start
echo $end

while [ "$current" != "$end" ]; do

  currentHour=$(date -d "$current" +"%Y%m%d%H")
  nextHour=$(date -d "$current 1 hour" +"%Y%m%d%H")

  //run job command currentHour nexHour

  current=$(date -d "$current 1 hour" +"%Y%m%d %H:%M:%S")
done