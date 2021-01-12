# redis-dumpload
====================

dump redis in json and and reload into redis using python

1:dump redis data to file

dump from host db0

>  python redis-dump.py -H 127.0.0.1 -p 6379 -w passwd -d 0 -o road.txt

key filter with road_*:

>  python redis-dump.py -H 127.0.0.1 -p 6379 -w passwd -d 0 -k road_* -o road.txt



2:load data into redis db0 from dumped file

> python redis-load.py -H 127.0.0.1 -p 6379 -w passwd -d 0 road.txt
