#!/bin/bash

IN_CURR=$1
IN_NEW=$2
#U_BASE=$3 #dir for update tmp files & results
CAPTURES=$4
U_BASE=/user/yasemin/delis/3M-updates

MAP_CURR=$IN_CURR/map
MAP_NEW=$IN_NEW/map

#merge maps
./wg-bin/updates/mergemaps.sh $MAP_CURR $MAP_NEW $CAPTURES

#update layers
./wg-bin/updates/update-layers-quick.sh $IN_CURR $U_BASE/tts-and-map/tt-current
./wg-bin/updates/update-layers-quick.sh $IN_NEW $U_BASE/tts-and-map/tt-new

#merge updated layers
./wg-bin/updates/merge-updated-layers.sh $IN_CURR $IN_NEW $U_BASE/3M-merged-layers



