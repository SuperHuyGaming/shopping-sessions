#!/bin/bash
# Interleaved timing: bash exp/own5a/scripts/time_own5a.sh <dataset> <runs> img1 img2 ...  (caller holds exp/.dockerlock)
export MSYS_NO_PATHCONV=1
DS=$1; N=$2; shift 2
for i in $(seq 1 $N); do
  for IMG in "$@"; do
    OV=t5a-out; SV=t5a-scr
    docker volume create $OV >/dev/null; docker volume create $SV >/dev/null
    docker run --rm -v $OV:/o -v $SV:/s alpine sh -c 'rm -rf /o/* /s/*; chown 1000:1000 /o /s'
    s=$(date +%s%N)
    docker run --rm --platform linux/amd64 --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL \
      --security-opt no-new-privileges --pids-limit 4096 --cpus 8 --cpuset-cpus 0-7 --memory 12g --memory-swap 12g --user 1000:1000 \
      --mount type=volume,src=$DS,dst=/data,readonly,volume-subpath=d -v $OV:/output -v $SV:/scratch \
      -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=12 $IMG >/dev/null 2>&1
    e=$(date +%s%N)
    echo "$DS $IMG $(( (e - s) / 1000000 ))"
  done
done
