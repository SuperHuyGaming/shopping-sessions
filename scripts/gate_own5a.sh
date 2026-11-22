#!/bin/bash
# Exactness gate: bash exp/own5a/scripts/gate_own5a.sh <image>   (caller must hold exp/.dockerlock)
# runs the image on shop5-dev2, shop5b-20m, shop5b-60m with scorer flags and compares against the refs.
export MSYS_NO_PATHCONV=1 PYTHONUTF8=1
IMG=$1
B="$(cd "$(dirname "$0")/../../.." && (pwd -W 2>/dev/null || pwd))"
cd "$B"
PAIRS=${PAIRS:-"shop5-dev2:eval/shopping/v5/dev-answer shop5b-20m:exp/out5-shop5b-20m-shop-ck shop5b-60m:exp/out5-shop5b-60m-shop-ck"}
for pair in $PAIRS; do
  DS=${pair%%:*}; REF=${pair#*:}
  OV=g5a-out-$IMG; SV=g5a-scr-$IMG
  docker volume create $OV >/dev/null; docker volume create $SV >/dev/null
  for v in $OV $SV; do docker run --rm -v $v:/v alpine sh -c 'rm -rf /v/* ; chown 1000:1000 /v'; done
  s=$(date +%s%N)
  docker run --rm --platform linux/amd64 --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 --cap-drop ALL \
    --security-opt no-new-privileges --pids-limit 4096 --cpus 8 --cpuset-cpus 0-7 --memory 12g --memory-swap 12g --user 1000:1000 \
    --mount type=volume,src=$DS,dst=/data,readonly,volume-subpath=d -v $OV:/output -v $SV:/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=12 $IMG >/dev/null 2>"$B/scratch/stderr-$IMG-$DS.log"; rc=$?
  e=$(date +%s%N)
  O="$B/scratch/g5a-$IMG-$DS"; rm -rf "$O"; mkdir -p "$O"
  cid=$(docker create -v $OV:/o alpine); docker cp $cid:/o/. "$O/" >/dev/null; docker rm $cid >/dev/null
  r=$(python eval/shopping/compare.py "$O" "$REF" 2>&1 | tail -1)
  echo "$DS rc=$rc wall=$(( (e - s) / 1000000 ))ms :: $r"
done
