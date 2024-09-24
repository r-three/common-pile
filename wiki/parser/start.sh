#!/usr/bin/env bash

NUMSERVERS=${1:-16}

function port {
    local id=${1}
    if [[ ${id} -ge 10 ]]; then
        echo "50${id}"
    else
        echo "500${id}"
    fi
}

function launch {
    local id=${1}
    node --max-old-space-size=65536 --max-semi-space-size=16384 parser.js --port $(port ${id}) --timeout 180 --maxworkers 1 >> ./logs/worker${id}.log 2>&1 &
}

function ping {
    local id=${1}
    echo $(curl -I -X GET localhost:$(port ${id})/health 2> /dev/null | head -n 1 | cut -d$" " -f2)
}

mkdir -p logs

while true; do
  for i in $(seq 1 $NUMSERVERS); do
    if [[ $(ping ${i}) -ne "200" ]]; then
      echo "Worker ${i} not running, starting."
      launch ${i}
    fi
  done
  sleep 5
done
