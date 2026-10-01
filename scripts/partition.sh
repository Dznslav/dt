#!/usr/bin/env bash
# ./scripts/partition.sh disconnect b   |   ./scripts/partition.sh connect b
set -euo pipefail
action=$1; node=$2; c="dt-api-$node"
if [ "$action" = disconnect ]; then docker network disconnect dt_cluster "$c"
else docker network connect --alias "api-$node" dt_cluster "$c"; fi
docker network inspect dt_cluster --format "Siet dt_cluster: {{range .Containers}}{{.Name}} {{end}}"
