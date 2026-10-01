#!/usr/bin/env bash
# Automatické nasadenie (Linux/macOS):  ./scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { cp .env.example .env; echo "Vytvorený .env z .env.example"; }
docker compose up -d --build --wait
docker compose ps
echo
echo "Uzol A: http://localhost:8001   Uzol B: http://localhost:8002   Uzol C: http://localhost:8003"
