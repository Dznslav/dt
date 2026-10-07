#!/usr/bin/env bash
# Reálne odpojenie uzla od ostatných VM v Docker Swarm klastri (pre obhajobu).
#
# Spúšťa sa na VM, ktorú chceme odpojiť (napr. VM2 = uzol B):
#
#   ./scripts/swarm-partition.sh odpoj <IP VM1> <IP VM3>   # odreže tento uzol od A a C
#   ./scripts/swarm-partition.sh stav                       # ukáže, či je uzol odpojený
#   ./scripts/swarm-partition.sh pripoj                     # obnoví spojenie
#
# Pri ďalšom odpojení netreba IP zadávať znova – uložia sa do ~/.dt-peers.
#
# Ako to funguje: pravidlá sú vo vlastnom iptables reťazci DT_PARTITION, ktorý zahodí
# všetky pakety z/do zadaných IP (vrátane Swarm overlay siete). Prístup z ostatných
# adries (napr. z Windows hostiteľa do stavového panela) zostáva zachovaný, takže je
# vidieť, že odpojený uzol ďalej pracuje lokálne.

set -euo pipefail

CHAIN=DT_PARTITION
PEERS_FILE="${HOME}/.dt-peers"
GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

if [[ $EUID -ne 0 ]]; then
  exec sudo -E PEERS_FILE_OVERRIDE="$PEERS_FILE" "$0" "$@"
fi
PEERS_FILE="${PEERS_FILE_OVERRIDE:-$PEERS_FILE}"

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

chain_exists() { iptables -nL "$CHAIN" >/dev/null 2>&1; }

is_isolated() { chain_exists && [[ -n "$(iptables -S "$CHAIN" | grep -- '-j DROP' || true)" ]]; }

blocked_ips() { chain_exists && iptables -S "$CHAIN" | awk '/-s .* -j DROP/ {sub("/32","",$4); print $4}' | sort -u; }

ensure_chain() {
  chain_exists || iptables -N "$CHAIN"
  iptables -C INPUT  -j "$CHAIN" 2>/dev/null || iptables -I INPUT 1 -j "$CHAIN"
  iptables -C OUTPUT -j "$CHAIN" 2>/dev/null || iptables -I OUTPUT 1 -j "$CHAIN"
}

check_peer() {  # vráti "dostupný"/"nedostupný" pre IP (ping)
  if ping -c1 -W1 "$1" >/dev/null 2>&1; then echo "${GREEN}dostupný${RESET}"; else echo "${RED}nedostupný${RESET}"; fi
}

cmd_odpoj() {
  local ips=("$@")
  if [[ ${#ips[@]} -eq 0 && -s "$PEERS_FILE" ]]; then
    read -r -a ips < "$PEERS_FILE"
  fi
  [[ ${#ips[@]} -gt 0 ]] || { echo "${RED}Zadaj IP adresy ostatných VM:${RESET} $0 odpoj <IP VM1> <IP VM3>"; exit 1; }
  for ip in "${ips[@]}"; do
    [[ $ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "${RED}Neplatná IP adresa: $ip${RESET}"; exit 1; }
  done
  echo "${ips[*]}" > "$PEERS_FILE"

  ensure_chain
  iptables -F "$CHAIN"
  for ip in "${ips[@]}"; do
    iptables -A "$CHAIN" -s "$ip" -j DROP
    iptables -A "$CHAIN" -d "$ip" -j DROP
  done

  echo
  echo "${BOLD}${RED}>>> Uzol $(hostname) je ODPOJENÝ od ostatných uzlov klastra${RESET}"
  for ip in "${ips[@]}"; do echo "    $ip : $(check_peer "$ip")"; done
  echo "    Lokálna práca pokračuje – zmeny sa ukladajú do vlastnej DB a outboxu."
  echo
}

cmd_pripoj() {
  if chain_exists; then
    iptables -D INPUT  -j "$CHAIN" 2>/dev/null || true
    iptables -D OUTPUT -j "$CHAIN" 2>/dev/null || true
    iptables -F "$CHAIN"
    iptables -X "$CHAIN"
  fi
  echo
  echo "${BOLD}${GREEN}>>> Uzol $(hostname) je znova PRIPOJENÝ ku klastru${RESET}"
  if [[ -s "$PEERS_FILE" ]]; then
    read -r -a ips < "$PEERS_FILE"
    for ip in "${ips[@]}"; do echo "    $ip : $(check_peer "$ip")"; done
  fi
  echo "    Neodoslané operácie sa automaticky synchronizujú (do ~5 s)."
  echo
}

cmd_stav() {
  echo
  if is_isolated; then
    echo "${BOLD}${RED}Uzol $(hostname): ODPOJENÝ${RESET}"
    for ip in $(blocked_ips); do echo "    blokované: $ip : $(check_peer "$ip")"; done
  else
    echo "${BOLD}${GREEN}Uzol $(hostname): PRIPOJENÝ${RESET}"
    if [[ -s "$PEERS_FILE" ]]; then
      read -r -a ips < "$PEERS_FILE"
      for ip in "${ips[@]}"; do echo "    $ip : $(check_peer "$ip")"; done
    fi
  fi
  echo
}

case "${1:-}" in
  odpoj|disconnect) shift; cmd_odpoj "$@" ;;
  pripoj|connect)   cmd_pripoj ;;
  stav|status)      cmd_stav ;;
  *)                usage ;;
esac
