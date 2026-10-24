#!/usr/bin/env bash
set -euo pipefail
shopt -s nocasematch

violations=()
while IFS= read -r -d '' path; do
  case "$path" in
    data/real/*)
      violations+=("$path (real data directory)")
      ;;
    *.cowrie.json)
      if [[ "$path" != data/samples/* ]]; then
        violations+=("$path (Cowrie log outside synthetic samples)")
      fi
      ;;
    eval/results/*)
      if [[ "$path" != eval/results/latest.md ]]; then
        violations+=("$path (unapproved generated evaluation output)")
      fi
      ;;
  esac

  if [[ "$path" == *.csv ]] && [[ "$path" == *cicids* || "$path" == *cic-ids* || "$path" == *workinghours* || "$path" == *pcap_iscx* ]]; then
    violations+=("$path (CIC-IDS dataset file)")
  fi
done < <(git ls-files -z)

if ((${#violations[@]})); then
  printf 'Tracked sensitive/generated data is forbidden:\n' >&2
  printf ' - %s\n' "${violations[@]}" >&2
  exit 1
fi

echo "Tracked-file data safety check passed."
