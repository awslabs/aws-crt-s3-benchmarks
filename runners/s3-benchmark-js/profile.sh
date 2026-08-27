#!/bin/bash
# Run main.mjs with CPU profiling enabled for main thread + all worker threads.
# Profiles are saved to ./profiles/<timestamp>/ as .cpuprofile files (openable in Chrome DevTools).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
PROFILES_DIR="$SCRIPT_DIR/profiles/$TIMESTAMP"

mkdir -p "$PROFILES_DIR"

echo "Profiling enabled. Profiles will be saved to: $PROFILES_DIR"
echo "Running: node --cpu-prof --cpu-prof-dir=$PROFILES_DIR main.mjs $@"
echo ""

node \
  --cpu-prof \
  --cpu-prof-dir="$PROFILES_DIR" \
  --cpu-prof-interval=1000 \
  "$SCRIPT_DIR/main.mjs" "$@"

echo ""
echo "Profiles saved to: $PROFILES_DIR"
ls -lh "$PROFILES_DIR"/*.cpuprofile 2>/dev/null | wc -l | xargs -I{} echo "{} profile(s) generated"
echo "Open in Chrome DevTools: chrome://inspect -> Performance tab -> Load profile"
