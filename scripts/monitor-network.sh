#!/bin/bash
# Realtime network throughput for single NIC. Helpful to eyeballing the throughput.

# Auto-detect the default network interface
INTERFACE=$(ip route show default | awk '{print $5; exit}')

# Get initial values
RX1=$(awk "/$INTERFACE/"'{print $2}' /proc/net/dev)
TX1=$(awk "/$INTERFACE/"'{print $10}' /proc/net/dev)

while true; do
    sleep 1

    RX2=$(awk "/$INTERFACE/"'{print $2}' /proc/net/dev)
    TX2=$(awk "/$INTERFACE/"'{print $10}' /proc/net/dev)

    RX_GBPS=$(echo "scale=3; ($RX2 - $RX1) * 8 / 1000000000" | bc)
    TX_GBPS=$(echo "scale=3; ($TX2 - $TX1) * 8 / 1000000000" | bc)

    printf "\rRX: %6.3f Gbps | TX: %6.3f Gbps" $RX_GBPS $TX_GBPS

    RX1=$RX2
    TX1=$TX2
done
