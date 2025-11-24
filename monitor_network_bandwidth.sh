#!/bin/bash

INTERFACE="ens5"

# Get initial values
RX1=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $2}')
TX1=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $10}')

while true; do
    sleep 1

    # Get new values
    RX2=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $2}')
    TX2=$(cat /proc/net/dev | grep $INTERFACE | awk '{print $10}')

    # Calculate speed in Gbps
    RX_GBPS=$(echo "scale=3; ($RX2 - $RX1) * 8 / 1000000000" | bc)
    TX_GBPS=$(echo "scale=3; ($TX2 - $TX1) * 8 / 1000000000" | bc)

    # Print on same line (carriage return)
    printf "\rRX: %6.3f Gbps | TX: %6.3f Gbps" $RX_GBPS $TX_GBPS

    # Update for next iteration
    RX1=$RX2
    TX1=$TX2
done