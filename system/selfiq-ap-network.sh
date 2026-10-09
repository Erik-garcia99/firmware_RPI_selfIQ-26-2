#!/bin/sh
set -eu
ip link set wlan0 up
ip addr replace 192.158.50.1/24 dev wlan0
