#!/bin/bash
source ~/.bashrc

./unichem2index -v

echo "LE TAIL"
tail -f /dev/null