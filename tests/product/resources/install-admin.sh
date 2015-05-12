#!/bin/bash
set -e

cp /mnt/presto-admin/prestoadmin-*.tar.bz2 /opt
cd /opt
tar -jxf prestoadmin-*.tar.bz2
cd prestoadmin
./install-prestoadmin.sh