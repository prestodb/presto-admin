#!/usr/bin/env bash

set -e
set -o pipefail
set -x

ROOT_DIR=$(readlink -f $(dirname $0)/..)

if [[ -z "${BASE_IMAGE_NAME}" ]]; then
  BASE_IMAGE_NAME="teradatalabs/centos6-ssh-oj8"
fi

if [[ -z "${BASE_IMAGE_TAG}" ]]; then
  BASE_IMAGE_TAG=$(cat ${ROOT_DIR}/base-images-tag.json | python -c 'import sys, json; print json.load(sys.stdin)["base_images_tag"]')
fi

echo Building presto-admin-artifacts in container ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}

CONTAINER_NAME="presto-admin-build-$(date '+%s')"
CONTAINER_DIR="/mnt/presto-admin"

docker run --name ${CONTAINER_NAME} -v ${ROOT_DIR}:${CONTAINER_DIR} --rm -i ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG} \
  env CONTAINER_DIR="${CONTAINER_DIR}" bash <<"EOF"
    cd ${CONTAINER_DIR}
    pip install --upgrade pip==9.0.1
    pip install -r requirements.txt
    pip install tox-travis
    export PYTHONPATH=${PYTHONPATH}:$(pwd)
    make dist dist-offline
EOF

