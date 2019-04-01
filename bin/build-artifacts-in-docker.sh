#!/usr/bin/env bash

set -e
set -o pipefail
set -x

ROOT_DIR=$(readlink -f $(dirname $0)/..)

if [[ -z "${BASE_IMAGE_NAME}" ]]; then
  BASE_IMAGE_NAME="prestodb/centos6-presto-admin-tests"
fi

BASE_IMAGE_NAME=${BASE_IMAGE_NAME}-build

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
    pip install tox-travis==0.10
    # use explicit versions of dependent packages
    pip install pycparser==2.18
    pip install Babel==2.5.3
    pip install cffi==1.11.5
    pip install PyNaCl==1.2.1
    pip install idna==2.7
    pip install cryptography==2.1.1
    pip install -r requirements.txt
    export PYTHONPATH=${PYTHONPATH}:$(pwd)
    make dist dist-offline
EOF
