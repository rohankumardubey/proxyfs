#!/bin/bash

set -e

ENV_NAME=$1
SS_PACKAGES=$2
GOLANG_VERSION=$3

if [ -z "$ENV_NAME" ]; then
  echo "usage: $0 <env-name>"
  exit 1
fi

if [ -n "$SS_PACKAGES" ] && [ "$ENV_NAME" == "runway" ] && [ "$SS_PACKAGES" == "swiftstack" ]; then
    ENV_NAME="runway_ss"
fi

if [ -z "$GOLANG_VERSION" ]; then
  GOLANG_VERSION="current"
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ansible-playbook -i "localhost," -c local -e env=$ENV_NAME -e env_arg="$1" -e ss_packages_arg="$2" -e golang_version="$GOLANG_VERSION" "$SCRIPT_DIR"/tasks/main.yml
chef-solo -c "$SCRIPT_DIR"/chef_files/$ENV_NAME.cfg
