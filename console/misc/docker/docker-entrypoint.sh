#!/bin/sh

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Renders the nginx config from the environment, then runs nginx. Only the
# listed variables are substituted, so nginx's own `$uri` etc. survive.
set -eu

# nginx's `resolver` needs IP addresses, so take them from the pod's
# resolv.conf. IPv6 addresses need brackets.
NGINX_LOCAL_RESOLVERS=$(awk '$1 == "nameserver" { if ($2 ~ ":") printf "[%s] ", $2; else printf "%s ", $2 }' /etc/resolv.conf)
export NGINX_LOCAL_RESOLVERS

# shellcheck disable=SC2016 # the names are for envsubst, not the shell
envsubst '${MZ_ENDPOINT} ${MZ_NGINX_LISTENER_CONFIG} ${MZ_NGINX_METRICS_LISTENER_CONFIG} ${NGINX_LOCAL_RESOLVERS}' \
    < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf

exec "$@"
