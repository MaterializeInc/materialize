#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# set-sso-entity-id.sh: give a Materialize organization its own SAML SP Entity ID.
#
# Run `./set-sso-entity-id.sh --help` for end-user documentation; the `usage`
# function below is the single source of it.
#
# Background for maintainers: Frontegg applies one SP Entity ID to every
# account in the environment unless an account's SSO configuration sets
# `spEntityId`. That field is not exposed in the admin portal, but an
# Organization Admin's own token may PATCH it through Frontegg's self-service
# API (`fe.secure.write.samlConfiguration`), so this script needs no Materialize
# staff credentials. Once set, Frontegg uses the override for the SAML Audience
# check, the Issuer of SP-initiated logins, and the account's SP metadata.
#
# The script is deliberately self-contained (bash 3.2+, curl, jq) because
# customers download it as a single file from the docs site (it lives in
# doc/user/static/scripts/ and is documented in
# doc/user/content/security/cloud/users-service-accounts/sso.md). The undocumented `--staging` flag points
# it at Materialize's staging environment for internal testing.

set -euo pipefail

app_hostname=cloud.materialize.com
config_id=
assume_yes=false
command=

usage() {
    local script
    script=$(basename "$0")
    cat <<EOF
Give your Materialize organization its own SAML SP Entity ID.

WHY YOU MIGHT NEED THIS
  By default, every Materialize organization uses the same SAML SP Entity ID.
  If you connect several Materialize organizations to one identity provider
  (for example, one Microsoft Entra ID tenant with a separate enterprise app
  per organization), the identity provider requires each app to have a
  different Identifier. This script gives the organization a unique one:

      https://$app_hostname/auth/saml/metadata/<your-organization-id>

  If you have only one Materialize organization, you don't need this.

BEFORE YOU START
  - Install curl and jq (macOS: brew install jq; Debian/Ubuntu: apt install jq).
  - Have a personal app password created by an Organization Admin of the
    organization you want to change. Create one in the Materialize console
    under your account's App Passwords page. The script works out which
    organization to change from the app password. ("preview" works with any
    member's app password.)
  - To use "set" or "reset", the organization must already have SAML SSO set
    up (Admin portal -> Single Sign On). The script changes that
    configuration; it can't create one.

USAGE
  $script preview
  $script list
  $script set   [--config-id ID] [--yes]
  $script reset [--config-id ID] [--yes]

COMMANDS
  preview Show the organization-specific Entity ID this script would set,
          so you can enter it in your identity provider ahead of time.
          Changes nothing and doesn't need SSO to be set up yet.
  list    Show the organization's SSO configurations and the Entity ID each
          one uses. Changes nothing.
  set     Switch the SSO configuration to the organization-specific Entity ID.
  reset   Switch back to the shared default Entity ID.

OPTIONS
  --config-id ID   Which SSO configuration to change. Only needed when the
                   organization has more than one; get the ID from "list".
  --yes            Don't ask for confirmation before making the change.
  -h, --help       Show this help.

  The app password is read from the MZ_APP_PASSWORD environment variable. If
  it isn't set, the script asks for it without echoing it to the screen.

STEP BY STEP
  1. See the new Entity ID and what's configured today:
         $script preview
         $script list

  2. Apply the organization-specific Entity ID:
         $script set
     The script prints the new value and asks you to confirm.

  3. Straight away, set the same value in your identity provider:
       Microsoft Entra ID: Enterprise application -> Single sign-on ->
                           Basic SAML Configuration -> Identifier (Entity ID)
       Okta:               Application -> General -> SAML Settings ->
                           Audience URI (SP Entity ID)
     Leave the Reply URL / Single sign-on URL (ACS) unchanged.

  4. Sign in to Materialize through SSO to confirm it works.

  IMPORTANT: between steps 2 and 3, SSO sign-in for this organization fails,
  because Materialize and your identity provider disagree on the Entity ID.
  Do both steps together. If something goes wrong, run "$script reset"
  and restore the old value in your identity provider.

  For a brand-new SSO setup, run "$script preview" and enter that value
  in your identity provider first, then create the SSO configuration in the
  admin portal, then run "$script set".

DOCUMENTATION
  https://materialize.com/docs/security/cloud/users-service-accounts/sso/#use-a-unique-entity-id-for-each-organization

EXAMPLES
  export MZ_APP_PASSWORD=mzp_...
  $script preview
  $script list
  $script set
  $script set --config-id 2a15f3bc-347a-44f7-8ad8-401be8d545e5
  $script reset --yes
EOF
    exit "${1:-0}"
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        preview|list|set|reset) command=$1 ;;
        --config-id) config_id=${2:-}; [[ -n "$config_id" ]] || die "--config-id needs a value"; shift ;;
        --yes) assume_yes=true ;;
        --staging) app_hostname=staging.cloud.materialize.com ;;
        -h|--help|help) usage 0 ;;
        *) die "unknown argument: $1. Run '$(basename "$0") --help' for usage." ;;
    esac
    shift
done
if [[ -z "$command" ]]; then
    echo "Usage: $(basename "$0") preview | list | set | reset [--config-id ID] [--yes]" >&2
    echo "Run '$(basename "$0") --help' for a full explanation." >&2
    exit 1
fi

for tool in curl jq; do
    command -v "$tool" >/dev/null \
        || die "$tool is not installed. Install it (macOS: brew install $tool) and try again."
done

api="https://admin.$app_hostname"
shared_entity_id="https://$app_hostname/auth/saml/metadata"

if [[ -z "${MZ_APP_PASSWORD:-}" ]]; then
    read -r -s -p "Materialize app password (input hidden): " MZ_APP_PASSWORD || true
    echo >&2
fi
[[ -n "${MZ_APP_PASSWORD:-}" ]] \
    || die "no app password given. Set MZ_APP_PASSWORD or enter it when asked."

# An app password is "mzp_" followed by the client ID and secret, each 32 hex
# characters without dashes.
raw=${MZ_APP_PASSWORD#mzp_}
[[ "$raw" =~ ^[0-9a-fA-F]{64}$ ]] \
    || die "that doesn't look like a Materialize app password. It should start with 'mzp_'."
to_uuid() {
    echo "${1:0:8}-${1:8:4}-${1:12:4}-${1:16:4}-${1:20:12}"
}

token=$(
    jq -n --arg c "$(to_uuid "${raw:0:32}")" --arg s "$(to_uuid "${raw:32:32}")" \
        '{clientId: $c, secret: $s}' \
    | curl -sS -H 'Content-Type: application/json' --data @- \
        "$api/identity/resources/auth/v1/api-token" \
    | jq -r '.accessToken // empty'
)
[[ -n "$token" ]] \
    || die "could not sign in with that app password. Check that it was copied in full and hasn't been deleted."

# Read the organization ID from the token. JWT segments are unpadded base64url.
payload=$(cut -d. -f2 <<< "$token" | tr '_-' '/+')
while (( ${#payload} % 4 )); do payload+="="; done
org_id=$(base64 --decode <<< "$payload" 2>/dev/null | jq -r '.tenantId // empty')
[[ -n "$org_id" ]] || die "could not determine the organization from the app password"

api_call() {
    local method=$1 path=$2 body=${3:-}
    local args=(-sS -X "$method" -H "Authorization: Bearer $token" -w '\n%{http_code}')
    [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' --data "$body")
    local response status
    response=$(curl "${args[@]}" "$api$path")
    status=${response##*$'\n'}
    response=${response%$'\n'*}
    if [[ "$status" != 2* ]]; then
        echo "error: $method $path returned HTTP $status" >&2
        case "$status" in
            401|403)
                echo "The app password must be a personal app password created by an" >&2
                echo "Organization Admin of this organization." >&2
                ;;
            *)
                echo "$response" >&2
                ;;
        esac
        exit 1
    fi
    echo "$response"
}

# Renders one SSO configuration for display. This is a jq program, not shell.
# shellcheck disable=SC2016
describe='"  Configuration ID: \(.id)\n    Type:      \(.type | ascii_upcase)\n    Enabled:   \(.enabled)\n    Domains:   \([.domains[]?.domain] | join(", "))\n    Entity ID: \(if (.spEntityId // "") == "" then "\($shared) (shared default)" else .spEntityId end)\n"'

org_entity_id="$shared_entity_id/$org_id"

if [[ "$command" == preview ]]; then
    cat <<EOF
Organization: $org_id

Organization-specific Entity ID:
  $org_entity_id

Enter this value in your identity provider:
  Microsoft Entra ID: Basic SAML Configuration -> Identifier (Entity ID)
  Okta:               SAML Settings -> Audience URI (SP Entity ID)

Nothing has changed in Materialize. If this organization already uses SSO,
change your identity provider at the same moment you run
'$(basename "$0") set', or sign-in will fail in between.
EOF
    exit 0
fi

configs=$(api_call GET /team/resources/sso/v1/configurations)

if [[ "$command" == list ]]; then
    echo "Organization: $org_id"
    echo
    if [[ $(jq length <<< "$configs") -eq 0 ]]; then
        echo "  No SSO configurations yet. Set up SAML SSO in the admin portal first."
    else
        jq -r --arg shared "$shared_entity_id" ".[] | $describe" <<< "$configs"
        echo "Organization-specific Entity ID: $org_entity_id"
        echo "To switch to it, run: $(basename "$0") set"
    fi
    exit 0
fi

count=$(jq length <<< "$configs")
if [[ "$count" -eq 0 ]]; then
    die "organization $org_id has no SSO configuration yet. Set up SAML SSO in the admin portal first, then run this again."
fi
if [[ -n "$config_id" ]]; then
    config=$(jq -c --arg id "$config_id" '.[] | select(.id == $id)' <<< "$configs")
    [[ -n "$config" ]] \
        || die "no SSO configuration with ID $config_id. Run '$(basename "$0") list' to see the IDs."
elif [[ "$count" -gt 1 ]]; then
    echo "Organization $org_id has $count SSO configurations. Choose one by adding" >&2
    echo "--config-id <Configuration ID> to the command:" >&2
    echo >&2
    jq -r --arg shared "$shared_entity_id" ".[] | $describe" <<< "$configs" >&2
    exit 1
else
    config=$(jq -c '.[0]' <<< "$configs")
fi
config_id=$(jq -r .id <<< "$config")

if [[ "$command" == set ]]; then
    entity_id=$org_entity_id
else
    entity_id=""
fi
current=$(jq -r '.spEntityId // ""' <<< "$config")
# The value the identity provider must use once this change is made.
idp_value=${entity_id:-$shared_entity_id}

echo "Organization:      $org_id"
echo "Configuration ID:  $config_id"
echo "Current Entity ID: ${current:-$shared_entity_id (shared default)}"
if [[ -n "$entity_id" ]]; then
    echo "New Entity ID:     $entity_id"
else
    echo "New Entity ID:     $shared_entity_id (shared default)"
fi
if [[ "$current" == "$entity_id" ]]; then
    echo
    echo "Nothing to change. Make sure your identity provider uses: $idp_value"
    exit 0
fi

cat <<EOF

After this change, SSO sign-in for this organization fails until your identity
provider uses the same Entity ID:
  Microsoft Entra ID: Identifier (Entity ID)      = $idp_value
  Okta:               Audience URI (SP Entity ID) = $idp_value
Have that change ready to make straight away.

EOF
if [[ "$assume_yes" != true ]]; then
    read -r -p "Type 'yes' to continue: " answer
    [[ "$answer" == yes ]] || die "cancelled; nothing was changed."
fi

api_call PATCH "/team/resources/sso/v1/configurations/$config_id" \
    "$(jq -cn --arg e "$entity_id" '{spEntityId: $e}')" > /dev/null

cat <<EOF

Done. Materialize now uses this Entity ID for the organization:
  $idp_value

Next steps:
  1. Set the same value in your identity provider (see above).
  2. Sign in to Materialize through SSO to confirm it works.
To undo, run '$(basename "$0") $( [[ "$command" == set ]] && echo reset || echo set )' and restore the previous value in your identity provider.
EOF
