## Setting

PGOAUTHDEBUG=UNSAFE psql 'host=192.168.215.3 user=employees dbname=promo oauth_issuer=http://host.docker.internal:4444 oauth_client_id=1186624a-7bed-44f8-867e-d3938a29c924 oauth_client_secret=88OlbvOkiDaPSypu94qK_WHDjG'


code_client=$(docker compose -f quickstart.yml exec hydra \
    hydra create client \
    --endpoint http://127.0.0.1:4445 \
    --grant-type authorization_code,refresh_token \
    --response-type code,id_token \
    --format json \
    --scope openid --scope offline --scope profile --scope email\
    --access-token-strategy jwt \
    --redirect-uri http://127.0.0.1:5555/callback)

code_client_id=$(echo $code_client | jq -r '.client_id')
code_client_secret=$(echo $code_client | jq -r '.client_secret')

docker compose -f quickstart.yml exec hydra \
    hydra perform authorization-code \
    --client-id $code_client_id \
    --client-secret $code_client_secret \
    --endpoint http://127.0.0.1:4444/ \
    --port 5555 \
    --scope openid --scope offline --scope profile --scope email

## Deleting a client
hydra delete oauth2-client --endpoint http://localhost:4445  b1a93de1-e4dd-4da9-8e81-083ec4e89f6e=$

client id: 060a4f3d-1cac-46e4-b5a5-6b9c66cd9431
secret: wAghHCKR_E26yuLRpSkaoz2epq


<!-- export URLS_SELF_ISSUER="http://192.168.215.4:4444"
export URLS_LOGIN="http://192.168.215.4:3000/login"
export URLS_CONSENT="http://192.168.215.4:3000/consent"
export URLS_LOGOUT="http://192.168.215.4:3000/logout" -->


device

device_client=$(docker compose -f quickstart.yml exec hydra \
  hydra create client \
    --endpoint http://127.0.0.1:4445 \
    --format json \
    --name "my device app" \
    --grant-type urn:ietf:params:oauth:grant-type:device_code,refresh_token \
    --token-endpoint-auth-method none \
    --access-token-strategy jwt \
    --scope openid,offline_access,profile)

device_client_id=$(echo $device_client | jq -r '.client_id')
device_client_secret=$(echo $device_client | jq -r '.client_secret')

echo $device_client_id
echo $device_client_secret


docker compose -f quickstart.yml exec hydra \
  hydra perform device-code \
    --client-id $device_client_id \
    --client-secret $device_client_secret \
    --endpoint http://127.0.0.1:4444/ \
    --scope openid,offline_access


Visit http://host.docker.internal:4444/oauth2/device/verify and enter the code: mpGRAMPk


http://localhost:4444/.well-known/jwks.json

  bin/environmentd -- \
    --oidc-issuer="http://127.0.0.1:4444" \
    --listeners-config-path='src/materialized/ci/listener_configs/oidc.json'


Access token:

eyJhbGciOiJSUzI1NiIsImtpZCI6Ijk3ZTJmOTJhLWM2YjQtNDQ0ZC1hNjZhLWY3Y2YwOTIwNzdhMyIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJjbGllbnRfaWQiOiJlMGZkYzJkZC05YTU1LTRlZjEtYWU2Zi05YjQyYTJhMzA3NWYiLCJleHAiOjE3NjgyNTgzODksImV4dCI6e30sImlhdCI6MTc2ODI1NDc4OCwiaXNzIjoiaHR0cDovLzEyNy4wLjAuMTo0NDQ0IiwianRpIjoiNTk1N2RiN2YtZGVjZi00OWE0LTliNTMtOTBiM2M1ZDhlMWI5IiwibmJmIjoxNzY4MjU0Nzg4LCJzY3AiOlsib3BlbmlkIiwib2ZmbGluZSIsInByb2ZpbGUiLCJlbWFpbCJdLCJzdWIiOiJmb29AYmFyLmNvbSJ9.w_Vype6NRh_gAIowtja24alhINfXOGRavwq9Nd3gu1tW5l1zxDza6X5iPhMmSTnnlDm1dbekAVQ8tldZs5XDfycDFsFfuMa-IvsoQ3GUyglGMv-hdVa8hDBLGLonVkn5fZDAiotnRzDKZo1qGJ7nDGkV1_oO7DE_BqlXC6OebqQKXdyzZI4xXrreQvQCF0JiW4Kz7F3FZrJeIMyBgMgwgt1spi6YFuER-08l0ZPotrQ20KGhTHy0k-zpyjPUZA8vm8AAiyePvgIHh4pAm_0k4gG_fcX6rw5Hv3UsNtDH42b2QQhGgqY_gvBTNCxCW_wHmHtrgFYiIH7N3NwQE36ZJLSAVL9xuVdaV9km1ZSHAnJ5TdXrtB1wEEsjwYFIrv0AwUv-mlUk0QS7E_8Wv_-BqwgbE4TjdcTIe2-S85N3i7w_LkJT5D1tIwSKlotXCfRV_nTvrWAwar9bLBdynBXYAhwpzASCub_L4qqwCvrWnOPYIHqb9EQFsIEqYaKv_Iz5BLUaMC4fgymSDpsb_kujlQNmR1R_EfMIZA2noFQ8HZ3JJfWckYgLLpJL5RhHDZoQIQOpZL2RvXE1Ud8roT1f2sRGqotNJ93PcBuISzVzJ5ov3ZM7VU3QjhQ4q4Z5q6BQIAFW_j8WoLbWZ9KlCbhVTzXu0usjeI4BHf0_HluIkNE

    Run ```
    PGPASSWORD="eyJhbGciOiJSUzI1NiIsImtpZCI6Ijk3ZTJmOTJhLWM2YjQtNDQ0ZC1hNjZhLWY3Y2YwOTIwNzdhMyIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJjbGllbnRfaWQiOiIyY2MzZTE0Ny1iZTMxLTQzNGYtOWNiYy02MzY3Y2Y0NDE3Y2MiLCJleHAiOjE3Njg5Mzk5MTYsImV4dCI6e30sImlhdCI6MTc2ODkzNjMxNiwiaXNzIjoiaHR0cDovLzEyNy4wLjAuMTo0NDQ0IiwianRpIjoiMjlkMjY4YjgtOTc1ZC00NzhkLTkzOTktNDNjMDk2MTVmZGJlIiwibmJmIjoxNzY4OTM2MzE2LCJzY3AiOlsib3BlbmlkIiwib2ZmbGluZSIsInByb2ZpbGUiLCJlbWFpbCJdLCJzdWIiOiJmb29AYmFyLmNvbSJ9.LlxflqxSf8l0EsgR0F1mVW2JVBYAsVBeElZcqUK6CD-_wBfvgQzCQlIVZNRNYnVQYZwrDzJvrv4-niysYxJ-PRgMAp826nuZIjgQz5qMDovmrqO6UFXSW6pA1rR4N1tQVAYmCAoS-O3PgntJHUE5vpX19D28YbyyS60Yo1u5KzXkQdqZPrkStUhcHXJP-4CfX43k9ginbn23XtKqp0NXzMbJRXk1wY4P6Si-9pqeqLiD7CtNCRXbFowFBePsr9cYoQJRV6ausBRTFE7mXxybU7NFKvuerGNUFI5u0LKzuRsvhw5iuJHPi2PLxrxWjqh3idKgCRbFGJR-Vk63El7Z4O-piwREShyNDfAU1_KREIPt5-zxCp-qe0JhCYEPVikICRT2NF0c29ZzvaxsGEOb8PZBXgPRncfDAsz-fXTOr2MXuGsxIZBgcRx6oDR2mnGZKIXrLqRiBDwik66M2LDE7x5FQZqiha0y2_h_PwNhnDdWqcRAb-l48FqtZCXDi5V4zyfYCw24sWhXVyqLi5WGozavVSxCvZmUP3Qd1OvE9j2n3JOjlecY-7G3ccV1Te_uYNcALyo2DRaiA1mO7XHhmh-9W1_DOZWljmYF9j6qJhMft38N-6fB4Wp8U7vKwdHVfBk5dHb8q95qaLefJkaXk79vA28Wmu6_LHn7EFC0U_M" psql -h localhost -p 6875 -U foo@bar.com materialize
    ```
