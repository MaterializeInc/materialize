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

  bin/environmentd \
    --oidc-issuer="http://127.0.0.1:4444" \
    --oidc-jwks-uri="http://127.0.0.1:4444/.well-known/jwks.json" \
    --listeners-config-path='src/materialized/ci/listener_configs/oidc.json'

eyJhbGciOiJSUzI1NiIsImtpZCI6Ijk3ZTJmOTJhLWM2YjQtNDQ0ZC1hNjZhLWY3Y2YwOTIwNzdhMyIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJjbGllbnRfaWQiOiJlZTY3ZDIzNi1mMzY4LTQ0ZmYtYTBiNS1mYWIxZDQwNjZhOTEiLCJleHAiOjE3NjU0Nzg3NDIsImV4dCI6e30sImlhdCI6MTc2NTQ3NTE0MiwiaXNzIjoiaHR0cDovLzEyNy4wLjAuMTo0NDQ0IiwianRpIjoiMzVmNjZkZTEtMzYzNC00ZmM5LWE2ZTktZjM3MmI0YTVlZTRjIiwibmJmIjoxNzY1NDc1MTQyLCJzY3AiOlsib3BlbmlkIiwib2ZmbGluZSJdLCJzdWIiOiJmb29AYmFyLmNvbSJ9.v_tSd01RUVxUvvpO3lBRtFTyZwekliiXpDGAFDeaoOALVUQhtzrYOqWowUNPoMK8mFowwLAVHpc0VOnVHqPVEino4cp3Q3o_FGOGzSdkDBvh2ZmGeC_4uT_C3fzIz7I6fHNCqE7kY_r0EZgguJPepMpgfZ0irjV972tLHV612vrG9LyoplbJCGr6mvkkWBB1fqzcn6C4WnjNaSCArb-riJtfMLNH-AWzotOfJbvdHqNbQcFiNKpA7Xc1sderpLlFdf19U-5NRuQ1YpT1jhKj10JwUIX_Ct7btk8H2LPJ405pVaIU-TvYWMY8mOfoPvmjOxbhMPOgn_aYhJkRjTk1f1H4K8MTMEnKRon7H-Jn-YMnEUVH8bQvLY76fu0LQ6mGbbsdy_o-1bp99n1cSdQIJkHYtPXUspgQPbOLwjYns19M8nEwFamHwUruAm_RBZYiAQTB3Z-SjKGi4HWLwZXr0OaRx-aP-HTVaG6v14z7Vr_mKbcLN_ZyQTailqAwPbu0NRIP-tXp8owlVwNbyL1FdqbM58g5lnxao6H77bmHCyG8YZTYm7ID-plCrGnrSkJm2AN_9PaxgeMvJ9ekcQg2nEpvsO5D1eDyMuKE5CCTZrs7c8Y5G-tCHiCbqftwFcBLgDUc-voSk0gFavS1bFCcXw4ExKfDGjy_4oXDOeBUWcw
