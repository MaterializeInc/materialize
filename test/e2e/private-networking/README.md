# End-to-end private networking test

This directory

## Instructions

```
pulumi stack select materialize/dev
pulumi up
```

```sql
CREATE CONNECTION pl1 TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0497864129062e875',
    AVAILABILITY ZONES ('use1-az1')
);

CREATE CONNECTION pl2 TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0497864129062e875',
    AVAILABILITY ZONES ('use1-az2')
);

CREATE CONNECTION pl4 TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0497864129062e875',
    AVAILABILITY ZONES ('use1-az4')
);

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'b-1.privatenetworkingdev.bow1vc.c18.kafka.us-east-1.amazonaws.com:9094' USING AWS PRIVATELINK pl1 (PORT 9092),
        'b-2.privatenetworkingdev.bow1vc.c18.kafka.us-east-1.amazonaws.com:9094' USING AWS PRIVATELINK pl2 (PORT 9093),
        'b-3.privatenetworkingdev.bow1vc.c18.kafka.us-east-1.amazonaws.com:9094' USING AWS PRIVATELINK pl2 (PORT 9094),
        'b-4.privatenetworkingdev.bow1vc.c18.kafka.us-east-1.amazonaws.com:9094' USING AWS PRIVATELINK pl2 (PORT 9095),
        'b-5.privatenetworkingdev.bow1vc.c18.kafka.us-east-1.amazonaws.com:9094' USING AWS PRIVATELINK pl1 (PORT 9096),
        'b-6.privatenetworkingdev.bow1vc.c18.kafka.us-east-1.amazonaws.com:9094' USING AWS PRIVATELINK pl1 (PORT 9097),
    )
);
```
