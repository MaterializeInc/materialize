create materialized source "mz_source" from postgres
    host 'host=localhost user=XXX password=XXX dbname=benchmark' publication 'mz_source';

create views from source "mz_source";
show full views;
select count(1) from cpu;
