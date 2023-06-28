\timing
\! date +%s
\i ddl.sql
\! ./generate-copy.sh
\i copy.sql
\i views.sql
\! date +%s
