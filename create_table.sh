#/bin/bash
createdb aozora;
psql -d aozora -f create_table.sql
