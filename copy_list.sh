#!/bin/bash
psql -d aozora -c "COPY list FROM '$1' WITH CSV HEADER"
