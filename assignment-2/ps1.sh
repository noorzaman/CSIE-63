#!/bin/bash
echo "Usage load_json.sh 'http://json.api.com?params=values' import_json.cypher"
echo "Use {data} as parameter in your query for the JSON data"

curl --header "Content-Type: application/json" \
--request POST \
--data "@data/input/create-statements.json" \
http://localhost:7474/db/data/transaction/commit
