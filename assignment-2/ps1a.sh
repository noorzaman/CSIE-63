#!/bin/bash

curl -i -H accept:application/json -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d "{\"statements\":[{\"statement\":\"MATCH (m) return m\"}]}"
