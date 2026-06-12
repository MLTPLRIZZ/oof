#!/bin/bash

echo "🚀 Sending initial requests to load balance across nodes..."
curl -i http://127.0.0
echo -e "\n"
curl -i http://127.0.0

echo -e "\n⏱️ Requesting a 3rd time to test cache delivery speed (expect identical payload)..."
time curl -s http://127.0.0 | grep source

echo -e "\n📊 Querying the Proxy Diagnostic Dashboard..."
curl -s http://127.0.0 | json_pp
