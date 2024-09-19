#!/usr/bin/env bash

if [ "$1" = "debug" ]; then
    nitro-cli run-enclave --cpu-count 2 --memory 2048 --enclave-cid 16 --eif-path app.eif --attach-console
else
    nitro-cli run-enclave --cpu-count 2 --memory 2048 --enclave-cid 16 --eif-path app.eif
fi