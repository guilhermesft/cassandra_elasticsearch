#!/bin/bash
#Script para inicar aplicação com valores default. Eh só um facilitador

python src/sync.py -t 60 -r $PWD/resource -c localhost -e localhost
