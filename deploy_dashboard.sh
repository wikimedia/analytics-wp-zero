#!/bin/bash

LIMN_DEPLOY_KEY="/home/erosen/src/wp-zero/deploy_key"
DATA_REPO_PATH="/home/erosen/src/wp-zero-data"

CARRIER_DATA_PATH="http://stats.wikimedia.org/kraken-public/webrequest/mobile/zero/carrier/zero_carrier-daily.tsv"
COUNTRY_DATA_PATH="http://stats.wikimedia.org/kraken-public/webrequest/mobile/zero/country/zero_country-daily.tsv"

rm -rf data

# make dashboard files
python make_dashabords.py -l=DEBUG --carrier_counts=$CARRIER_DATA_PATH --country_counts=$COUNTRY_DATA_PATH --daily --limn_basedir=$DATA_REPO_PATH

# setup limn_deploy ssh identity
eval `ssh-agent`
ssh-add $LIMN_DEPLOY_KEY

# update data repo
cd $DATA_REPO_PATH
git pull
git add -A
git commit -m 'automatic commit using deploy_dashboard.sh script'
git push

# remove deploy key
ssh-add -d $LIMN_DEPLOY_KEY
