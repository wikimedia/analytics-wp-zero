#!/bin/bash

LIMN_DEPLOY_PATH="/home/erosen/src/limn-deploy"
DATA_REPO_PATH="/home/erosen/src/dashboard-data"

CARRIER_DATA_PATH="http://stats.wikimedia.org/kraken-public/webrequest/mobile/zero/carrier/zero_carrier-daily.tsv"
COUNTRY_DATA_PATH="http://stats.wikimedia.org/kraken-public/webrequest/mobile/zero/country/zero_country-daily.tsv"

rm -rf data

# make dashboard files
python make_dashabords.py -l=DEBUG --carrier_counts=$CARRIER_DATA_PATH --country_counts=$COUNTRY_DATA_PATH --daily

# update data repo
cd $DATA_REPO_PATH
git pull
cd -
cp -r data/* $DATA_REPO_PATH
cd $DATA_REPO_PATH
git add -A
git commit -a -m 'automatic commit using deploy_dashboard.sh script'
git push

# deploy data
cd $LIMN_DEPLOY_PATH
fab gp deploy.only_data
