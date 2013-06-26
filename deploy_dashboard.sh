#!/bin/bash

LIMN_DEPLOY_PATH="/home/erosen/src/limn-deploy"
DATA_REPO_PATH="/home/erosen/src/dashboard-data"

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
