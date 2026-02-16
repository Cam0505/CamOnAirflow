#!/bin/bash
# Load .env variables
set -o allexport
source /workspaces/CamOnAirFlow/.env
set +o allexport

# Set git config from .env
git config --global user.name "$GIT_USER_NAME"
git config --global user.email "$GIT_USER_EMAIL"
