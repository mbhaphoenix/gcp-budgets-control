#!/usr/bin/env bash

# at the moment of writing we need to use the beta version to deploy CF with env vars
gcloud beta --project=mehdi-labs-201811 functions deploy handle_budgets_notifications --set-env-vars COLLECTION_NAME_PREFIX=budget-notifs --region=europe-west1 --memory=128MB --runtime=python37 --trigger-topic=budgets-notifications