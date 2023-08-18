#!/bin/bash

# Check if the .replicator directory already exists
if [ ! -d ".replicator" ]; then
  # Clone the hub-monorepo repository into a temporary folder
  git clone https://github.com/farcasterxyz/hub-monorepo.git temp-repo

  # Create the .replicator directory and move the replicate-data-postgres folder into it
  mkdir .replicator
  mv temp-repo/packages/hub-nodejs/examples/replicate-data-postgres .replicator/

  # Remove the temporary cloned repository
  rm -rf temp-repo
fi

# Navigate to the .replicator directory
cd .replicator/replicate-data-postgres

# Run Docker compose
docker compose up
