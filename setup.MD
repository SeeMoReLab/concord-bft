# Step 1: Remove the stale build directory           
  `rm -rf build`

  # Step 2: Build everything inside Docker
  `make build SKIP_FORMAT=1`

  # Step 3: Login to the container
  `make login`

  ---
  Inside the container (all remaining steps):

  # Step 4: Generate new key files (new format)
  `cd /concord-bft`

  `build/tools/GenerateConcordKeys -n 4 -f 1 -r 0 -o tests/simpleKVBC/scripts/setA_replica_`

  # Step 5: Generate TLS certs (5 = 4 replicas + 1 client)
  `cd /concord-bft/build/tests/simpleKVBC/scripts`

  `/concord-bft/scripts/linux/create_tls_certs.sh 5`

  # Step 6: Clean stale DB files
  `rm -rf simpleKVBTests_DB_*`

  # Step 7: Run the test
  Make changes to shell script to pass in fault configuration using JSON
  
  (sample fault config: concord-bft/tests/simpleKVBC/TesterReplica/strategy/byzantine_fault_config.json)


  `/concord-bft/tests/simpleKVBC/scripts/run_byzantine_test.sh`