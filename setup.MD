# Step 1: Remove the stale build directory                                                                                             
  cd /home/praj/projects/concord-bft                                                                                                     
  rm -rf build

  # Step 2: Build everything inside Docker
  make build

  ---
  Enter the container:

  # Step 3: Login to the container
  make login

  ---
  Inside the container (all remaining steps):

  # Step 4: Generate new key files (new format)
  cd /concord-bft
  build/tools/GenerateConcordKeys -n 4 -f 1 -r 0 -o tests/simpleKVBC/scripts/setA_replica_

  # Step 5: Generate TLS certs (5 = 4 replicas + 1 client)
  cd /concord-bft/build/tests/simpleKVBC/scripts
  /concord-bft/scripts/linux/create_tls_certs.sh 5

  # Step 6: Clean stale DB files
  rm -rf simpleKVBTests_DB_*

  # Step 7: Run the test
  /concord-bft/tests/simpleKVBC/scripts/run_byzantine_test.sh