#!/bin/sh

set -e

for file in src/testcases/*.txt;
 do
  EXPECTED=`awk 'NR % 2 == 0' $file`;
  RECEIVED=`awk 'NR % 2 == 1' $file | ./target/debug/largetable-cli --stdin localhost:8080`;
  if [ "$EXPECTED" = "$RECEIVED" ]
    then
      echo "test passed."
      exit 0
    else
      echo "Test failed, output did not match."
      echo "EXPECTED:"
      echo $EXPECTED
      echo "RECEIVED:"
      echo $RECEIVED
      exit 1
    fi;
done;
