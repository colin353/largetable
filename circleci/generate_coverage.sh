#!/bin/bash

find ./target/debug/ -maxdepth 1  -type f -executable -print0 | while IFS= read -r -d $'\0' file; do
        mkdir -p "target/cov/$(basename $file)";
        kcov --include-pattern=largetable --verify "target/cov/$(basename $file)" "$file"
done

bash <(curl -s https://codecov.io/bash)
