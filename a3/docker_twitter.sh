#!/bin/bash
docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash
