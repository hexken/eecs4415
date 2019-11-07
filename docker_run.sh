#!/bin/bash

docker run -it -p 9870:9870 -p 8088:8088 -v $PWD:/app eecsyorku/eecs4415
