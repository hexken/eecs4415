#!/bin/bash

docker run -it --add-host="localhost:192.168.0.100" -v  $PWD:/app --link twitter:twitter eecsyorku/eecs4415
