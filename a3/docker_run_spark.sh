#!/bin/bash

docker run -it --add-host="localhost:10.24.194.21" -v  $PWD:/app --link twitter:twitter eecsyorku/eecs4415
