#!/bin/bash
#docker run -it --add-host="localhost:172.17.0.1" -v  $PWD:/app --link twitter:twitter eecsyorku/eecs4415
docker run -it -v  $PWD:/app --link twitter:twitter eecsyorku/eecs4415
