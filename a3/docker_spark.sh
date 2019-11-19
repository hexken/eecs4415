#!/bin/bash
docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415
