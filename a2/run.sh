hadoop jar /usr/hadoop-3.0.0/share/hadoop/tools/lib/hadoop-streaming-3.0.0.jar \
-file ./umapper.py \
-mapper ./umapper.py \
-file ./ureducer.py \
-reducer ./ureducer.py \
-input /small_tips.csv \
-output /ureducer
