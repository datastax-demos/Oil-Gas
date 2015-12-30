nohup python KafkaListener.py &> /dev/null &
cd DrillSim
nohup ./DrillSim3 &> /dev/null &
cd ..
dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 \
      KafkaConsumer.py \
      localhost:9998 oil-gas

