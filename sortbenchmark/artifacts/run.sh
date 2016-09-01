#!/usr/bin/env bash
sudo sh -c "sync ; echo 3 > /proc/sys/vm/drop_caches"
sudo pdsh -w ^/home/ec2-user/workers sh -c "sync ; echo 3 > /proc/sys/vm/drop_caches"

CLOUDSORT_DIR=/efs/cloudsort

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <run id>"
fi

RUN_ID=$1
RUN_DIR=${CLOUDSORT_DIR}/run/${RUN_ID}

if [ -d "$RUN_DIR" ]; then
  echo "Run directory $RUN_DIR already exists!"
  exit -1
fi

python -u ${CLOUDSORT_DIR}/statsd_server.py 9020 > statsd_server.log &
statsd_server_pid=$!

date +%s.%N
./bin/start-cluster.sh

# wait for all TaskManagers to start
CONF=conf/flink-conf.yaml
read HOST PORT SLOTS <<<$(python -c 'import yaml; conf=yaml.load(open("'${CONF}'")); \
  print conf["jobmanager.rpc.address"], conf["jobmanager.web.port"], conf["parallelism.default"]')

while [ $SLOTS -ne `curl -s http://${HOST}:${PORT}/overview | python -c $'import sys, json; \
  data=sys.stdin.read(); print(json.loads(data)["slots-total"] if data else 0)'` ] ; do sleep 1 ; done

# execute FlinkSort
./bin/flink run -q -class org.apache.flink.cloudsort.indy.IndySort ${CLOUDSORT_DIR}/flink-cloudsort-0.1-dev_shm_timeout.jar \
  --input awscli --input_bucket cloudsort --input_prefix input/ \
  --output awscli --output_bucket cloudsort --output_prefix output${RUN_ID}/ \
  --buffer_size 67108864 --chunk_size 250000000 --concurrent_files 16 --storage_class REDUCED_REDUNDANCY \
  --download_timeout 120 --upload_timeout 60

date +%s.%N
./bin/stop-cluster.sh

if kill -0 $statsd_server_pid 2>&1; then
  kill $statsd_server_pid
else
  echo "No statsd_server found with PID $statsd_server_pid"
fi

mkdir -p $RUN_DIR

mv statsd_server.log $RUN_DIR

mv log $RUN_DIR
mkdir log

