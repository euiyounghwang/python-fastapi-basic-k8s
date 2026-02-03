#!/bin/sh

JUPYTER_NOTEBOOK_PATH=/apps/monitoring_script/jupyter_notebook
PATH=$PATH:$JUPYTER_NOTEBOOK_PATH
SERVICE_NAME=jupyter-notebook-service-all-service

# -- enable jdk11 for h2/disable jdk11 path for oracle
JAVA_HOME=/apps/java/jdk-11.0.10+9
export PATH=$JAVA_HOME/bin:$PATH
export JAVA_HOME


# -- spark
SPARK_PATH=/apps/monitoring_script/spark
export PYSPARK_PYTHON=$SPARK_PATH/.venv/bin/python
#export PYSPARK_DRIVER_PYTHON=$SPARK_PATH/.venv/bin/python
#export PYSPARK_DRIVER_PYTHON=jupyter
#export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.9
#export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
#export PYSPARK_PYTHON=/usr/local/bin/python3.9
#export PYSPARK_SUBMIT_ARGS="--jars /apps/monitoring_script/spark/jars/elasticsearch-spark-30_2.12-8.7.0.jar pyspark-shell"


SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd $SCRIPTDIR
echo $SCRIPTDIR

VENV=".venv"

# Python 3.11.7 with Window
if [ -d "$SCRIPTDIR/$VENV/bin" ]; then
    source $SCRIPTDIR/$VENV/bin/activate
else
    source $SCRIPTDIR/$VENV/Scripts/activate
fi

#echo $SCRIPTDIR


# See how we were called.
case "$1" in
  start)
        # Start daemon.
        echo "Starting $SERVICE_NAME";
        nohup jupyter lab --ip 0.0.0.0 --allow-root --certfile=./certs/mycert.pem --keyfile ./certs/mykey.key &> /dev/null &
        #jupyter notebook --ip 0.0.0.0 --allow-root --certfile=./certs/mycert.pem --keyfile ./certs/mykey.key
        ;;
  stop)
        # Stop daemons.
        echo "Shutting down $SERVICE_NAME";
        pid=`ps ax | grep -i '/jupyter-notebook' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          kill -9 $pid
         else
          echo "$SERVICE_NAME was not Running"
        fi
        ;;
  restart)
        $0 stop
        sleep 2
        $0 start
        ;;
  status)
        pid=`ps ax | grep -i '/jupyter-notebook' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          echo "$SERVICE_NAME is Running as PID: $pid"
        else
          echo "$SERVICE_NAME is not Running"
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
esac


