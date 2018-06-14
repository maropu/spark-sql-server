#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Shell script for starting the Spark SQL server

# If multi-context mode enabled, this is the memory size for the server itself
SPARK_SQL_SERVER_MEM_IN_MULTI_CONTEXT_MODE="1g"

# Determine the current working directory
_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "${SPARK_HOME}" ]; then
  _CALLING_DIR="$(pwd)"

  # Install the proper version of Spark for launching the SQL server
  . ${_DIR}/../thirdparty/install.sh
  install_spark

  cd "${_CALLING_DIR}"
else
  SPARK_DIR=${SPARK_HOME}
fi

if [ -z "${LIVY_HOME}" ]; then
  _CALLING_DIR="$(pwd)"

  # Install the proper version of Livy for multi-context mode
  . ${_DIR}/../thirdparty/install.sh
  install_livy

  cd "${_CALLING_DIR}"
else
  LIVY_DIR=${LIVY_HOME}
fi

function usage {
  echo "Usage: ./sbin/start-sql-server.sh [options] [SQL server options]"
  "${SPARK_DIR}"/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  echo "SQL server options:"
  echo "  --conf spark.sql.server.port=NUM                    Port number of SQL server interface (Default: 5432)."
  echo "  --conf spark.sql.server.executionMode=STR           Execution mode: single-session, multi-session, or multi-context (Default: multi-session)".
  echo "  --conf spark.sql.server.worker.threads=NUM          # of SQLServer worker threads (Default: 4)."
  echo "  --conf spark.sql.server.binaryTransferMode=BOOL     Whether binary transfer mode is enabled (Default: true)."
  echo "  --conf spark.sql.server.ssl.enabled=BOOL            Enable SSL encryption (Default: false)."
  echo "  --conf spark.sql.server.ssl.path=STR                Keystore path."
  echo "  --conf spark.sql.server.ssl.keystore.passwd=STR     Keystore password."
  echo "  --conf spark.sql.server.ssl.certificate.passwd=STR  Certificate password."
  echo "  --conf spark.yarn.keytab=STR                        Keytab file location."
  echo "  --conf spark.yarn.principal=STR                     Principal name in a secure cluster."
  echo "  --conf spark.yarn.impersonation.enabled=BOOL        Whether authentication impersonates connected users (Default: false)."
  echo
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

# Resolve a jar location for the SQL server
function find_resource {
  local sql_server_version=`grep "<version>" "${_DIR}/../pom.xml" | head -n2 | tail -n1 | awk -F '[<>]' '{print $3}'`
  local spark_version=`grep "<spark.version>" "${_DIR}/../pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  local scala_version=`grep "<scala.binary.version>" "${_DIR}/../pom.xml" | head -n1 | awk -F '[<>]' '{print $3}'`
  local jar_file="sql-server_${scala_version}_${spark_version}_${sql_server_version}-with-dependencies.jar"
  local built_jar="$_DIR/../target/${jar_file}"
  if [ -e $built_jar ]; then
    RESOURCES=$built_jar
  else
    RESOURCES="$_DIR/../assembly/${jar_file}"
    echo "${built_jar} not found, so use pre-compiled ${RESOURCES}"
  fi
}

function join_by {
  local IFS="$1"
  shift
  echo "$*"
}

function check_if_multi_context_mode_enabled {
  SPARK_SQL_SERVER_MULTI_CONTEXT_MODE="0"

  while [ ! -z "$1" ]; do
    if [ "$1" == "--conf" ]; then
      shift
      if [ $1 == "spark.sql.server.executionMode=multi-context" ]; then
        SPARK_SQL_SERVER_MULTI_CONTEXT_MODE="1"
      fi
    fi
    shift
  done
}

function parse_args_for_spark_submit {
  SPARK_SQL_SERVER_CONF=()

  # TODO: Needs to build a custom Spark launcher for multi-context mode
  if [ "$SPARK_SQL_SERVER_MULTI_CONTEXT_MODE" == 1 ]; then
    SPARK_MASTER="local[*]"
    SPARK_DEPLOY_MODE="client"
    SPARK_DRIVER_CORES="1"
    SPARK_DRIVER_MEM="1g"
    SPARK_EXECUTOR_CORES="1"
    SPARK_EXECUTOR_MEM="1g"

    # Set "=" to extract input parameters
    IFS_ORIGINAL="$IFS"
    IFS="="

    while [ ! -z "$1" ]; do
      if [[ "$1" =~ ^--master ]]; then
        shift; SPARK_MASTER=$1
      elif [[ "$1" =~ ^--deploy-mode ]]; then
        shift; SPARK_DEPLOY_MODE=$1
      elif [[ "$1" =~ ^--driver-cores ]]; then
        shift; SPARK_DRIVER_CORES=$1
      elif [[ "$1" =~ ^--driver-memory ]]; then
        shift; SPARK_DRIVER_MEM=$1
      elif [[ "$1" =~ ^--executor-cores ]]; then
        shift; SPARK_EXECUTOR_CORES=$1
      elif [[ "$1" =~ ^--executor-memory ]]; then
        shift; SPARK_EXECUTOR_MEM=$1

      # Check other conf params
      elif [ "$1" == "--conf" ]; then
        shift
        if [[ "$1" =~ ^spark.master ]]; then
          v=($1); SPARK_MASTER=${v[1]}
        elif [[ "$1" =~ ^spark.deploy-mode ]]; then
          v=($1); SPARK_DEPLOY_MODE=${v[1]}
        elif [[ "$1" =~ ^spark.driver.cores ]]; then
          v=($1); SPARK_DRIVER_CORES=${v[1]}
        elif [[ "$1" =~ ^spark.driver.memory ]]; then
          v=($1); SPARK_DRIVER_MEM=${v[1]}
        elif [[ "$1" =~ ^spark.executor.cores ]]; then
          v=($1); SPARK_EXECUTOR_CORES=${v[1]}
        elif [[ "$1" =~ ^spark.executor.memory ]]; then
          v=($1); SPARK_EXECUTOR_MEM=${v[1]}
        else
          # Pass other conf params as they are
          SPARK_SQL_SERVER_CONF+=("--conf $1")
        fi
      else
        # Pass other options as they are
        SPARK_SQL_SERVER_CONF+=("$1 $2")
        shift
      fi
      shift
    done

    # Set parameters for the Spark jobs that Livy lauches
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.spark.master=$SPARK_MASTER")
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.spark.deploy-mode=$SPARK_DEPLOY_MODE")
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.spark.driver.cores=$SPARK_DRIVER_CORES")
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.spark.driver.memory=$SPARK_DRIVER_MEM")
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.spark.executor.cores=$SPARK_EXECUTOR_CORES")
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.spark.executor.memory=$SPARK_EXECUTOR_MEM")

    # Set basic parameters for the SQL server itself
    SPARK_SQL_SERVER_CONF+=("--conf spark.sql.server.livy.home=${LIVY_DIR}")
    SPARK_SQL_SERVER_CONF+=("--conf spark.master=local")
    SPARK_SQL_SERVER_CONF+=("--conf spark.driver.cores=1")
    SPARK_SQL_SERVER_CONF+=("--conf spark.driver.memory=${SPARK_SQL_SERVER_MEM_IN_MULTI_CONTEXT_MODE}")

    IFS="$IFS_ORIGINAL"
  else
    # Pass as they are
    SPARK_SQL_SERVER_CONF="$@"
  fi
}

# Do some preparations before launching spark-submit
check_if_multi_context_mode_enabled "$@"
parse_args_for_spark_submit "$@"
find_resource

echo "Using \`spark-submit\` from path: $SPARK_DIR" 1>&2
CLASS="org.apache.spark.sql.server.SQLServer"
PROPERTY_FILE=${_DIR}/../conf/spark-defaults.conf
APP_NAME="Spark SQL Server"
exec "${SPARK_DIR}"/sbin/spark-daemon.sh submit $CLASS 1 --name "${APP_NAME}" --properties-file ${PROPERTY_FILE} \
  $(join_by " " ${SPARK_SQL_SERVER_CONF[@]}) \
  ${RESOURCES}

