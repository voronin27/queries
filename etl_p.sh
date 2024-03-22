if [[ -f /tmp/env_metrics.sh ]]; then . /tmp/env_metrics.sh; fi
export PGPASSWORD=xxxx
# grab the last string of MSTR Performance Counters log
# convert it into OpenMetrics format
	# MSTR server pid
	pid=$(pgrep '[M]STRSvr')
	pm_file="${MSTR_LOG_DIR}/mstr/${MSTR_LOG_PC_LOG_DESTINATION}${pid}.csv"
if [[ -f "$pm_file" ]]; then
		 IFS=, read -r  headers <<< "$(head -1 "$pm_file")"
         IFS=, read -r items <<< "$(tail -1 "$pm_file")"
    headers=("${headers// /_}")
    headers=("${headers//./_}")
    headers=("${headers//\\\\/}")
    headers=("${headers//\\//}")
    headers=("${headers//\"/}")   
    items=("${items//\"/}")
  
	dbname="gi2-cent7-6"
	username="performance"
	port="5432"
 	# password="g1n2s3s4"
	ipadr=$(hostname -I)
psql --user=${username} --no-password --host="gi2-cent7-6" --port=5432 --dbname="performance_db" -v ON_ERROR_STOP=1 -c "SELECT * FROM etl_perf ('${ipadr}','${headers}','${items}')"

fi
