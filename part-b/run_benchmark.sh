#!/bin/bash

# Ensure the result directory exists

RESULT_DIR="result"
mkdir -p "${RESULT_DIR}"

# Define the set of concurrent requests and parallel connections to test
CONCURRENTS=(10000 100000 1000000)
PARALLELS=(10 100 1000)

echo "=================== BLINK DB BENCHMARK RESULTS ==================="

# Loop over each combination
for c in "${CONCURRENTS[@]}"; do
  for p in "${PARALLELS[@]}"; do
    echo "Running benchmark with ${c} requests and ${p} connections..."
    OUTPUT_FILE="${RESULT_DIR}/result_${c}_${p}.txt"
    
    # Run redis-benchmark and capture output
    BENCHMARK=$(redis-benchmark -h 127.0.0.1 -p 9001 -c "${p}" -n "${c}" -t set,get)
    
    # Extract only the summary sections for SET
    echo "====== SET ======" > "${OUTPUT_FILE}"
    echo "${c} requests completed" >> "${OUTPUT_FILE}"
    echo "${p} parallel clients" >> "${OUTPUT_FILE}"
    echo "3 bytes payload" >> "${OUTPUT_FILE}"
    echo "keep alive: 1" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    
    # Extract SET latency percentiles and summary
    SET_PERCENTILES=$(echo "$BENCHMARK" | sed -n '/Latency by percentile distribution:/,/Cumulative distribution/p' | grep -v "Cumulative")
    SET_SUMMARY=$(echo "$BENCHMARK" | grep -A 7 "Summary:" | head -n 8)
    
    # Write SET sections to file
    echo "Latency by percentile distribution:" >> "${OUTPUT_FILE}"
    echo "$SET_PERCENTILES" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    echo "Summary:" >> "${OUTPUT_FILE}"
    echo "$SET_SUMMARY" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    
    # Extract only the summary sections for GET
    echo "====== GET ======" >> "${OUTPUT_FILE}"
    echo "${c} requests completed" >> "${OUTPUT_FILE}"
    echo "${p} parallel clients" >> "${OUTPUT_FILE}"
    echo "3 bytes payload" >> "${OUTPUT_FILE}"
    echo "keep alive: 1" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    
    # Extract GET latency percentiles and summary (from second occurrence)
    GET_PERCENTILES=$(echo "$BENCHMARK" | sed -n '/Latency by percentile distribution:/,/Cumulative distribution/p' | grep -v "Cumulative" | tail -n $(echo "$BENCHMARK" | sed -n '/Latency by percentile distribution:/,/Cumulative distribution/p' | grep -v "Cumulative" | wc -l))
    GET_SUMMARY=$(echo "$BENCHMARK" | grep -A 7 "Summary:" | tail -n 7)
    
    # Write GET sections to file
    echo "Latency by percentile distribution:" >> "${OUTPUT_FILE}"
    echo "$GET_PERCENTILES" >> "${OUTPUT_FILE}"
    echo "" >> "${OUTPUT_FILE}"
    echo "Summary:" >> "${OUTPUT_FILE}"
    echo "$GET_SUMMARY" >> "${OUTPUT_FILE}"
    
    echo "  Results saved to ${OUTPUT_FILE}"
  done
done

echo "All benchmarks completed successfully."