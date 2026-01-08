#!/bin/bash
# Health IoT Database Benchmarking - Complete Test Runner
# Run all the tests across PostgreSQL, InfluxDB, and MongoDB

set -e

echo "================================================"
echo "HEALTH IOT DATABASE BENCHMARKING SUITE"
echo "================================================"
echo "Starting at: $(date)"
echo ""

# Configuration
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="$BASE_DIR/outputs"
LOG_DIR="$BASE_DIR/logs"
CONFIG_DIR="$BASE_DIR/config"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_ID="run_${TIMESTAMP}"

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$LOG_DIR"
mkdir -p "$CONFIG_DIR"

# Load configuration
if [ -f "$CONFIG_DIR/benchmark_config.sh" ]; then
    source "$CONFIG_DIR/benchmark_config.sh"
else
    # Default configuration
    DATASET_SIZE=1000000
    TEST_ITERATIONS=3
    WARMUP_ITERATIONS=1
    DATABASE_HOST="localhost"
    POSTGRES_PORT=5432
    INFLUXDB_PORT=8086
    MONGODB_PORT=27017
    DATABASE_USER="benchmark_user"
    DATABASE_PASSWORD="benchmark_pass"
    DATABASE_NAME="health_iot_benchmark"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a service is running
check_service() {
    local service=$1
    local port=$2
    
    log_info "Checking $service on port $port..."
    
    if nc -z localhost "$port" 2>/dev/null; then
        log_success "$service is running"
        return 0
    else
        log_error "$service is not running on port $port"
        return 1
    fi
}

# Function to run PostgreSQL tests
run_postgresql_tests() {
    log_info "Starting PostgreSQL benchmark tests..."
    
    local pg_log="$LOG_DIR/postgresql_${RUN_ID}.log"
    
    # 1. Setup RLS
    log_info "Setting up Row Level Security..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/02_setup_rls.sql" >> "$pg_log" 2>&1
    
    # 2. Import data
    log_info "Importing data..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/03_data_import.sql" >> "$pg_log" 2>&1
    
    # 3. Ingestion performance
    log_info "Testing ingestion performance..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/04_ingestion_performance.sql" >> "$pg_log" 2>&1
    
    # 4. Query performance
    log_info "Testing query performance..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/05_query_performance.sql" >> "$pg_log" 2>&1
    
    # 5. Security overhead
    log_info "Testing security overhead..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/06_security_overhead.sql" >> "$pg_log" 2>&1
    
    # 6. Storage efficiency
    log_info "Testing storage efficiency..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/07_storage_efficiency.sql" >> "$pg_log" 2>&1
    
    # 7. Indexing performance
    log_info "Testing indexing performance..."
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/02_postgresql/08_indexing_performance.sql" >> "$pg_log" 2>&1
    
    log_success "PostgreSQL tests completed"
}

# Function to run InfluxDB tests
run_influxdb_tests() {
    log_info "Starting InfluxDB benchmark tests..."
    
    local influx_log="$LOG_DIR/influxdb_${RUN_ID}.log"
    
    # 1. Setup
    log_info "Setting up InfluxDB..."
    bash "$BASE_DIR/03_influxdb/09_setup.sh" >> "$influx_log" 2>&1
    
    # 2. Import data
    log_info "Importing data..."
    python3 "$BASE_DIR/03_influxdb/10_data_import.py" >> "$influx_log" 2>&1
    
    # 3. Ingestion performance
    log_info "Testing ingestion performance..."
    influx query -f "$BASE_DIR/03_influxdb/11_ingestion_performance.flux" >> "$influx_log" 2>&1
    
    # 4. Query performance
    log_info "Testing query performance..."
    python3 "$BASE_DIR/03_influxdb/12_query_performance.py" >> "$influx_log" 2>&1
    
    # 5. Security tokens
    log_info "Testing security tokens..."
    python3 "$BASE_DIR/03_influxdb/13_security_tokens.py" >> "$influx_log" 2>&1
    
    # 6. Storage efficiency
    log_info "Testing storage efficiency..."
    influx query -f "$BASE_DIR/03_influxdb/14_storage_efficiency.flux" >> "$influx_log" 2>&1
    
    # 7. Indexing performance
    log_info "Testing indexing performance..."
    python3 "$BASE_DIR/03_influxdb/15_indexing_performance.py" >> "$influx_log" 2>&1
    
    log_success "InfluxDB tests completed"
}

# Function to run MongoDB tests
run_mongodb_tests() {
    log_info "Starting MongoDB benchmark tests..."
    
    local mongo_log="$LOG_DIR/mongodb_${RUN_ID}.log"
    
    # 1. Setup
    log_info "Setting up MongoDB..."
    bash "$BASE_DIR/04_mongodb/16_setup.sh" >> "$mongo_log" 2>&1
    
    # 2. Import data
    log_info "Importing data..."
    mongosh --host "$DATABASE_HOST" --port "$MONGODB_PORT" \
        --username "$DATABASE_USER" --password "$DATABASE_PASSWORD" \
        "$DATABASE_NAME" "$BASE_DIR/04_mongodb/17_data_import.js" >> "$mongo_log" 2>&1
    
    # 3. Ingestion performance
    log_info "Testing ingestion performance..."
    mongosh --host "$DATABASE_HOST" --port "$MONGODB_PORT" \
        --username "$DATABASE_USER" --password "$DATABASE_PASSWORD" \
        "$DATABASE_NAME" "$BASE_DIR/04_mongodb/18_ingestion_performance.js" >> "$mongo_log" 2>&1
    
    # 4. Query performance
    log_info "Testing query performance..."
    mongosh --host "$DATABASE_HOST" --port "$MONGODB_PORT" \
        --username "$DATABASE_USER" --password "$DATABASE_PASSWORD" \
        "$DATABASE_NAME" "$BASE_DIR/04_mongodb/19_query_performance.js" >> "$mongo_log" 2>&1
    
    # 5. Security roles
    log_info "Testing security roles..."
    mongosh --host "$DATABASE_HOST" --port "$MONGODB_PORT" \
        --username "$DATABASE_USER" --password "$DATABASE_PASSWORD" \
        "$DATABASE_NAME" "$BASE_DIR/04_mongodb/20_security_roles.js" >> "$mongo_log" 2>&1
    
    # 6. Storage efficiency
    log_info "Testing storage efficiency..."
    mongosh --host "$DATABASE_HOST" --port "$MONGODB_PORT" \
        --username "$DATABASE_USER" --password "$DATABASE_PASSWORD" \
        "$DATABASE_NAME" "$BASE_DIR/04_mongodb/21_storage_efficiency.js" >> "$mongo_log" 2>&1
    
    # 7. Indexing performance
    log_info "Testing indexing performance..."
    mongosh --host "$DATABASE_HOST" --port "$MONGODB_PORT" \
        --username "$DATABASE_USER" --password "$DATABASE_PASSWORD" \
        "$DATABASE_NAME" "$BASE_DIR/04_mongodb/22_indexing_performance.js" >> "$mongo_log" 2>&1
    
    log_success "MongoDB tests completed"
}

# Function to collect and analyze results
collect_results() {
    log_info "Collecting and analyzing results..."
    
    local collector_log="$LOG_DIR/collector_${RUN_ID}.log"
    
    # Run results collector
    python3 "$BASE_DIR/05_benchmarking/24_results_collector.py" --run-id "$RUN_ID" ${KEEP_ONLY_CURRENT} >> "$collector_log" 2>&1
    
    # Generate comparison SQL
    psql -h "$DATABASE_HOST" -p "$POSTGRES_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" \
        -f "$BASE_DIR/05_benchmarking/25_comparison.sql" >> "$collector_log" 2>&1
    
    # Run security analysis
    python3 "$BASE_DIR/05_benchmarking/26_security_analysis.py" --run-id "$RUN_ID" >> "$collector_log" 2>&1
    
    # Generate final tables
    python3 "$BASE_DIR/05_benchmarking/27_final_tables.py" --run-id "$RUN_ID" >> "$collector_log" 2>&1
    
    # Create visualizations
    python3 "$BASE_DIR/05_benchmarking/28_visualize.py" --run-id "$RUN_ID" >> "$collector_log" 2>&1
    
    log_success "Results collection completed"
}

# Function to generate summary report
generate_summary() {
    log_info "Generating summary report..."
    
    local summary_file="$OUTPUT_DIR/summary_${RUN_ID}.txt"
    
    cat > "$summary_file" << EOF
HEALTH IOT DATABASE BENCHMARKING SUMMARY
=========================================
Run ID: $RUN_ID
Timestamp: $(date)
Total Execution Time: $SECONDS seconds

DATABASES TESTED:
- PostgreSQL: $(check_service "PostgreSQL" "$POSTGRES_PORT" && echo "✓" || echo "✗")
- InfluxDB: $(check_service "InfluxDB" "$INFLUXDB_PORT" && echo "✓" || echo "✗")
- MongoDB: $(check_service "MongoDB" "$MONGODB_PORT" && echo "✓" || echo "✗")

TEST CATEGORIES COMPLETED:
1. Dataset Generation
2. Database Setup
3. Data Import
4. Ingestion Performance
5. Query Performance
6. Security Analysis
7. Storage Efficiency
8. Indexing Performance

OUTPUT FILES:
- Logs: $LOG_DIR/
- Results: $OUTPUT_DIR/
- Visualizations: $OUTPUT_DIR/visualizations/

NEXT STEPS:
1. Review detailed results in: $OUTPUT_DIR/final_results_${RUN_ID}.csv
2. View visualizations: $OUTPUT_DIR/visualizations/
3. Check logs for any warnings or errors: $LOG_DIR/

EOF
    
    cat "$summary_file"
    log_success "Summary report saved to: $summary_file"
}

# Function to clean up old results
cleanup_old_results() {
    local days_to_keep=7
    
    log_info "Cleaning up results older than $days_to_keep days..."
    
    find "$OUTPUT_DIR" -name "*.json" -type f -mtime +$days_to_keep -delete
    find "$OUTPUT_DIR" -name "*.csv" -type f -mtime +$days_to_keep -delete
    find "$OUTPUT_DIR" -name "*.txt" -type f -mtime +$days_to_keep -delete
    find "$LOG_DIR" -name "*.log" -type f -mtime +$days_to_keep -delete
    
    log_success "Cleanup completed"
}

# Main execution function
main() {
    local start_time=$SECONDS
    
    echo ""
    log_info "Initializing benchmark suite..."
    log_info "Run ID: $RUN_ID"
    log_info "Output directory: $OUTPUT_DIR"
    log_info "Log directory: $LOG_DIR"
    
    # Check prerequisites
    log_info "Checking prerequisites..."
    
    # Check Python
    if command -v python3 &>/dev/null; then
        log_success "Python 3 is available"
    else
        log_error "Python 3 is required but not installed"
        exit 1
    fi
    
    # Check database clients
    if command -v psql &>/dev/null; then
        log_success "PostgreSQL client (psql) is available"
    else
        log_warning "PostgreSQL client not found, some tests may fail"
    fi
    
    if command -v influx &>/dev/null; then
        log_success "InfluxDB client is available"
    else
        log_warning "InfluxDB client not found, some tests may fail"
    fi
    
    if command -v mongosh &>/dev/null; then
        log_success "MongoDB client (mongosh) is available"
    else
        log_warning "MongoDB client not found, some tests may fail"
    fi
    
    # Parse command line arguments
    local run_all=true
    local run_postgres=false
    local run_influx=false
    local run_mongo=false
    local collect_only=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --postgresql-only)
                run_all=false
                run_postgres=true
                shift
                ;;
            --influxdb-only)
                run_all=false
                run_influx=true
                shift
                ;;
            --mongodb-only)
                run_all=false
                run_mongo=true
                shift
                ;;
            --collect-only)
                collect_only=true
                shift
                ;;
            --cleanup)
                cleanup_old_results
                exit 0
                ;;
            --keep-only-current)
                KEEP_ONLY_CURRENT="--keep-only-current"
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Run tests based on flags
    if [ "$collect_only" = true ]; then
        collect_results
    elif [ "$run_all" = true ]; then
        # Run all databases
        check_service "PostgreSQL" "$POSTGRES_PORT" && run_postgresql_tests || log_error "Skipping PostgreSQL tests"
        check_service "InfluxDB" "$INFLUXDB_PORT" && run_influxdb_tests || log_error "Skipping InfluxDB tests"
        check_service "MongoDB" "$MONGODB_PORT" && run_mongodb_tests || log_error "Skipping MongoDB tests"
        collect_results
    else
        # Run specific databases
        [ "$run_postgres" = true ] && check_service "PostgreSQL" "$POSTGRES_PORT" && run_postgresql_tests
        [ "$run_influx" = true ] && check_service "InfluxDB" "$INFLUXDB_PORT" && run_influxdb_tests
        [ "$run_mongo" = true ] && check_service "MongoDB" "$MONGODB_PORT" && run_mongodb_tests
        collect_results
    fi
    
    local end_time=$SECONDS
    local duration=$((end_time - start_time))
    
    echo ""
    log_success "Benchmark suite completed successfully!"
    log_info "Total execution time: $duration seconds"
    
    generate_summary
    
    # Open results directory
    if command -v xdg-open &>/dev/null; then
        xdg-open "$OUTPUT_DIR" 2>/dev/null
    elif command -v open &>/dev/null; then
        open "$OUTPUT_DIR" 2>/dev/null
    fi
}

# Function to show help
show_help() {
    cat << EOF
Usage: $0 [OPTIONS]

Health IoT Database Benchmarking Suite

Options:
    --postgresql-only    Run only PostgreSQL tests
    --influxdb-only      Run only InfluxDB tests
    --mongodb-only       Run only MongoDB tests
    --collect-only       Collect and analyze existing results only
    --cleanup            Clean up old results (older than 7 days)
    --help               Show this help message

Examples:
    $0                    # Run all tests
    $0 --postgresql-only # Run only PostgreSQL tests
    $0 --collect-only    # Analyze existing results

EOF
}

# Trap signals for clean shutdown
trap 'log_error "Benchmark interrupted by user"; exit 1' INT TERM

# Run main function
main "$@"
