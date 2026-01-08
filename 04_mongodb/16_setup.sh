#!/bin/bash
# MONGODB SETUP SCRIPTS

set -e  # Exit on error

echo "============================================="
echo "MONGODB SETUP AND CONFIGURATION"
echo "============================================="

# Check if MongoDB is installed
if ! command -v mongod &> /dev/null; then
    echo "Error: MongoDB is not installed."
    echo "Please install MongoDB from: https://www.mongodb.com/try/download/community"
    exit 1
fi

if ! command -v mongo &> /dev/null && ! command -v mongosh &> /dev/null; then
    echo "Error: MongoDB shell is not installed."
    exit 1
fi

# Configuration variables
MONGO_HOST="localhost"
MONGO_PORT="27017"
MONGO_DB="health_iot_benchmark"
MONGO_USER="health_iot_admin"
MONGO_PASS="HealthIoT123!"
DATA_DIR="./mongo_data"
LOG_DIR="./logs"
CONFIG_FILE="./mongodb.conf"

echo ""
echo "Step 1: Creating directories..."

# Create necessary directories
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"
mkdir -p ./scripts
mkdir -p ./backups
mkdir -p ./test_data

echo "  Created directories:"
echo "    • $DATA_DIR"
echo "    • $LOG_DIR"
echo "    • ./scripts"
echo "    • ./backups"
echo "    • ./test_data"

echo ""
echo "Step 2: Creating MongoDB configuration file..."

# Create MongoDB configuration
cat > "$CONFIG_FILE" << EOF
# MongoDB Configuration
storage:
  dbPath: "$DATA_DIR"
  journal:
    enabled: true
  wiredTiger:
    engineConfig:
      cacheSizeGB: 1
      directoryForIndexes: false
    collectionConfig:
      blockCompressor: snappy
    indexConfig:
      prefixCompression: true

systemLog:
  destination: file
  path: "$LOG_DIR/mongodb.log"
  logAppend: true
  quiet: false
  verbosity: 1

processManagement:
  fork: false
  pidFilePath: "$LOG_DIR/mongod.pid"

net:
  port: $MONGO_PORT
  bindIp: 127.0.0.1
  maxIncomingConnections: 100

security:
  authorization: enabled

operationProfiling:
  mode: slowOp
  slowOpThresholdMs: 100
  rateLimit: 100

replication:
  oplogSizeMB: 1024
  replSetName: "healthIoTReplica"

sharding:
  clusterRole: "configsvr"
EOF

echo "  Configuration file created: $CONFIG_FILE"

echo ""
echo "Step 3: Starting MongoDB..."

# Check if MongoDB is already running
if pgrep -x "mongod" > /dev/null; then
    echo "  MongoDB is already running"
    MONGO_RUNNING=true
else
    # Start MongoDB with configuration
    mongod --config "$CONFIG_FILE" --fork --logpath "$LOG_DIR/mongod_start.log"
    
    # Wait for MongoDB to start
    sleep 5
    
    if pgrep -x "mongod" > /dev/null; then
        echo "  MongoDB started successfully"
        MONGO_RUNNING=true
    else
        echo "  Failed to start MongoDB"
        echo "  Check logs: $LOG_DIR/mongod_start.log"
        exit 1
    fi
fi

echo ""
echo "Step 4: Setting up authentication and database..."

# Use mongosh if available, otherwise use mongo
if command -v mongosh &> /dev/null; then
    MONGO_SHELL="mongosh"
else
    MONGO_SHELL="mongo"
fi

# Create admin user and setup database
$MONGO_SHELL --host $MONGO_HOST --port $MONGO_PORT << EOF
// Switch to admin database
use admin

// Create admin user if it doesn't exist
var adminUser = db.getUser("$MONGO_USER");
if (!adminUser) {
    print("Creating admin user...");
    db.createUser({
        user: "$MONGO_USER",
        pwd: "$MONGO_PASS",
        roles: [
            { role: "root", db: "admin" },
            { role: "userAdminAnyDatabase", db: "admin" },
            { role: "dbAdminAnyDatabase", db: "admin" },
            { role: "readWriteAnyDatabase", db: "admin" }
        ]
    });
    print("Admin user created successfully");
} else {
    print("Admin user already exists");
}

// Authenticate
db.auth("$MONGO_USER", "$MONGO_PASS");

// Create benchmark database
use $MONGO_DB

// Create collections with schema validation
print("Creating collections...");

// Patient vitals collection
db.createCollection("patient_vitals", {
    validator: {
        \$jsonSchema: {
            bsonType: "object",
            required: ["patient_id", "measurement_time", "vital_type", "vital_value"],
            properties: {
                patient_id: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                measurement_time: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                vital_type: {
                    bsonType: "string",
                    description: "must be a string and is required",
                    enum: ["heart_rate_bpm", "blood_pressure_systolic", "blood_pressure_diastolic", "temperature_celsius", "oxygen_saturation", "respiratory_rate"]
                },
                vital_value: {
                    bsonType: "double",
                    description: "must be a double and is required",
                    minimum: 0,
                    maximum: 1000
                },
                is_alert: {
                    bsonType: "bool",
                    description: "must be a boolean"
                },
                patient_department: {
                    bsonType: "string",
                    description: "must be a string",
                    enum: ["ICU", "WARD", "OUTPATIENT", "EMERGENCY", "RECOVERY"]
                },
                device_id: {
                    bsonType: "string",
                    description: "must be a string"
                },
                data_classification: {
                    bsonType: "string",
                    description: "must be a string",
                    enum: ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"]
                },
                confidence: {
                    bsonType: "double",
                    description: "must be a double",
                    minimum: 0,
                    maximum: 1
                },
                metadata: {
                    bsonType: "object",
                    description: "additional metadata"
                }
            }
        }
    },
    validationLevel: "strict",
    validationAction: "error"
});

// Audit logs collection
db.createCollection("audit_logs", {
    validator: {
        \$jsonSchema: {
            bsonType: "object",
            required: ["event_time", "event_type", "user_id"],
            properties: {
                event_time: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                event_type: {
                    bsonType: "string",
                    description: "must be a string and is required",
                    enum: ["data_access", "data_modification", "user_login", "alert_triggered"]
                },
                user_id: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                patient_id: {
                    bsonType: "string",
                    description: "must be a string"
                },
                details: {
                    bsonType: "object",
                    description: "event details"
                },
                ip_address: {
                    bsonType: "string",
                    description: "must be a string"
                }
            }
        }
    }
});

// System metrics collection
db.createCollection("system_metrics", {
    timeseries: {
        timeField: "timestamp",
        metaField: "metadata",
        granularity: "seconds"
    },
    validator: {
        \$jsonSchema: {
            bsonType: "object",
            required: ["timestamp", "metric_type", "metric_value"],
            properties: {
                timestamp: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                metric_type: {
                    bsonType: "string",
                    description: "must be a string and is required",
                    enum: ["cpu_usage", "memory_usage", "disk_io", "network_io", "query_performance"]
                },
                metric_value: {
                    bsonType: "double",
                    description: "must be a double and is required"
                },
                metadata: {
                    bsonType: "object",
                    description: "additional metadata"
                }
            }
        }
    }
});

print("Collections created successfully");

// Create indexes
print("Creating indexes...");

// Patient vitals indexes
db.patient_vitals.createIndex({ patient_id: 1 });
db.patient_vitals.createIndex({ measurement_time: -1 });
db.patient_vitals.createIndex({ vital_type: 1 });
db.patient_vitals.createIndex({ patient_department: 1 });
db.patient_vitals.createIndex({ is_alert: 1 });
db.patient_vitals.createIndex({ patient_id: 1, measurement_time: -1 });
db.patient_vitals.createIndex({ vital_type: 1, measurement_time: -1 });
db.patient_vitals.createIndex({ patient_department: 1, measurement_time: -1 });

// TTL index for automatic data expiration (optional)
// db.patient_vitals.createIndex({ measurement_time: 1 }, { expireAfterSeconds: 2592000 }); // 30 days

// Audit logs indexes
db.audit_logs.createIndex({ event_time: -1 });
db.audit_logs.createIndex({ event_type: 1 });
db.audit_logs.createIndex({ user_id: 1 });
db.audit_logs.createIndex({ patient_id: 1 });

// System metrics indexes (automatically created for timeseries)

print("Indexes created successfully");

// Create database users with different roles
print("Creating application users...");

// Read-only user
db.createUser({
    user: "health_iot_reader",
    pwd: "ReaderPass123!",
    roles: [
        { role: "read", db: "$MONGO_DB" }
    ]
});

// Read-write user
db.createUser({
    user: "health_iot_writer",
    pwd: "WriterPass123!",
    roles: [
        { role: "readWrite", db: "$MONGO_DB" }
    ]
});

// Admin user for this database
db.createUser({
    user: "health_iot_app",
    pwd: "AppPass123!",
    roles: [
        { role: "dbAdmin", db: "$MONGO_DB" },
        { role: "readWrite", db: "$MONGO_DB" }
    ]
});

print("Application users created successfully");

// Create view for recent vitals
print("Creating views...");

db.createView(
    "recent_vitals_view",
    "patient_vitals",
    [
        {
            \$match: {
                measurement_time: {
                    \$gte: new Date(Date.now() - 24 * 60 * 60 * 1000) // Last 24 hours
                }
            }
        },
        {
            \$project: {
                _id: 0,
                patient_id: 1,
                measurement_time: 1,
                vital_type: 1,
                vital_value: 1,
                is_alert: 1,
                patient_department: 1,
                hour_of_day: { \$hour: "\$measurement_time" }
            }
        },
        {
            \$sort: { measurement_time: -1 }
        }
    ]
);

print("View created successfully");

// Set up change stream (for real-time processing)
print("Setting up change stream...");

// Note: Change streams require replica set
// This is just a demonstration of the setup

print("Setup complete!");
EOF

echo ""
echo "Step 5: Creating utility scripts..."

# Create data generation script
cat > ./scripts/generate_test_data.js << 'EOF'
// MongoDB Test Data Generator
// Usage: mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! health_iot_benchmark generate_test_data.js

function generateTestData(numRecords) {
    print("Generating " + numRecords + " test records...");
    
    const vitalTypes = [
        "heart_rate_bpm",
        "blood_pressure_systolic",
        "blood_pressure_diastolic",
        "temperature_celsius",
        "oxygen_saturation",
        "respiratory_rate"
    ];
    
    const departments = ["ICU", "WARD", "OUTPATIENT", "EMERGENCY", "RECOVERY"];
    const classifications = ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"];
    
    const batchSize = 1000;
    let inserted = 0;
    
    for (let i = 0; i < numRecords; i += batchSize) {
        const currentBatchSize = Math.min(batchSize, numRecords - i);
        const batch = [];
        
        for (let j = 0; j < currentBatchSize; j++) {
            const patientNum = Math.floor(Math.random() * 1000) + 1;
            const vitalType = vitalTypes[Math.floor(Math.random() * vitalTypes.length)];
            
            // Generate realistic values based on vital type
            let vitalValue;
            switch(vitalType) {
                case "heart_rate_bpm":
                    vitalValue = 50 + Math.random() * 70; // 50-120
                    break;
                case "blood_pressure_systolic":
                    vitalValue = 90 + Math.random() * 90; // 90-180
                    break;
                case "blood_pressure_diastolic":
                    vitalValue = 60 + Math.random() * 60; // 60-120
                    break;
                case "temperature_celsius":
                    vitalValue = 36.0 + Math.random() * 2.5; // 36.0-38.5
                    break;
                case "oxygen_saturation":
                    vitalValue = 85 + Math.random() * 15; // 85-100
                    break;
                case "respiratory_rate":
                    vitalValue = 12 + Math.random() * 13; // 12-25
                    break;
                default:
                    vitalValue = 50 + Math.random() * 100;
            }
            
            const record = {
                patient_id: "PATIENT_" + patientNum.toString().padStart(5, '0'),
                measurement_time: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000), // Random time in last 30 days
                vital_type: vitalType,
                vital_value: parseFloat(vitalValue.toFixed(2)),
                is_alert: Math.random() > 0.95, // 5% chance of alert
                patient_department: departments[Math.floor(Math.random() * departments.length)],
                device_id: "DEVICE_" + (Math.floor(Math.random() * 100) + 1).toString().padStart(3, '0'),
                data_classification: classifications[Math.floor(Math.random() * classifications.length)],
                confidence: parseFloat((0.8 + Math.random() * 0.2).toFixed(2)), // 0.8-1.0
                metadata: {
                    batch_id: i,
                    generated_at: new Date()
                }
            };
            
            batch.push(record);
        }
        
        // Insert batch
        const result = db.patient_vitals.insertMany(batch);
        inserted += result.insertedCount;
        
        if (inserted % 10000 === 0) {
            print("Inserted " + inserted + " records...");
        }
    }
    
    print("Data generation complete!");
    print("Total records inserted: " + inserted);
    
    // Update statistics
    db.patient_vitals.getPlanCache().clear();
    db.runCommand({ collStats: "patient_vitals" });
}

// Generate sample data
generateTestData(100000);
EOF

# Create backup script
cat > ./scripts/backup_database.sh << 'EOF'
#!/bin/bash

# MongoDB Backup Script
set -e

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="./backups/mongodb_backup_$TIMESTAMP"
LOG_FILE="./logs/backup_$TIMESTAMP.log"

echo "Starting MongoDB backup at $(date)" | tee -a "$LOG_FILE"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Run mongodump
mongodump \
  --host localhost \
  --port 27017 \
  --username health_iot_admin \
  --password HealthIoT123! \
  --authenticationDatabase admin \
  --db health_iot_benchmark \
  --out "$BACKUP_DIR" \
  2>&1 | tee -a "$LOG_FILE"

# Check if backup was successful
if [ $? -eq 0 ]; then
    echo "Backup completed successfully" | tee -a "$LOG_FILE"
    
    # Create backup info file
    cat > "$BACKUP_DIR/backup_info.txt" << INFO
Backup Information:
- Timestamp: $TIMESTAMP
- Database: health_iot_benchmark
- Backup directory: $BACKUP_DIR
- Size: $(du -sh "$BACKUP_DIR" | cut -f1)
- Collections: $(ls "$BACKUP_DIR/health_iot_benchmark/" | wc -l)
INFO
    
    # Compress backup
    echo "Compressing backup..." | tee -a "$LOG_FILE"
    tar -czf "$BACKUP_DIR.tar.gz" -C "$BACKUP_DIR" .
    
    # Remove uncompressed backup
    rm -rf "$BACKUP_DIR"
    
    echo "Backup compressed: $BACKUP_DIR.tar.gz" | tee -a "$LOG_FILE"
    echo "Backup size: $(du -h "$BACKUP_DIR.tar.gz" | cut -f1)" | tee -a "$LOG_FILE"
else
    echo "Backup failed!" | tee -a "$LOG_FILE"
    exit 1
fi

echo "Backup completed at $(date)" | tee -a "$LOG_FILE"
EOF

chmod +x ./scripts/backup_database.sh

# Create restore script
cat > ./scripts/restore_database.sh << 'EOF'
#!/bin/bash

# MongoDB Restore Script
set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <backup_file.tar.gz>"
    exit 1
fi

BACKUP_FILE="$1"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESTORE_DIR="./restore_$TIMESTAMP"
LOG_FILE="./logs/restore_$TIMESTAMP.log"

echo "Starting MongoDB restore at $(date)" | tee -a "$LOG_FILE"

# Extract backup
echo "Extracting backup file..." | tee -a "$LOG_FILE"
mkdir -p "$RESTORE_DIR"
tar -xzf "$BACKUP_FILE" -C "$RESTORE_DIR"

# Find the actual backup directory
BACKUP_DATA_DIR=$(find "$RESTORE_DIR" -name "health_iot_benchmark" -type d | head -1)
BACKUP_DATA_DIR=$(dirname "$BACKUP_DATA_DIR")

if [ -z "$BACKUP_DATA_DIR" ]; then
    echo "Error: Could not find backup data" | tee -a "$LOG_FILE"
    exit 1
fi

echo "Restoring from: $BACKUP_DATA_DIR" | tee -a "$LOG_FILE"

# Run mongorestore
mongorestore \
  --host localhost \
  --port 27017 \
  --username health_iot_admin \
  --password HealthIoT123! \
  --authenticationDatabase admin \
  --db health_iot_benchmark \
  --drop \
  "$BACKUP_DATA_DIR" \
  2>&1 | tee -a "$LOG_FILE"

# Check if restore was successful
if [ $? -eq 0 ]; then
    echo "Restore completed successfully" | tee -a "$LOG_FILE"
    
    # Clean up
    rm -rf "$RESTORE_DIR"
    
    echo "Running post-restore tasks..." | tee -a "$LOG_FILE"
    
    # Recreate indexes
    echo "Recreating indexes..." | tee -a "$LOG_FILE"
    mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! --authenticationDatabase admin health_iot_benchmark << 'MONGO'
print("Recreating indexes...");
db.patient_vitals.createIndex({ patient_id: 1 });
db.patient_vitals.createIndex({ measurement_time: -1 });
db.patient_vitals.createIndex({ vital_type: 1 });
db.patient_vitals.createIndex({ patient_department: 1 });
db.patient_vitals.createIndex({ is_alert: 1 });
db.patient_vitals.createIndex({ patient_id: 1, measurement_time: -1 });
db.patient_vitals.createIndex({ vital_type: 1, measurement_time: -1 });
db.patient_vitals.createIndex({ patient_department: 1, measurement_time: -1 });
print("Indexes recreated successfully");
MONGO
    
    echo "Post-restore tasks completed" | tee -a "$LOG_FILE"
else
    echo "Restore failed!" | tee -a "$LOG_FILE"
    exit 1
fi

echo "Restore completed at $(date)" | tee -a "$LOG_FILE"
EOF

chmod +x ./scripts/restore_database.sh

# Create monitoring script
cat > ./scripts/monitor_database.js << 'EOF'
// MongoDB Monitoring Script
// Usage: mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! health_iot_benchmark monitor_database.js

function monitorDatabase() {
    print("=== MongoDB Database Monitoring ===");
    print("Timestamp: " + new Date());
    print("");
    
    // Database statistics
    print("1. DATABASE STATISTICS:");
    print("-".repeat(50));
    
    const dbStats = db.stats();
    print("Database: " + dbStats.db);
    print("Collections: " + dbStats.collections);
    print("Objects: " + dbStats.objects.toLocaleString());
    print("Data size: " + (dbStats.dataSize / 1024 / 1024).toFixed(2) + " MB");
    print("Storage size: " + (dbStats.storageSize / 1024 / 1024).toFixed(2) + " MB");
    print("Index size: " + (dbStats.indexSize / 1024 / 1024).toFixed(2) + " MB");
    print("");
    
    // Collection statistics
    print("2. COLLECTION STATISTICS:");
    print("-".repeat(50));
    
    const collections = db.getCollectionNames();
    collections.forEach(collectionName => {
        const collStats = db[collectionName].stats();
        print(collectionName + ":");
        print("  Documents: " + collStats.count.toLocaleString());
        print("  Size: " + (collStats.size / 1024 / 1024).toFixed(2) + " MB");
        print("  Storage: " + (collStats.storageSize / 1024 / 1024).toFixed(2) + " MB");
        print("  Indexes: " + collStats.nindexes + " (" + (collStats.totalIndexSize / 1024 / 1024).toFixed(2) + " MB)");
        print("");
    });
    
    // Index usage statistics
    print("3. INDEX USAGE STATISTICS:");
    print("-".repeat(50));
    
    const indexStats = db.patient_vitals.aggregate([
        { $indexStats: {} }
    ]).toArray();
    
    indexStats.forEach((index, i) => {
        print("Index " + (i + 1) + ": " + JSON.stringify(index.name));
        print("  Operations: " + index.ops.toLocaleString());
        print("  Since: " + new Date(index.since).toISOString());
        print("");
    });
    
    // Current operations
    print("4. CURRENT OPERATIONS:");
    print("-".repeat(50));
    
    const currentOps = db.currentOp().inprog;
    if (currentOps.length > 0) {
        currentOps.slice(0, 5).forEach(op => {
            if (op.op && op.ns) {
                print("Operation: " + op.op);
                print("Namespace: " + op.ns);
                print("Running for: " + op.secs_running + " seconds");
                print("");
            }
        });
    } else {
        print("No active operations");
        print("");
    }
    
    // Connection statistics
    print("5. CONNECTION STATISTICS:");
    print("-".repeat(50));
    
    const serverStatus = db.serverStatus();
    if (serverStatus.connections) {
        print("Current connections: " + serverStatus.connections.current);
        print("Available connections: " + serverStatus.connections.available);
        print("Total created: " + serverStatus.connections.totalCreated);
        print("");
    }
    
    // Performance metrics
    print("6. PERFORMANCE METRICS:");
    print("-".repeat(50));
    
    if (serverStatus.opcounters) {
        print("Operations per second:");
        print("  Insert: " + serverStatus.opcounters.insert);
        print("  Query: " + serverStatus.opcounters.query);
        print("  Update: " + serverStatus.opcounters.update);
        print("  Delete: " + serverStatus.opcounters.delete);
        print("  GetMore: " + serverStatus.opcounters.getmore);
        print("  Command: " + serverStatus.opcounters.command);
        print("");
    }
    
    // Memory usage
    print("7. MEMORY USAGE:");
    print("-".repeat(50));
    
    if (serverStatus.mem) {
        print("Resident: " + (serverStatus.mem.resident / 1024).toFixed(2) + " MB");
        print("Virtual: " + (serverStatus.mem.virtual / 1024).toFixed(2) + " MB");
        print("Mapped: " + (serverStatus.mem.mapped / 1024).toFixed(2) + " MB");
        print("");
    }
    
    // Recommendations
    print("8. RECOMMENDATIONS:");
    print("-".repeat(50));
    
    const recommendations = [];
    
    // Check if indexes are being used
    const unusedIndexes = indexStats.filter(index => index.ops === 0);
    if (unusedIndexes.length > 0) {
        recommendations.push("Consider removing unused indexes: " + 
            unusedIndexes.map(idx => JSON.stringify(idx.name)).join(", "));
    }
    
    // Check collection sizes
    collections.forEach(collectionName => {
        const collStats = db[collectionName].stats();
        const fragmentation = (collStats.storageSize - collStats.size) / collStats.storageSize;
        if (fragmentation > 0.3) {
            recommendations.push("High fragmentation in " + collectionName + 
                " (" + (fragmentation * 100).toFixed(1) + "%). Consider compaction.");
        }
    });
    
    if (recommendations.length > 0) {
        recommendations.forEach((rec, i) => {
            print((i + 1) + ". " + rec);
        });
    } else {
        print("No recommendations at this time.");
    }
    
    print("");
    print("=== Monitoring Complete ===");
}

// Run monitoring
monitorDatabase();
EOF

echo ""
echo "Step 6: Creating benchmark test scripts..."

# Create performance test script
cat > ./scripts/run_performance_tests.js << 'EOF'
// MongoDB Performance Test Script
// Usage: mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! health_iot_benchmark run_performance_tests.js

function runQueryWithTiming(description, queryFunc) {
    const startTime = new Date();
    const result = queryFunc();
    const endTime = new Date();
    const executionTime = endTime - startTime;
    
    return {
        description: description,
        executionTime: executionTime,
        result: result
    };
}

function runPerformanceTests() {
    print("=== MongoDB Performance Tests ===");
    print("Timestamp: " + new Date());
    print("");
    
    const results = [];
    
    // Test 1: Simple find query
    print("Test 1: Simple find query...");
    results.push(runQueryWithTiming(
        "Find recent vitals for a patient",
        () => db.patient_vitals.find({
            patient_id: "PATIENT_00001",
            measurement_time: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) }
        }).limit(100).toArray()
    ));
    
    // Test 2: Aggregation query
    print("Test 2: Aggregation query...");
    results.push(runQueryWithTiming(
        "Average vital values by type",
        () => db.patient_vitals.aggregate([
            {
                $match: {
                    measurement_time: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) }
                }
            },
            {
                $group: {
                    _id: "$vital_type",
                    avg_value: { $avg: "$vital_value" },
                    min_value: { $min: "$vital_value" },
                    max_value: { $max: "$vital_value" },
                    count: { $sum: 1 }
                }
            },
            {
                $sort: { avg_value: -1 }
            }
        ]).toArray()
    ));
    
    // Test 3: Complex aggregation with multiple stages
    print("Test 3: Complex aggregation...");
    results.push(runQueryWithTiming(
        "Department statistics with alerts",
        () => db.patient_vitals.aggregate([
            {
                $match: {
                    measurement_time: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) }
                }
            },
            {
                $group: {
                    _id: {
                        department: "$patient_department",
                        vital_type: "$vital_type"
                    },
                    avg_value: { $avg: "$vital_value" },
                    alert_count: {
                        $sum: { $cond: [{ $eq: ["$is_alert", true] }, 1, 0] }
                    },
                    total_readings: { $sum: 1 }
                }
            },
            {
                $project: {
                    department: "$_id.department",
                    vital_type: "$_id.vital_type",
                    avg_value: 1,
                    alert_count: 1,
                    total_readings: 1,
                    alert_percentage: {
                        $multiply: [
                            { $divide: ["$alert_count", "$total_readings"] },
                            100
                        ]
                    }
                }
            },
            {
                $match: {
                    alert_percentage: { $gt: 0 }
                }
            },
            {
                $sort: { alert_percentage: -1 }
            },
            {
                $limit: 10
            }
        ]).toArray()
    ));
    
    // Test 4: Text search (if text index exists)
    print("Test 4: Index scan test...");
    results.push(runQueryWithTiming(
        "Find using compound index",
        () => db.patient_vitals.find({
            patient_department: "ICU",
            vital_type: "heart_rate_bpm",
            measurement_time: {
                $gte: new Date(Date.now() - 24 * 60 * 60 * 1000),
                $lte: new Date()
            }
        })
        .sort({ measurement_time: -1 })
        .limit(100)
        .toArray()
    ));
    
    // Test 5: Write performance
    print("Test 5: Write performance test...");
    results.push(runQueryWithTiming(
        "Insert 1000 documents",
        () => {
            const documents = [];
            for (let i = 0; i < 1000; i++) {
                documents.push({
                    patient_id: "TEST_PATIENT_" + i,
                    measurement_time: new Date(),
                    vital_type: "heart_rate_bpm",
                    vital_value: 60 + Math.random() * 60,
                    is_alert: false,
                    patient_department: "TEST",
                    device_id: "TEST_DEVICE",
                    data_classification: "INTERNAL",
                    confidence: 0.9,
                    metadata: { test: true }
                });
            }
            return db.patient_vitals.insertMany(documents);
        }
    ));
    
    // Test 6: Update performance
    print("Test 6: Update performance test...");
    results.push(runQueryWithTiming(
        "Update 100 documents",
        () => db.patient_vitals.updateMany(
            { patient_id: /^TEST_PATIENT_/, is_alert: false },
            { $set: { is_alert: true, confidence: 0.95 } }
        )
    ));
    
    // Test 7: Delete performance
    print("Test 7: Delete performance test...");
    results.push(runQueryWithTiming(
        "Delete test documents",
        () => db.patient_vitals.deleteMany({ patient_id: /^TEST_PATIENT_/ })
    ));
    
    // Print results
    print("");
    print("=== TEST RESULTS ===");
    print("-".repeat(80));
    
    results.forEach((result, index) => {
        print("Test " + (index + 1) + ": " + result.description);
        print("Execution time: " + result.executionTime + " ms");
        
        if (result.result && result.result.length !== undefined) {
            print("Results returned: " + result.result.length);
        } else if (result.result && result.result.insertedCount !== undefined) {
            print("Documents inserted: " + result.result.insertedCount);
        } else if (result.result && result.result.modifiedCount !== undefined) {
            print("Documents modified: " + result.result.modifiedCount);
        } else if (result.result && result.result.deletedCount !== undefined) {
            print("Documents deleted: " + result.result.deletedCount);
        }
        
        print("");
    });
    
    // Calculate statistics
    const executionTimes = results.map(r => r.executionTime);
    const avgTime = executionTimes.reduce((a, b) => a + b, 0) / executionTimes.length;
    const maxTime = Math.max(...executionTimes);
    const minTime = Math.min(...executionTimes);
    
    print("=== PERFORMANCE SUMMARY ===");
    print("-".repeat(80));
    print("Average execution time: " + avgTime.toFixed(2) + " ms");
    print("Fastest test: " + minTime + " ms");
    print("Slowest test: " + maxTime + " ms");
    print("Total test time: " + executionTimes.reduce((a, b) => a + b, 0) + " ms");
    print("");
    
    // Save results to collection
    const testResult = {
        timestamp: new Date(),
        test_type: "performance",
        results: results.map(r => ({
            description: r.description,
            executionTime: r.executionTime
        })),
        summary: {
            avgTime: avgTime,
            minTime: minTime,
            maxTime: maxTime
        }
    };
    
    db.performance_test_results.insertOne(testResult);
    print("Results saved to performance_test_results collection");
}

// Run tests
runPerformanceTests();
EOF

echo ""
echo "Step 7: Creating verification script..."

cat > ./verify_setup.sh << 'EOF'
#!/bin/bash

echo "============================================="
echo "VERIFYING MONGODB SETUP"
echo "============================================="

# Check MongoDB service
echo ""
echo "1. Checking MongoDB service..."
if pgrep -x "mongod" > /dev/null; then
    echo "   ✓ MongoDB is running"
else
    echo "   ✗ MongoDB is not running"
    exit 1
fi

# Check connection
echo ""
echo "2. Checking database connection..."
if command -v mongosh &> /dev/null; then
    MONGO_CMD="mongosh"
else
    MONGO_CMD="mongo"
fi

if $MONGO_CMD --host localhost --port 27017 --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "   ✓ MongoDB connection successful"
else
    echo "   ✗ MongoDB connection failed"
    exit 1
fi

# Check authentication
echo ""
echo "3. Checking authentication..."
if $MONGO_CMD --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! --authenticationDatabase admin --quiet --eval "db.runCommand({connectionStatus: 1})" > /dev/null 2>&1; then
    echo "   ✓ Authentication successful"
else
    echo "   ✗ Authentication failed"
    exit 1
fi

# Check database and collections
echo ""
echo "4. Checking database and collections..."
if $MONGO_CMD --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! --authenticationDatabase admin health_iot_benchmark --quiet --eval "
    const collections = db.getCollectionNames();
    print('Database: ' + db.getName());
    print('Collections: ' + collections.length);
    collections.forEach(c => print('  • ' + c));
" 2>&1 | grep -q "patient_vitals"; then
    echo "   ✓ Database and collections exist"
else
    echo "   ✗ Database setup incomplete"
    exit 1
fi

# Check indexes
echo ""
echo "5. Checking indexes..."
INDEX_COUNT=$($MONGO_CMD --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! --authenticationDatabase admin health_iot_benchmark --quiet --eval "
    db.patient_vitals.getIndexes().length;
" 2>&1)

if [ "$INDEX_COUNT" -ge 8 ]; then
    echo "   ✓ Indexes created ($INDEX_COUNT indexes found)"
else
    echo "   ✗ Insufficient indexes ($INDEX_COUNT indexes found)"
fi

# Check data directory
echo ""
echo "6. Checking data directory..."
if [ -d "./mongo_data" ]; then
    DATA_SIZE=$(du -sh ./mongo_data 2>/dev/null | cut -f1)
    echo "   ✓ Data directory exists ($DATA_SIZE)"
else
    echo "   ✗ Data directory not found"
fi

# Check log directory
echo ""
echo "7. Checking log directory..."
if [ -d "./logs" ] && [ -f "./logs/mongodb.log" ]; then
    LOG_SIZE=$(du -h ./logs/mongodb.log 2>/dev/null | cut -f1)
    echo "   ✓ Log directory exists (log size: $LOG_SIZE)"
else
    echo "   ✗ Log directory or file not found"
fi

# Check scripts
echo ""
echo "8. Checking utility scripts..."
SCRIPTS=(
    "./scripts/generate_test_data.js"
    "./scripts/monitor_database.js"
    "./scripts/run_performance_tests.js"
    "./scripts/backup_database.sh"
    "./scripts/restore_database.sh"
)

ALL_SCRIPTS_EXIST=true
for script in "${SCRIPTS[@]}"; do
    if [ -f "$script" ]; then
        echo "   ✓ $(basename "$script") exists"
    else
        echo "   ✗ $(basename "$script") not found"
        ALL_SCRIPTS_EXIST=false
    fi
done

echo ""
echo "============================================="
echo "SETUP VERIFICATION COMPLETE"
echo "============================================="

if [ "$ALL_SCRIPTS_EXIST" = true ] && [ "$INDEX_COUNT" -ge 8 ]; then
    echo ""
    echo "✅ SETUP SUCCESSFUL"
    echo ""
    echo "Next steps:"
    echo "1. Generate test data:"
    echo "   mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! health_iot_benchmark ./scripts/generate_test_data.js"
    echo ""
    echo "2. Run performance tests:"
    echo "   mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! health_iot_benchmark ./scripts/run_performance_tests.js"
    echo ""
    echo "3. Monitor database:"
    echo "   mongo --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! health_iot_benchmark ./scripts/monitor_database.js"
    echo ""
    echo "4. Backup database:"
    echo "   ./scripts/backup_database.sh"
    echo ""
    echo "Connection string for applications:"
    echo "   mongodb://health_iot_app:AppPass123!@localhost:27017/health_iot_benchmark"
else
    echo ""
    echo "⚠️  SETUP INCOMPLETE"
    echo "   Some components may need manual setup"
fi
EOF

chmod +x ./verify_setup.sh

echo ""
echo "Step 8: Creating startup script..."

cat > ./start_mongodb.sh << 'EOF'
#!/bin/bash

# MongoDB Startup Script
set -e

echo "============================================="
echo "MONGODB STARTUP SCRIPT"
echo "============================================="

# Check if MongoDB is already running
if pgrep -x "mongod" > /dev/null; then
    echo "MongoDB is already running"
    echo "PID: $(pgrep mongod)"
else
    echo "Starting MongoDB..."
    
    # Create directories if they don't exist
    mkdir -p ./mongo_data
    mkdir -p ./logs
    
    # Start MongoDB with configuration
    mongod --config ./mongodb.conf --fork --logpath ./logs/mongod_startup.log
    
    # Wait for MongoDB to start
    echo -n "Waiting for MongoDB to start"
    for i in {1..30}; do
        if mongosh --host localhost --port 27017 --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
            echo " ✓"
            echo "MongoDB started successfully"
            break
        fi
        echo -n "."
        sleep 1
        
        if [ $i -eq 30 ]; then
            echo " ✗"
            echo "MongoDB failed to start within 30 seconds"
            echo "Check logs: ./logs/mongod_startup.log"
            exit 1
        fi
    done
fi

# Run verification
echo ""
echo "Running verification..."
./verify_setup.sh

echo ""
echo "============================================="
echo "MONGODB STARTUP COMPLETE"
echo "============================================="
echo ""
echo "To stop MongoDB:"
echo "  mongosh --host localhost --port 27017 -u health_iot_admin -p HealthIoT123! --authenticationDatabase admin --eval 'db.shutdownServer()'"
echo ""
echo "Or use: pkill mongod"
EOF

chmod +x ./start_mongodb.sh

echo ""
echo "Step 9: Setting permissions and finalizing..."

# Make all scripts executable
chmod +x ./scripts/*.sh 2>/dev/null || true

echo ""
echo "============================================="
echo "MONGODB SETUP COMPLETE!"
echo "============================================="
echo ""
echo "Setup summary:"
echo "✓ MongoDB configuration"
echo "✓ Database and collections with schema validation"
echo "✓ Indexes for optimal query performance"
echo "✓ User accounts with different privilege levels"
echo "✓ Database views for common queries"
echo "✓ Test data generation script"
echo "✓ Backup and restore scripts"
echo "✓ Monitoring script"
echo "✓ Performance test suite"
echo "✓ Verification script"
echo "✓ Startup script"
echo ""
echo "To start MongoDB:"
echo "  ./start_mongodb.sh"
echo ""
echo "To verify setup:"
echo "  ./verify_setup.sh"
echo ""
echo "Important files created:"
echo "  • ./mongodb.conf - MongoDB configuration"
echo "  • ./scripts/ - Utility scripts"
echo "  • ./mongo_data/ - Data directory"
echo "  • ./logs/ - Log directory"
echo ""
echo "Next steps:"
echo "1. Start MongoDB using the startup script"
echo "2. Verify the setup"
echo "3. Generate test data"
echo "4. Run performance tests"
echo "5. Begin benchmarking"
echo "============================================="
