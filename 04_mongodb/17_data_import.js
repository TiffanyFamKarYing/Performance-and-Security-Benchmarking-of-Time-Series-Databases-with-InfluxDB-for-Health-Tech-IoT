// MongoDB Data Import Script
// Imports health IoT data from CSV into MongoDB

// Configuration
const DATABASE_NAME = "health_iot_benchmark";
const COLLECTION_NAME = "patient_vitals";
const BATCH_SIZE = 1000;
const CSV_FILE_PATH = "../01_dataset/health_iot_dataset.csv";

// Function to parse CSV file
function parseCSV(filePath) {
    print("Reading CSV file: " + filePath);
    
    // Note: MongoDB shell doesn't have built-in CSV parsing
    // This function reads the file line by line
    const file = cat(filePath);
    const lines = file.split("\n");
    
    // Extract headers
    const headers = lines[0].split(",").map(h => h.trim());
    
    // Parse data rows
    const data = [];
    
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        const values = line.split(",");
        const record = {};
        
        for (let j = 0; j < headers.length; j++) {
            if (j < values.length) {
                const header = headers[j];
                let value = values[j].trim();
                
                // Convert values based on header
                switch(header) {
                    case "measurement_time":
                        // Parse ISO date string
                        value = new Date(value);
                        break;
                    case "vital_value":
                    case "confidence":
                        value = parseFloat(value);
                        break;
                    case "is_alert":
                        value = value.toLowerCase() === "true" || value === "1";
                        break;
                    default:
                        // Keep as string
                        break;
                }
                
                record[header] = value;
            }
        }
        
        // Add MongoDB _id if not present
        if (!record._id) {
            record._id = new ObjectId();
        }
        
        data.push(record);
        
        // Process in batches
        if (data.length >= BATCH_SIZE) {
            importBatch(data);
            data.length = 0; // Clear array
        }
    }
    
    // Import remaining records
    if (data.length > 0) {
        importBatch(data);
    }
    
    print("CSV parsing complete");
}

// Function to import a batch of records
function importBatch(batch) {
    try {
        const result = db[COLLECTION_NAME].insertMany(batch);
        print("Imported batch: " + result.insertedCount + " documents");
    } catch (error) {
        print("Error importing batch: " + error.message);
        
        // Try importing documents one by one to identify problematic documents
        let successCount = 0;
        for (let i = 0; i < batch.length; i++) {
            try {
                db[COLLECTION_NAME].insertOne(batch[i]);
                successCount++;
            } catch (docError) {
                print("Failed to import document " + i + ": " + docError.message);
            }
        }
        print("Successfully imported " + successCount + " documents from failed batch");
    }
}

// Function to generate test data
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
    const devices = Array.from({length: 100}, (_, i) => "DEVICE_" + (i + 1).toString().padStart(3, '0'));
    const patients = Array.from({length: 1000}, (_, i) => "PATIENT_" + (i + 1).toString().padStart(5, '0'));
    
    const startTime = Date.now() - (30 * 24 * 60 * 60 * 1000); // 30 days ago
    const endTime = Date.now();
    
    let inserted = 0;
    
    for (let batchStart = 0; batchStart < numRecords; batchStart += BATCH_SIZE) {
        const batchSize = Math.min(BATCH_SIZE, numRecords - batchStart);
        const batch = [];
        
        for (let i = 0; i < batchSize; i++) {
            const patient = patients[Math.floor(Math.random() * patients.length)];
            const vitalType = vitalTypes[Math.floor(Math.random() * vitalTypes.length)];
            
            // Generate realistic values based on vital type
            let vitalValue;
            let isAlert = false;
            
            switch(vitalType) {
                case "heart_rate_bpm":
                    vitalValue = 50 + Math.random() * 70; // 50-120
                    isAlert = vitalValue < 60 || vitalValue > 100;
                    break;
                case "blood_pressure_systolic":
                    vitalValue = 90 + Math.random() * 90; // 90-180
                    isAlert = vitalValue < 100 || vitalValue > 140;
                    break;
                case "blood_pressure_diastolic":
                    vitalValue = 60 + Math.random() * 60; // 60-120
                    isAlert = vitalValue < 70 || vitalValue > 90;
                    break;
                case "temperature_celsius":
                    vitalValue = 36.0 + Math.random() * 2.5; // 36.0-38.5
                    isAlert = vitalValue < 36.0 || vitalValue > 37.5;
                    break;
                case "oxygen_saturation":
                    vitalValue = 85 + Math.random() * 15; // 85-100
                    isAlert = vitalValue < 92;
                    break;
                case "respiratory_rate":
                    vitalValue = 12 + Math.random() * 13; // 12-25
                    isAlert = vitalValue < 12 || vitalValue > 20;
                    break;
                default:
                    vitalValue = 50 + Math.random() * 100;
                    isAlert = Math.random() > 0.95; // 5% chance
            }
            
            const record = {
                patient_id: patient,
                measurement_time: new Date(startTime + Math.random() * (endTime - startTime)),
                vital_type: vitalType,
                vital_value: parseFloat(vitalValue.toFixed(2)),
                is_alert: isAlert,
                patient_department: departments[Math.floor(Math.random() * departments.length)],
                device_id: devices[Math.floor(Math.random() * devices.length)],
                data_classification: classifications[Math.floor(Math.random() * classifications.length)],
                confidence: parseFloat((0.8 + Math.random() * 0.2).toFixed(2)), // 0.8-1.0
                metadata: {
                    batch_id: batchStart,
                    generated_at: new Date(),
                    source: "test_data_generator"
                },
                created_at: new Date(),
                updated_at: new Date()
            };
            
            batch.push(record);
        }
        
        // Insert batch
        try {
            const result = db[COLLECTION_NAME].insertMany(batch);
            inserted += result.insertedCount;
            
            if (inserted % 10000 === 0) {
                print("Inserted " + inserted + " records...");
            }
        } catch (error) {
            print("Error inserting batch: " + error.message);
        }
    }
    
    print("Test data generation complete!");
    print("Total records inserted: " + inserted);
    
    // Update collection statistics
    db[COLLECTION_NAME].getPlanCache().clear();
    
    return inserted;
}

// Function to measure import performance
function measureImportPerformance(numRecords) {
    print("Measuring import performance for " + numRecords + " records...");
    
    const testSizes = [1000, 5000, 10000, 50000];
    const results = [];
    
    for (const size of testSizes) {
        if (size > numRecords) continue;
        
        print("\nTesting with " + size + " records...");
        
        // Generate test data
        const startTime = Date.now();
        const inserted = generateTestData(size);
        const endTime = Date.now();
        
        const duration = endTime - startTime;
        const recordsPerSecond = (inserted / (duration / 1000)).toFixed(2);
        
        results.push({
            records: size,
            inserted: inserted,
            duration_ms: duration,
            records_per_second: recordsPerSecond
        });
        
        print("  Duration: " + duration + " ms");
        print("  Rate: " + recordsPerSecond + " records/second");
        
        // Clean up test data
        db[COLLECTION_NAME].deleteMany({ "metadata.source": "test_data_generator" });
    }
    
    // Print performance summary
    print("\n" + "=".repeat(60));
    print("IMPORT PERFORMANCE SUMMARY");
    print("=".repeat(60));
    
    results.forEach(result => {
        print("Records: " + result.records.toString().padStart(6) + 
              " | Time: " + result.duration_ms.toString().padStart(6) + " ms" +
              " | Rate: " + parseFloat(result.records_per_second).toFixed(2).padStart(8) + " rec/sec");
    });
    
    return results;
}

// Function to validate imported data
function validateData() {
    print("Validating imported data...");
    
    const validationResults = {
        totalRecords: 0,
        validRecords: 0,
        invalidRecords: 0,
        validationErrors: []
    };
    
    // Get collection statistics
    const stats = db[COLLECTION_NAME].stats();
    validationResults.totalRecords = stats.count;
    
    // Sample validation - check schema compliance
    const sampleSize = Math.min(1000, validationResults.totalRecords);
    const sample = db[COLLECTION_NAME].find().limit(sampleSize).toArray();
    
    sample.forEach((doc, index) => {
        const errors = [];
        
        // Check required fields
        if (!doc.patient_id) errors.push("Missing patient_id");
        if (!doc.measurement_time) errors.push("Missing measurement_time");
        if (!doc.vital_type) errors.push("Missing vital_type");
        if (doc.vital_value === undefined || doc.vital_value === null) errors.push("Missing vital_value");
        
        // Check data types
        if (doc.patient_id && typeof doc.patient_id !== 'string') errors.push("patient_id must be string");
        if (doc.measurement_time && !(doc.measurement_time instanceof Date)) errors.push("measurement_time must be Date");
        if (doc.vital_type && typeof doc.vital_type !== 'string') errors.push("vital_type must be string");
        if (doc.vital_value && typeof doc.vital_value !== 'number') errors.push("vital_value must be number");
        
        // Check value ranges
        if (doc.vital_value && (doc.vital_value < 0 || doc.vital_value > 1000)) {
            errors.push("vital_value out of range (0-1000): " + doc.vital_value);
        }
        
        if (errors.length === 0) {
            validationResults.validRecords++;
        } else {
            validationResults.invalidRecords++;
            validationResults.validationErrors.push({
                document_id: doc._id,
                errors: errors
            });
        }
    });
    
    // Calculate validation statistics
    const validPercentage = (validationResults.validRecords / sampleSize * 100).toFixed(2);
    
    print("Validation Results:");
    print("  Total records in collection: " + validationResults.totalRecords.toLocaleString());
    print("  Sample size validated: " + sampleSize);
    print("  Valid records: " + validationResults.validRecords + " (" + validPercentage + "%)");
    print("  Invalid records: " + validationResults.invalidRecords);
    
    if (validationResults.validationErrors.length > 0) {
        print("  Validation errors found: " + validationResults.validationErrors.length);
        
        // Show first few errors
        const maxErrorsToShow = 5;
        validationResults.validationErrors.slice(0, maxErrorsToShow).forEach(error => {
            print("    Document " + error.document_id + ": " + error.errors.join(", "));
        });
        
        if (validationResults.validationErrors.length > maxErrorsToShow) {
            print("    ... and " + (validationResults.validationErrors.length - maxErrorsToShow) + " more");
        }
    }
    
    return validationResults;
}

// Function to create indexes for performance
function createIndexes() {
    print("Creating indexes for optimal query performance...");
    
    const indexes = [
        // Single field indexes
        { key: { patient_id: 1 }, name: "idx_patient_id" },
        { key: { measurement_time: -1 }, name: "idx_measurement_time_desc" },
        { key: { vital_type: 1 }, name: "idx_vital_type" },
        { key: { patient_department: 1 }, name: "idx_patient_department" },
        { key: { is_alert: 1 }, name: "idx_is_alert" },
        { key: { device_id: 1 }, name: "idx_device_id" },
        
        // Compound indexes
        { key: { patient_id: 1, measurement_time: -1 }, name: "idx_patient_time" },
        { key: { vital_type: 1, measurement_time: -1 }, name: "idx_vital_type_time" },
        { key: { patient_department: 1, measurement_time: -1 }, name: "idx_department_time" },
        { key: { patient_id: 1, vital_type: 1, measurement_time: -1 }, name: "idx_patient_vital_time" },
        
        // Text index for search (if needed)
        // { key: { patient_id: "text", vital_type: "text" }, name: "idx_text_search" },
        
        // TTL index for automatic data expiration (optional)
        // { key: { measurement_time: 1 }, expireAfterSeconds: 2592000, name: "idx_ttl_30days" } // 30 days
    ];
    
    let created = 0;
    let skipped = 0;
    
    indexes.forEach(indexSpec => {
        const indexName = indexSpec.name;
        
        // Check if index already exists
        const existingIndexes = db[COLLECTION_NAME].getIndexes();
        const indexExists = existingIndexes.some(idx => idx.name === indexName);
        
        if (!indexExists) {
            try {
                db[COLLECTION_NAME].createIndex(indexSpec.key, {
                    name: indexName,
                    background: true, // Create in background to avoid blocking
                    expireAfterSeconds: indexSpec.expireAfterSeconds
                });
                print("  Created index: " + indexName);
                created++;
            } catch (error) {
                print("  Error creating index " + indexName + ": " + error.message);
            }
        } else {
            print("  Index already exists: " + indexName);
            skipped++;
        }
    });
    
    print("\nIndex creation complete:");
    print("  Created: " + created + " indexes");
    print("  Skipped: " + skipped + " indexes (already exist)");
    
    // Show all indexes
    print("\nCurrent indexes:");
    const allIndexes = db[COLLECTION_NAME].getIndexes();
    allIndexes.forEach((index, i) => {
        print("  " + (i + 1) + ". " + index.name + ": " + JSON.stringify(index.key));
    });
}

// Function to analyze data distribution
function analyzeDataDistribution() {
    print("Analyzing data distribution...");
    
    const analysis = {};
    
    // Total count
    analysis.totalRecords = db[COLLECTION_NAME].countDocuments();
    
    // Count by vital type
    analysis.byVitalType = db[COLLECTION_NAME].aggregate([
        { $group: { _id: "$vital_type", count: { $sum: 1 } } },
        { $sort: { count: -1 } }
    ]).toArray();
    
    // Count by department
    analysis.byDepartment = db[COLLECTION_NAME].aggregate([
        { $group: { _id: "$patient_department", count: { $sum: 1 } } },
        { $sort: { count: -1 } }
    ]).toArray();
    
    // Alert statistics
    analysis.alertStats = db[COLLECTION_NAME].aggregate([
        { 
            $group: { 
                _id: null,
                total: { $sum: 1 },
                alerts: { $sum: { $cond: [{ $eq: ["$is_alert", true] }, 1, 0] } }
            }
        },
        {
            $project: {
                total: 1,
                alerts: 1,
                alert_percentage: { 
                    $multiply: [
                        { $divide: ["$alerts", "$total"] },
                        100
                    ]
                }
            }
        }
    ]).toArray()[0];
    
    // Time distribution (by day)
    analysis.timeDistribution = db[COLLECTION_NAME].aggregate([
        {
            $group: {
                _id: {
                    $dateToString: { format: "%Y-%m-%d", date: "$measurement_time" }
                },
                count: { $sum: 1 }
            }
        },
        { $sort: { _id: 1 } },
        { $limit: 10 } // Last 10 days
    ]).toArray();
    
    // Print analysis results
    print("\n" + "=".repeat(60));
    print("DATA DISTRIBUTION ANALYSIS");
    print("=".repeat(60));
    
    print("\nTotal records: " + analysis.totalRecords.toLocaleString());
    
    print("\nBy Vital Type:");
    analysis.byVitalType.forEach(type => {
        const percentage = (type.count / analysis.totalRecords * 100).toFixed(2);
        print("  " + type._id.padEnd(25) + ": " + 
              type.count.toLocaleString().padStart(10) + 
              " (" + percentage + "%)");
    });
    
    print("\nBy Department:");
    analysis.byDepartment.forEach(dept => {
        const percentage = (dept.count / analysis.totalRecords * 100).toFixed(2);
        print("  " + dept._id.padEnd(15) + ": " + 
              dept.count.toLocaleString().padStart(10) + 
              " (" + percentage + "%)");
    });
    
    print("\nAlert Statistics:");
    if (analysis.alertStats) {
        print("  Total alerts: " + analysis.alertStats.alerts.toLocaleString());
        print("  Alert percentage: " + analysis.alertStats.alert_percentage.toFixed(2) + "%");
    }
    
    print("\nRecent Time Distribution (last 10 days):");
    analysis.timeDistribution.forEach(day => {
        print("  " + day._id + ": " + day.count.toLocaleString().padStart(8) + " records");
    });
    
    // Size analysis
    const collStats = db[COLLECTION_NAME].stats();
    print("\nStorage Statistics:");
    print("  Data size: " + (collStats.size / 1024 / 1024).toFixed(2) + " MB");
    print("  Storage size: " + (collStats.storageSize / 1024 / 1024).toFixed(2) + " MB");
    print("  Index size: " + (collStats.totalIndexSize / 1024 / 1024).toFixed(2) + " MB");
    print("  Total size: " + ((collStats.storageSize + collStats.totalIndexSize) / 1024 / 1024).toFixed(2) + " MB");
    
    return analysis;
}

// Function to export data to JSON file
function exportDataToJSON(limit = 1000, outputFile = "exported_data.json") {
    print("Exporting data to JSON file...");
    
    try {
        // Get data
        const data = db[COLLECTION_NAME].find().limit(limit).toArray();
        
        // Convert to JSON string
        const jsonString = JSON.stringify(data, null, 2);
        
        // Write to file
        // Note: In MongoDB shell, we use print() to output
        // For file writing, we'd typically use system commands
        print("Data exported (first " + limit + " documents):");
        print("  Total documents: " + data.length);
        print("  Use mongoexport for full export:");
        print("  mongoexport --db " + DATABASE_NAME + " --collection " + COLLECTION_NAME + 
              " --out " + outputFile + " --jsonArray");
        
        // For now, just show sample
        if (data.length > 0) {
            print("\nSample document:");
            print(JSON.stringify(data[0], null, 2));
        }
        
        return data.length;
        
    } catch (error) {
        print("Error exporting data: " + error.message);
        return 0;
    }
}

// Main function to run data import
function main() {
    print("=".repeat(60));
    print("MONGODB HEALTH IOT DATA IMPORT");
    print("=".repeat(60));
    print("Database: " + DATABASE_NAME);
    print("Collection: " + COLLECTION_NAME);
    print("Timestamp: " + new Date());
    print("");
    
    // Check if collection exists and has data
    const collectionExists = db.getCollectionNames().includes(COLLECTION_NAME);
    const recordCount = collectionExists ? db[COLLECTION_NAME].countDocuments() : 0;
    
    print("Current status:");
    print("  Collection exists: " + (collectionExists ? "Yes" : "No"));
    print("  Records in collection: " + recordCount.toLocaleString());
    print("");
    
    // Menu for user selection
    print("Select an option:");
    print("  1. Import data from CSV file");
    print("  2. Generate test data");
    print("  3. Measure import performance");
    print("  4. Validate imported data");
    print("  5. Create indexes");
    print("  6. Analyze data distribution");
    print("  7. Export data to JSON");
    print("  8. Run complete import pipeline");
    print("");
    
    // For MongoDB shell, we need to read input differently
    // This is a simplified version - in practice you'd pass arguments
    const option = 8; // Default to complete pipeline
    
    switch(option) {
        case 1:
            // Import from CSV
            if (CSV_FILE_PATH) {
                parseCSV(CSV_FILE_PATH);
            } else {
                print("CSV file path not specified");
            }
            break;
            
        case 2:
            // Generate test data
            const testCount = 100000;
            generateTestData(testCount);
            break;
            
        case 3:
            // Measure import performance
            measureImportPerformance(50000);
            break;
            
        case 4:
            // Validate data
            validateData();
            break;
            
        case 5:
            // Create indexes
            createIndexes();
            break;
            
        case 6:
            // Analyze data distribution
            analyzeDataDistribution();
            break;
            
        case 7:
            // Export data
            exportDataToJSON(1000, "health_iot_export.json");
            break;
            
        case 8:
            // Complete pipeline
            print("Running complete import pipeline...");
            print("");
            
            // Step 1: Generate test data
            print("Step 1: Generating test data");
            generateTestData(100000);
            print("");
            
            // Step 2: Create indexes
            print("Step 2: Creating indexes");
            createIndexes();
            print("");
            
            // Step 3: Validate data
            print("Step 3: Validating data");
            validateData();
            print("");
            
            // Step 4: Analyze distribution
            print("Step 4: Analyzing data distribution");
            analyzeDataDistribution();
            print("");
            
            // Step 5: Measure performance
            print("Step 5: Measuring import performance");
            measureImportPerformance(50000);
            print("");
            
            print("Import pipeline completed successfully!");
            break;
            
        default:
            print("Invalid option");
    }
    
    print("");
    print("=".repeat(60));
    print("DATA IMPORT COMPLETE");
    print("=".repeat(60));
}

// Run the main function
main();