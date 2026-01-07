// MongoDB Ingestion Performance Analysis
// Comprehensive analysis of data ingestion performance

// Configuration
const DATABASE_NAME = "health_iot_benchmark";
const COLLECTION_NAME = "patient_vitals";
const TEST_COLLECTION = "ingestion_performance_test";
const RESULTS_COLLECTION = "ingestion_performance_results";

// Function to test single document insertion
function testSingleDocumentInsertion(iterations = 1000) {
    print(`Testing single document insertion (${iterations} iterations)...`);
    
    const results = {
        test_type: "single_document_insertion",
        iterations: iterations,
        execution_times: [],
        total_time: 0
    };
    
    for (let i = 0; i < iterations; i++) {
        const document = {
            patient_id: `TEST_PATIENT_${i}`,
            measurement_time: new Date(),
            vital_type: "heart_rate_bpm",
            vital_value: 60 + Math.random() * 60,
            is_alert: false,
            metadata: { test: true, iteration: i }
        };
        
        const startTime = Date.now();
        db[TEST_COLLECTION].insertOne(document);
        const endTime = Date.now();
        
        results.execution_times.push(endTime - startTime);
        results.total_time += (endTime - startTime);
    }
    
    // Calculate statistics
    results.avg_time = results.total_time / iterations;
    results.min_time = Math.min(...results.execution_times);
    results.max_time = Math.max(...results.execution_times);
    results.throughput = (iterations / (results.total_time / 1000)).toFixed(2);
    
    // Save results
    db[RESULTS_COLLECTION].insertOne({
        timestamp: new Date(),
        ...results
    });
    
    print(`  Average time: ${results.avg_time.toFixed(2)} ms`);
    print(`  Throughput: ${results.throughput} documents/second`);
    
    return results;
}

// Function to test batch insertion
function testBatchInsertion(batchSizes = [100, 500, 1000, 5000]) {
    print("Testing batch insertion performance...");
    
    const results = [];
    
    batchSizes.forEach(batchSize => {
        print(`  Testing batch size: ${batchSize}`);
        
        // Generate batch
        const batch = [];
        for (let i = 0; i < batchSize; i++) {
            batch.push({
                patient_id: `BATCH_PATIENT_${i}`,
                measurement_time: new Date(),
                vital_type: "heart_rate_bpm",
                vital_value: 60 + Math.random() * 60,
                is_alert: Math.random() > 0.95,
                metadata: { test: true, batch_size: batchSize }
            });
        }
        
        // Test insertion
        const startTime = Date.now();
        const result = db[TEST_COLLECTION].insertMany(batch);
        const endTime = Date.now();
        
        const executionTime = endTime - startTime;
        const throughput = (batchSize / (executionTime / 1000)).toFixed(2);
        
        const batchResult = {
            batch_size: batchSize,
            execution_time: executionTime,
            throughput: throughput,
            documents_inserted: result.insertedCount
        };
        
        results.push(batchResult);
        
        print(`    Time: ${executionTime} ms, Throughput: ${throughput} docs/sec`);
    });
    
    // Save results
    db[RESULTS_COLLECTION].insertOne({
        timestamp: new Date(),
        test_type: "batch_insertion",
        results: results
    });
    
    return results;
}

// Function to test concurrent insertion
function testConcurrentInsertion(concurrentWriters = 5, documentsPerWriter = 1000) {
    print(`Testing concurrent insertion (${concurrentWriters} writers, ${documentsPerWriter} docs/writer)...`);
    
    // This is simplified - in real MongoDB shell, we'd need to simulate concurrency
    const results = {
        test_type: "concurrent_insertion",
        concurrent_writers: concurrentWriters,
        documents_per_writer: documentsPerWriter,
        total_documents: concurrentWriters * documentsPerWriter,
        start_time: new Date()
    };
    
    let totalInserted = 0;
    const startTime = Date.now();
    
    // Simulate concurrent writes (in reality, this would be parallel)
    for (let writer = 0; writer < concurrentWriters; writer++) {
        const batch = [];
        for (let i = 0; i < documentsPerWriter; i++) {
            batch.push({
                patient_id: `CONCURRENT_PATIENT_${writer}_${i}`,
                measurement_time: new Date(),
                vital_type: "heart_rate_bpm",
                vital_value: 60 + Math.random() * 60,
                is_alert: false,
                metadata: { 
                    test: true, 
                    writer_id: writer,
                    document_id: i 
                }
            });
        }
        
        const result = db[TEST_COLLECTION].insertMany(batch);
        totalInserted += result.insertedCount;
    }
    
    const endTime = Date.now();
    const totalTime = endTime - startTime;
    
    results.end_time = new Date();
    results.total_time_ms = totalTime;
    results.documents_inserted = totalInserted;
    results.throughput = (totalInserted / (totalTime / 1000)).toFixed(2);
    
    // Save results
    db[RESULTS_COLLECTION].insertOne(results);
    
    print(`  Total time: ${totalTime} ms`);
    print(`  Throughput: ${results.throughput} documents/second`);
    
    return results;
}

// Function to test write concern impact
function testWriteConcernImpact() {
    print("Testing write concern impact on performance...");
    
    const writeConcerns = [
        { w: 1, name: "w:1 (default)" },
        { w: "majority", name: "w:majority" },
        { w: 1, j: true, name: "w:1, j:true" },
        { w: "majority", j: true, name: "w:majority, j:true" }
    ];
    
    const results = [];
    const testDocuments = 1000;
    
    writeConcerns.forEach(wc => {
        print(`  Testing: ${wc.name}`);
        
        // Clean test collection
        db[TEST_COLLECTION].drop();
        
        // Generate test documents
        const documents = [];
        for (let i = 0; i < testDocuments; i++) {
            documents.push({
                patient_id: `WC_TEST_${i}`,
                measurement_time: new Date(),
                vital_type: "heart_rate_bpm",
                vital_value: 60 + Math.random() * 60,
                metadata: { write_concern: wc.name }
            });
        }
        
        // Test insertion with specific write concern
        const startTime = Date.now();
        const result = db[TEST_COLLECTION].insertMany(documents, { writeConcern: wc });
        const endTime = Date.now();
        
        const executionTime = endTime - startTime;
        const throughput = (testDocuments / (executionTime / 1000)).toFixed(2);
        
        results.push({
            write_concern: wc.name,
            execution_time_ms: executionTime,
            throughput_docs_sec: throughput,
            documents_inserted: result.insertedCount
        });
        
        print(`    Time: ${executionTime} ms, Throughput: ${throughput} docs/sec`);
    });
    
    // Save results
    db[RESULTS_COLLECTION].insertOne({
        timestamp: new Date(),
        test_type: "write_concern_impact",
        results: results
    });
    
    return results;
}

// Function to test index impact on write performance
function testIndexImpact() {
    print("Testing index impact on write performance...");
    
    const results = [];
    const testDocuments = 5000;
    
    // Test 1: No indexes (except _id)
    print("  Test 1: No additional indexes");
    db[TEST_COLLECTION].drop();
    
    const documents1 = [];
    for (let i = 0; i < testDocuments; i++) {
        documents1.push({
            patient_id: `INDEX_TEST_${i}`,
            measurement_time: new Date(),
            vital_type: "heart_rate_bpm",
            vital_value: 60 + Math.random() * 60,
            is_alert: false
        });
    }
    
    const startTime1 = Date.now();
    const result1 = db[TEST_COLLECTION].insertMany(documents1);
    const endTime1 = Date.now();
    
    results.push({
        test: "no_indexes",
        execution_time_ms: endTime1 - startTime1,
        throughput_docs_sec: (testDocuments / ((endTime1 - startTime1) / 1000)).toFixed(2)
    });
    
    // Test 2: With indexes
    print("  Test 2: With indexes");
    
    // Create indexes
    db[TEST_COLLECTION].createIndex({ patient_id: 1 });
    db[TEST_COLLECTION].createIndex({ measurement_time: -1 });
    db[TEST_COLLECTION].createIndex({ vital_type: 1 });
    
    const documents2 = [];
    for (let i = 0; i < testDocuments; i++) {
        documents2.push({
            patient_id: `INDEX_TEST2_${i}`,
            measurement_time: new Date(),
            vital_type: "blood_pressure_systolic",
            vital_value: 90 + Math.random() * 90,
            is_alert: Math.random() > 0.95
        });
    }
    
    const startTime2 = Date.now();
    const result2 = db[TEST_COLLECTION].insertMany(documents2);
    const endTime2 = Date.now();
    
    results.push({
        test: "with_indexes",
        execution_time_ms: endTime2 - startTime2,
        throughput_docs_sec: (testDocuments / ((endTime2 - startTime2) / 1000)).toFixed(2),
        index_count: 4 // _id + 3 created indexes
    });
    
    // Calculate impact
    const noIndexTime = results[0].execution_time_ms;
    const withIndexTime = results[1].execution_time_ms;
    const impactPercentage = ((withIndexTime - noIndexTime) / noIndexTime * 100).toFixed(2);
    
    print(`    Index overhead: ${impactPercentage}%`);
    
    // Save results
    db[RESULTS_COLLECTION].insertOne({
        timestamp: new Date(),
        test_type: "index_impact",
        results: results,
        index_overhead_percentage: impactPercentage
    });
    
    return results;
}

// Function to test bulk write operations
function testBulkWriteOperations() {
    print("Testing bulk write operations...");
    
    const operations = [];
    const operationCount = 1000;
    
    // Create mixed operations
    for (let i = 0; i < operationCount; i++) {
        // 70% inserts, 20% updates, 10% deletes
        const opType = Math.random();
        
        if (opType < 0.7) {
            // Insert
            operations.push({
                insertOne: {
                    document: {
                        patient_id: `BULK_PATIENT_${i}`,
                        measurement_time: new Date(),
                        vital_type: "heart_rate_bpm",
                        vital_value: 60 + Math.random() * 60,
                        metadata: { operation: "insert" }
                    }
                }
            });
        } else if (opType < 0.9) {
            // Update (if document exists)
            operations.push({
                updateOne: {
                    filter: { patient_id: `BULK_PATIENT_${Math.floor(Math.random() * i)}` },
                    update: { $set: { updated_at: new Date(), vital_value: 70 + Math.random() * 50 } },
                    upsert: false
                }
            });
        } else {
            // Delete (if document exists)
            operations.push({
                deleteOne: {
                    filter: { patient_id: `BULK_PATIENT_${Math.floor(Math.random() * i)}` }
                }
            });
        }
    }
    
    const startTime = Date.now();
    const result = db[TEST_COLLECTION].bulkWrite(operations, { ordered: false });
    const endTime = Date.now();
    
    const executionTime = endTime - startTime;
    const operationsPerSecond = (operationCount / (executionTime / 1000)).toFixed(2);
    
    const bulkResult = {
        test_type: "bulk_write",
        operation_count: operationCount,
        execution_time_ms: executionTime,
        operations_per_second: operationsPerSecond,
        result: {
            inserted: result.insertedCount,
            updated: result.modifiedCount,
            deleted: result.deletedCount,
            upserted: result.upsertedCount
        }
    };
    
    // Save results
    db[RESULTS_COLLECTION].insertOne({
        timestamp: new Date(),
        ...bulkResult
    });
    
    print(`  Total operations: ${operationCount}`);
    print(`  Execution time: ${executionTime} ms`);
    print(`  Throughput: ${operationsPerSecond} ops/sec`);
    print(`  Inserted: ${result.insertedCount}, Updated: ${result.modifiedCount}, Deleted: ${result.deletedCount}`);
    
    return bulkResult;
}

// Function to analyze ingestion patterns
function analyzeIngestionPatterns() {
    print("Analyzing ingestion patterns...");
    
    // Get ingestion metrics from system
    const serverStatus = db.serverStatus();
    const opCounters = serverStatus.opcounters;
    
    // Calculate ingestion rate
    const currentTime = new Date();
    const oneHourAgo = new Date(currentTime.getTime() - 60 * 60 * 1000);
    
    const recentInserts = db[COLLECTION_NAME].countDocuments({
        created_at: { $gte: oneHourAgo }
    });
    
    const ingestionRate = recentInserts / 60; // per minute
    
    const analysis = {
        timestamp: currentTime,
        analysis_type: "ingestion_patterns",
        metrics: {
            total_inserts: opCounters.insert,
            ingestion_rate_per_minute: ingestionRate,
            server_uptime: serverStatus.uptime,
            connections: serverStatus.connections
        },
        recommendations: []
    };
    
    // Generate recommendations
    if (ingestionRate > 1000) {
        analysis.recommendations.push({
            type: "high_volume",
            message: "High ingestion rate detected. Consider batching writes.",
            severity: "info"
        });
    }
    
    if (serverStatus.connections.current > 50) {
        analysis.recommendations.push({
            type: "high_connections",
            message: "High number of connections. Consider connection pooling.",
            severity: "warning"
        });
    }
    
    // Save analysis
    db[RESULTS_COLLECTION].insertOne(analysis);
    
    // Print results
    print("Ingestion Analysis:");
    print(`  Total inserts: ${opCounters.insert.toLocaleString()}`);
    print(`  Current ingestion rate: ${ingestionRate.toFixed(2)} documents/minute`);
    print(`  Server uptime: ${serverStatus.uptime} seconds`);
    print(`  Current connections: ${serverStatus.connections.current}`);
    
    if (analysis.recommendations.length > 0) {
        print("\nRecommendations:");
        analysis.recommendations.forEach(rec => {
            print(`  • [${rec.severity.toUpperCase()}] ${rec.message}`);
        });
    }
    
    return analysis;
}

// Function to run comprehensive ingestion tests
function runComprehensiveIngestionTests() {
    print("=".repeat(60));
    print("MONGODB INGESTION PERFORMANCE TESTS");
    print("=".repeat(60));
    print(`Timestamp: ${new Date()}`);
    print("");
    
    // Create collections if they don't exist
    if (!db.getCollectionNames().includes(TEST_COLLECTION)) {
        db.createCollection(TEST_COLLECTION);
    }
    
    if (!db.getCollectionNames().includes(RESULTS_COLLECTION)) {
        db.createCollection(RESULTS_COLLECTION);
    }
    
    const allResults = {};
    
    // Test 1: Single document insertion
    print("TEST 1: SINGLE DOCUMENT INSERTION");
    print("-".repeat(40));
    allResults.singleInsert = testSingleDocumentInsertion(1000);
    print("");
    
    // Test 2: Batch insertion
    print("TEST 2: BATCH INSERTION");
    print("-".repeat(40));
    allResults.batchInsert = testBatchInsertion([100, 500, 1000, 5000]);
    print("");
    
    // Test 3: Write concern impact
    print("TEST 3: WRITE CONCERN IMPACT");
    print("-".repeat(40));
    allResults.writeConcern = testWriteConcernImpact();
    print("");
    
    // Test 4: Index impact
    print("TEST 4: INDEX IMPACT ON WRITES");
    print("-".repeat(40));
    allResults.indexImpact = testIndexImpact();
    print("");
    
    // Test 5: Bulk write operations
    print("TEST 5: BULK WRITE OPERATIONS");
    print("-".repeat(40));
    allResults.bulkWrite = testBulkWriteOperations();
    print("");
    
    // Test 6: Ingestion pattern analysis
    print("TEST 6: INGESTION PATTERN ANALYSIS");
    print("-".repeat(40));
    allResults.patternAnalysis = analyzeIngestionPatterns();
    print("");
    
    // Generate summary report
    print("=".repeat(60));
    print("INGESTION PERFORMANCE SUMMARY");
    print("=".repeat(60));
    
    // Calculate overall throughput
    let totalDocuments = 0;
    let totalTime = 0;
    
    if (allResults.singleInsert) {
        totalDocuments += allResults.singleInsert.iterations;
        totalTime += allResults.singleInsert.total_time;
    }
    
    if (allResults.batchInsert) {
        allResults.batchInsert.forEach(result => {
            totalDocuments += result.documents_inserted || 0;
            totalTime += result.execution_time || 0;
        });
    }
    
    const overallThroughput = totalDocuments > 0 ? 
        (totalDocuments / (totalTime / 1000)).toFixed(2) : 0;
    
    print(`Total documents processed: ${totalDocuments.toLocaleString()}`);
    print(`Total test time: ${totalTime} ms`);
    print(`Overall throughput: ${overallThroughput} documents/second`);
    print("");
    
    // Best performing configuration
    let bestThroughput = 0;
    let bestConfig = "";
    
    if (allResults.batchInsert) {
        allResults.batchInsert.forEach(result => {
            const throughput = parseFloat(result.throughput);
            if (throughput > bestThroughput) {
                bestThroughput = throughput;
                bestConfig = `Batch size: ${result.batch_size}`;
            }
        });
    }
    
    print(`Best configuration: ${bestConfig}`);
    print(`Best throughput: ${bestThroughput.toFixed(2)} documents/second`);
    print("");
    
    // Recommendations based on tests
    print("KEY RECOMMENDATIONS:");
    print("-".repeat(40));
    
    if (allResults.writeConcern) {
        const fastestWC = allResults.writeConcern.reduce((prev, current) => 
            prev.execution_time_ms < current.execution_time_ms ? prev : current
        );
        print(`1. Use ${fastestWC.write_concern} for best write performance`);
    }
    
    if (allResults.indexImpact && allResults.indexImpact.length >= 2) {
        const overhead = parseFloat(allResults.indexImpact[1].index_overhead_percentage || 0);
        if (overhead > 20) {
            print("2. Consider reducing number of indexes (high write overhead detected)");
        }
    }
    
    if (overallThroughput < 1000) {
        print("3. Consider increasing batch sizes for better throughput");
    }
    
    print("");
    print("Results saved to: " + RESULTS_COLLECTION);
    print("=".repeat(60));
    
    return allResults;
}

// Main execution
function main() {
    try {
        // Check if we're in the right database
        if (db.getName() !== DATABASE_NAME) {
            print(`Switching to database: ${DATABASE_NAME}`);
            db = db.getSiblingDB(DATABASE_NAME);
        }
        
        // Run comprehensive tests
        const results = runComprehensiveIngestionTests();
        
        // Export results to JSON file
        const exportData = {
            timestamp: new Date(),
            database: DATABASE_NAME,
            tests_run: Object.keys(results).length,
            results: results
        };
        
        // Create export file
        const exportFileName = `ingestion_performance_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
        print(`\nExporting results to: ${exportFileName}`);
        
        // Note: In MongoDB shell, we'd typically use mongoexport
        // For this script, we'll just print the structure
        print("To export results, use:");
        print(`mongoexport --db ${DATABASE_NAME} --collection ${RESULTS_COLLECTION} --out ${exportFileName} --jsonArray`);
        
        return results;
        
    } catch (error) {
        print("Error running ingestion tests: " + error.message);
        return null;
    }
}

// Run the main function
main();