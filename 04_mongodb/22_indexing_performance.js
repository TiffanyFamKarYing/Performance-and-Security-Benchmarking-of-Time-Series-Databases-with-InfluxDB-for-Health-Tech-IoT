// MongoDB Indexing Performance Analysis
// Comprehensive index performance analysis and optimization

// Configuration
const DATABASE_NAME = "health_iot_benchmark";
const COLLECTION_NAME = "patient_vitals";
const TEST_COLLECTION = "index_performance_test";
const RESULTS_COLLECTION = "index_performance_results";

// Function to create test environment
function createTestEnvironment() {
    print("Creating test environment...");
    
    // Create test collection if it doesn't exist
    if (!db.getCollectionNames().includes(TEST_COLLECTION)) {
        db.createCollection(TEST_COLLECTION);
        print(`  Created test collection: ${TEST_COLLECTION}`);
    }
    
    // Create results collection if it doesn't exist
    if (!db.getCollectionNames().includes(RESULTS_COLLECTION)) {
        db.createCollection(RESULTS_COLLECTION);
        print(`  Created results collection: ${RESULTS_COLLECTION}`);
    }
    
    // Generate test data if collection is empty
    const testDataCount = db[TEST_COLLECTION].countDocuments();
    if (testDataCount === 0) {
        print("  Generating test data...");
        generateTestData(100000);
    }
    
    print(`  Test environment ready with ${db[TEST_COLLECTION].countDocuments().toLocaleString()} documents`);
}

// Function to generate test data
function generateTestData(numDocuments) {
    const batchSize = 1000;
    let inserted = 0;
    
    for (let i = 0; i < numDocuments; i += batchSize) {
        const currentBatchSize = Math.min(batchSize, numDocuments - i);
        const batch = [];
        
        for (let j = 0; j < currentBatchSize; j++) {
            const docId = i + j;
            batch.push({
                patient_id: `PATIENT_${(docId % 1000).toString().padStart(5, '0')}`,
                measurement_time: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000),
                vital_type: docId % 6 === 0 ? "heart_rate_bpm" :
                           docId % 6 === 1 ? "blood_pressure_systolic" :
                           docId % 6 === 2 ? "blood_pressure_diastolic" :
                           docId % 6 === 3 ? "temperature_celsius" :
                           docId % 6 === 4 ? "oxygen_saturation" : "respiratory_rate",
                vital_value: 50 + Math.random() * 100,
                is_alert: docId % 100 === 0,
                patient_department: docId % 5 === 0 ? "ICU" :
                                   docId % 5 === 1 ? "WARD" :
                                   docId % 5 === 2 ? "OUTPATIENT" :
                                   docId % 5 === 3 ? "EMERGENCY" : "RECOVERY",
                device_id: `DEVICE_${(docId % 100).toString().padStart(3, '0')}`,
                data_classification: docId % 4 === 0 ? "PUBLIC" :
                                    docId % 4 === 1 ? "INTERNAL" :
                                    docId % 4 === 2 ? "CONFIDENTIAL" : "RESTRICTED",
                confidence: 0.8 + Math.random() * 0.2,
                metadata: {
                    test: true,
                    batch: Math.floor(docId / batchSize),
                    index: docId
                }
            });
        }
        
        db[TEST_COLLECTION].insertMany(batch);
        inserted += batch.length;
        
        if (inserted % 10000 === 0) {
            print(`    Inserted ${inserted.toLocaleString()} documents...`);
        }
    }
    
    print(`  Generated ${inserted.toLocaleString()} test documents`);
    return inserted;
}

// Function to test single field index performance
function testSingleFieldIndexes() {
    print("\nTesting single field indexes...");
    
    const testFields = ['patient_id', 'measurement_time', 'vital_type', 'patient_department', 'is_alert'];
    const results = [];
    
    testFields.forEach(field => {
        print(`  Testing index on: ${field}`);
        
        // Drop existing index if it exists
        try {
            db[TEST_COLLECTION].dropIndex(`${field}_1`);
        } catch (e) {
            // Index doesn't exist, which is fine
        }
        
        // Test without index
        const query = { [field]: getTestValue(field) };
        const explainWithout = db[TEST_COLLECTION].find(query).explain("executionStats");
        
        // Create index
        db[TEST_COLLECTION].createIndex({ [field]: 1 }, { name: `${field}_idx` });
        
        // Test with index
        const explainWith = db[TEST_COLLECTION].find(query).explain("executionStats");
        
        // Analyze results
        const result = {
            field: field,
            index_name: `${field}_idx`,
            without_index: {
                execution_time_ms: explainWithout.executionStats.executionTimeMillis,
                docs_examined: explainWithout.executionStats.totalDocsExamined,
                keys_examined: explainWithout.executionStats.totalKeysExamined,
                stage: explainWithout.queryPlanner.winningPlan.stage
            },
            with_index: {
                execution_time_ms: explainWith.executionStats.executionTimeMillis,
                docs_examined: explainWith.executionStats.totalDocsExamined,
                keys_examined: explainWith.executionStats.totalKeysExamined,
                stage: explainWith.queryPlanner.winningPlan.stage
            }
        };
        
        // Calculate improvement
        if (result.without_index.execution_time_ms > 0) {
            result.improvement_factor = 
                result.without_index.execution_time_ms / result.with_index.execution_time_ms;
        }
        
        results.push(result);
        
        print(`    Without index: ${result.without_index.execution_time_ms}ms (${result.without_index.stage})`);
        print(`    With index: ${result.with_index.execution_time_ms}ms (${result.with_index.stage})`);
        
        if (result.improvement_factor) {
            print(`    Improvement: ${result.improvement_factor.toFixed(2)}x`);
        }
    });
    
    return results;
}

// Helper function to get test values for queries
function getTestValue(field) {
    switch(field) {
        case 'patient_id':
            return 'PATIENT_00500';
        case 'measurement_time':
            return { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) };
        case 'vital_type':
            return 'heart_rate_bpm';
        case 'patient_department':
            return 'ICU';
        case 'is_alert':
            return true;
        default:
            return 'test_value';
    }
}

// Function to test compound index performance
function testCompoundIndexes() {
    print("\nTesting compound indexes...");
    
    const compoundIndexes = [
        {
            name: 'patient_time_idx',
            fields: { patient_id: 1, measurement_time: -1 },
            query: { 
                patient_id: 'PATIENT_00500',
                measurement_time: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
            }
        },
        {
            name: 'dept_vital_time_idx',
            fields: { patient_department: 1, vital_type: 1, measurement_time: -1 },
            query: {
                patient_department: 'ICU',
                vital_type: 'heart_rate_bpm',
                measurement_time: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) }
            }
        },
        {
            name: 'vital_alert_time_idx',
            fields: { vital_type: 1, is_alert: 1, measurement_time: -1 },
            query: {
                vital_type: 'heart_rate_bpm',
                is_alert: true,
                measurement_time: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) }
            }
        }
    ];
    
    const results = [];
    
    compoundIndexes.forEach(indexDef => {
        print(`  Testing compound index: ${indexDef.name}`);
        print(`    Fields: ${JSON.stringify(indexDef.fields)}`);
        
        // Drop existing index if it exists
        try {
            db[TEST_COLLECTION].dropIndex(indexDef.name);
        } catch (e) {
            // Index doesn't exist
        }
        
        // Test with single field indexes (if they exist)
        const singleFieldTime = testWithSingleFieldIndexes(indexDef.query);
        
        // Create compound index
        db[TEST_COLLECTION].createIndex(indexDef.fields, { name: indexDef.name });
        
        // Test with compound index
        const explainWith = db[TEST_COLLECTION].find(indexDef.query).explain("executionStats");
        
        const result = {
            index_name: indexDef.name,
            fields: indexDef.fields,
            query: indexDef.query,
            single_field_performance: singleFieldTime,
            compound_index_performance: {
                execution_time_ms: explainWith.executionStats.executionTimeMillis,
                docs_examined: explainWith.executionStats.totalDocsExamined,
                keys_examined: explainWith.executionStats.totalKeysExamined,
                stage: explainWith.queryPlanner.winningPlan.stage
            }
        };
        
        // Calculate improvement over single field indexes
        if (singleFieldTime > 0) {
            result.improvement_over_single = singleFieldTime / result.compound_index_performance.execution_time_ms;
        }
        
        results.push(result);
        
        print(`    Single field indexes: ${singleFieldTime}ms`);
        print(`    Compound index: ${result.compound_index_performance.execution_time_ms}ms`);
        
        if (result.improvement_over_single) {
            print(`    Improvement: ${result.improvement_over_single.toFixed(2)}x`);
        }
    });
    
    return results;
}

// Helper function to test query with single field indexes
function testWithSingleFieldIndexes(query) {
    // Create single field indexes for query fields
    const fields = Object.keys(query).filter(f => !f.startsWith('$'));
    
    fields.forEach(field => {
        try {
            db[TEST_COLLECTION].createIndex({ [field]: 1 }, { name: `temp_${field}_idx` });
        } catch (e) {
            // Index might already exist
        }
    });
    
    // Test query
    const explain = db[TEST_COLLECTION].find(query).explain("executionStats");
    const executionTime = explain.executionStats.executionTimeMillis;
    
    // Clean up temporary indexes
    fields.forEach(field => {
        try {
            db[TEST_COLLECTION].dropIndex(`temp_${field}_idx`);
        } catch (e) {
            // Ignore errors
        }
    });
    
    return executionTime;
}

// Function to test index selectivity
function testIndexSelectivity() {
    print("\nTesting index selectivity...");
    
    const selectivityTests = [
        {
            field: 'patient_id',
            description: 'High selectivity (many unique values)'
        },
        {
            field: 'patient_department',
            description: 'Medium selectivity (few unique values)'
        },
        {
            field: 'is_alert',
            description: 'Low selectivity (boolean field)'
        },
        {
            field: 'vital_type',
            description: 'Medium selectivity (6 unique values)'
        }
    ];
    
    const results = [];
    
    selectivityTests.forEach(test => {
        print(`  Testing: ${test.field} - ${test.description}`);
        
        // Get cardinality
        const distinctValues = db[TEST_COLLECTION].distinct(test.field).length;
        const totalDocuments = db[TEST_COLLECTION].countDocuments();
        const selectivity = (distinctValues / totalDocuments * 100).toFixed(2);
        
        // Create index
        try {
            db[TEST_COLLECTION].dropIndex(`${test.field}_selectivity_idx`);
        } catch (e) {}
        
        db[TEST_COLLECTION].createIndex(
            { [test.field]: 1 }, 
            { name: `${test.field}_selectivity_idx` }
        );
        
        // Test query with average selectivity
        const sampleValue = getSampleValue(test.field, db[TEST_COLLECTION]);
        const query = { [test.field]: sampleValue };
        
        const explain = db[TEST_COLLECTION].find(query).explain("executionStats");
        const indexStats = db[TEST_COLLECTION].aggregate([
            { $indexStats: {} }
        ]).toArray();
        
        const indexUsage = indexStats.find(stat => stat.name === `${test.field}_selectivity_idx`);
        
        const result = {
            field: test.field,
            description: test.description,
            cardinality: {
                distinct_values: distinctValues,
                total_documents: totalDocuments,
                selectivity_percentage: selectivity
            },
            query_performance: {
                execution_time_ms: explain.executionStats.executionTimeMillis,
                docs_examined: explain.executionStats.totalDocsExamined,
                keys_examined: explain.executionStats.totalKeysExamined,
                n_returned: explain.executionStats.nReturned
            },
            index_usage: indexUsage ? indexUsage.ops : 0
        };
        
        // Categorize selectivity
        if (parseFloat(selectivity) < 1) {
            result.selectivity_category = 'HIGH';
        } else if (parseFloat(selectivity) < 10) {
            result.selectivity_category = 'MEDIUM';
        } else {
            result.selectivity_category = 'LOW';
        }
        
        results.push(result);
        
        print(`    Distinct values: ${distinctValues}`);
        print(`    Selectivity: ${selectivity}% (${result.selectivity_category})`);
        print(`    Query time: ${result.query_performance.execution_time_ms}ms`);
        print(`    Documents returned: ${result.query_performance.n_returned}`);
    });
    
    return results;
}

// Helper function to get sample value for query
function getSampleValue(field, collection) {
    const sample = collection.aggregate([
        { $match: { [field]: { $exists: true } } },
        { $sample: { size: 1 } },
        { $project: { value: `$${field}` } }
    ]).toArray();
    
    return sample.length > 0 ? sample[0].value : null;
}

// Function to test index size vs performance trade-off
function testIndexSizePerformanceTradeoff() {
    print("\nTesting index size vs performance trade-off...");
    
    const tests = [
        {
            name: 'small_index',
            fields: { is_alert: 1 },
            description: 'Small index (boolean field)'
        },
        {
            name: 'medium_index',
            fields: { patient_department: 1, vital_type: 1 },
            description: 'Medium index (two low-cardinality fields)'
        },
        {
            name: 'large_index',
            fields: { patient_id: 1, measurement_time: -1, vital_type: 1, vital_value: 1 },
            description: 'Large index (multiple fields including high-cardinality)'
        },
        {
            name: 'covering_index',
            fields: { patient_id: 1, measurement_time: -1 },
            include: { vital_type: 1, vital_value: 1 },
            description: 'Covering index with included fields'
        }
    ];
    
    const results = [];
    const collectionStats = db[TEST_COLLECTION].stats();
    
    tests.forEach(test => {
        print(`  Testing: ${test.description}`);
        
        // Drop existing index
        try {
            db[TEST_COLLECTION].dropIndex(test.name);
        } catch (e) {}
        
        // Create index
        if (test.include) {
            // Create index with included fields (MongoDB 3.2+)
            db[TEST_COLLECTION].createIndex(
                test.fields,
                { 
                    name: test.name,
                    background: true
                }
            );
        } else {
            db[TEST_COLLECTION].createIndex(
                test.fields,
                { name: test.name }
            );
        }
        
        // Get index size
        const updatedStats = db[TEST_COLLECTION].stats();
        const indexSize = updatedStats.indexSizes[test.name] || 0;
        const indexSizeMB = indexSize / (1024 * 1024);
        
        // Test query performance
        const query = createQueryForIndex(test);
        const projection = test.include ? null : { _id: 0, patient_id: 1, measurement_time: 1 };
        
        const explain = db[TEST_COLLECTION].find(query, projection).explain("executionStats");
        
        const result = {
            index_name: test.name,
            description: test.description,
            index_spec: test.fields,
            size_mb: indexSizeMB.toFixed(2),
            performance: {
                execution_time_ms: explain.executionStats.executionTimeMillis,
                docs_examined: explain.executionStats.totalDocsExamined,
                keys_examined: explain.executionStats.totalKeysExamined,
                stage: explain.queryPlanner.winningPlan.stage,
                covered: explain.executionStats.executionStages.inputStage ? 
                        explain.executionStats.executionStages.inputStage.stage === 'IXSCAN' : false
            }
        };
        
        // Calculate efficiency score (performance per MB)
        result.efficiency_score = result.performance.execution_time_ms > 0 ? 
            (1 / result.performance.execution_time_ms) * result.size_mb : 0;
        
        results.push(result);
        
        print(`    Index size: ${result.size_mb} MB`);
        print(`    Query time: ${result.performance.execution_time_ms}ms`);
        print(`    Covered query: ${result.performance.covered ? 'Yes' : 'No'}`);
        print(`    Efficiency score: ${result.efficiency_score.toFixed(4)}`);
    });
    
    return results;
}

// Helper function to create query for index test
function createQueryForIndex(test) {
    switch(test.name) {
        case 'small_index':
            return { is_alert: true };
        case 'medium_index':
            return { 
                patient_department: 'ICU',
                vital_type: 'heart_rate_bpm' 
            };
        case 'large_index':
            return {
                patient_id: 'PATIENT_00500',
                measurement_time: { $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) },
                vital_type: 'heart_rate_bpm'
            };
        case 'covering_index':
            return {
                patient_id: 'PATIENT_00500',
                measurement_time: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) }
            };
        default:
            return {};
    }
}

// Function to test partial indexes
function testPartialIndexes() {
    print("\nTesting partial indexes...");
    
    const partialIndexes = [
        {
            name: 'idx_recent_alerts',
            filter: { 
                is_alert: true,
                measurement_time: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
            },
            description: 'Index only recent alerts'
        },
        {
            name: 'idx_icu_critical',
            filter: {
                patient_department: 'ICU',
                vital_value: { $gt: 100 }
            },
            description: 'Index only critical readings in ICU'
        },
        {
            name: 'idx_restricted_data',
            filter: {
                data_classification: 'RESTRICTED'
            },
            description: 'Index only restricted data'
        }
    ];
    
    const results = [];
    
    partialIndexes.forEach(indexDef => {
        print(`  Testing: ${indexDef.description}`);
        
        // Drop existing index
        try {
            db[TEST_COLLECTION].dropIndex(indexDef.name);
        } catch (e) {}
        
        // Create partial index
        db[TEST_COLLECTION].createIndex(
            { measurement_time: -1 },
            { 
                name: indexDef.name,
                partialFilterExpression: indexDef.filter
            }
        );
        
        // Get index size
        const indexStats = db[TEST_COLLECTION].stats();
        const indexSize = indexStats.indexSizes[indexDef.name] || 0;
        const indexSizeMB = indexSize / (1024 * 1024);
        
        // Test queries that match filter
        const matchingQuery = { ...indexDef.filter };
        const nonMatchingQuery = { is_alert: false, patient_department: 'OUTPATIENT' };
        
        const explainMatch = db[TEST_COLLECTION].find(matchingQuery).explain("executionStats");
        const explainNonMatch = db[TEST_COLLECTION].find(nonMatchingQuery).explain("executionStats");
        
        const result = {
            index_name: indexDef.name,
            description: indexDef.description,
            filter: indexDef.filter,
            size_mb: indexSizeMB.toFixed(2),
            matching_query_performance: {
                execution_time_ms: explainMatch.executionStats.executionTimeMillis,
                stage: explainMatch.queryPlanner.winningPlan.stage,
                index_used: explainMatch.queryPlanner.winningPlan.stage !== 'COLLSCAN'
            },
            non_matching_query_performance: {
                execution_time_ms: explainNonMatch.executionStats.executionTimeMillis,
                stage: explainNonMatch.queryPlanner.winningPlan.stage
            },
            size_reduction_percentage: calculateSizeReduction(indexDef.filter)
        };
        
        results.push(result);
        
        print(`    Index size: ${result.size_mb} MB`);
        print(`    Matching query: ${result.matching_query_performance.execution_time_ms}ms ` +
              `(${result.matching_query_performance.index_used ? 'uses index' : 'collection scan'})`);
        print(`    Non-matching query: ${result.non_matching_query_performance.execution_time_ms}ms`);
        print(`    Estimated size reduction: ${result.size_reduction_percentage}%`);
    });
    
    return results;
}

// Helper function to calculate estimated size reduction for partial index
function calculateSizeReduction(filter) {
    // This is a simplified estimation
    // In production, you'd want to actually count matching documents
    const estimates = {
        'is_alert': 0.05, // 5% of documents are alerts
        'patient_department.ICU': 0.2, // 20% in ICU
        'vital_value.$gt.100': 0.3, // 30% above 100
        'data_classification.RESTRICTED': 0.25, // 25% restricted
        'measurement_time.$gte.30days': 1.0 // 100% in last 30 days (for test data)
    };
    
    let reduction = 1.0; // Start with full size
    
    // Parse filter to estimate reduction
    if (filter.is_alert === true) reduction *= estimates['is_alert'];
    if (filter.patient_department === 'ICU') reduction *= estimates['patient_department.ICU'];
    if (filter.vital_value && filter.vital_value.$gt === 100) reduction *= estimates['vital_value.$gt.100'];
    if (filter.data_classification === 'RESTRICTED') reduction *= estimates['data_classification.RESTRICTED'];
    
    return ((1 - reduction) * 100).toFixed(2);
}

// Function to test text search indexes
function testTextSearchIndexes() {
    print("\nTesting text search indexes...");
    
    // Add some text fields for testing
    addTextFieldsForTesting();
    
    const results = [];
    
    // Create text index
    try {
        db[TEST_COLLECTION].dropIndex('text_index');
    } catch (e) {}
    
    print("  Creating text index on notes field...");
    db[TEST_COLLECTION].createIndex(
        { notes: "text", patient_id: "text" },
        { 
            name: 'text_index',
            weights: { notes: 10, patient_id: 5 },
            default_language: 'english'
        }
    );
    
    // Test text searches
    const textSearches = [
        { search: 'critical', description: 'Search for "critical"' },
        { search: 'normal', description: 'Search for "normal"' },
        { search: 'PATIENT_005', description: 'Search for patient ID' },
        { search: '"high temperature"', description: 'Phrase search' }
    ];
    
    textSearches.forEach(searchTest => {
        print(`  Testing: ${searchTest.description}`);
        
        const query = { $text: { $search: searchTest.search } };
        
        const startTime = Date.now();
        const cursor = db[TEST_COLLECTION].find(query);
        const resultsCount = cursor.count();
        const searchResults = cursor.limit(10).toArray();
        const endTime = Date.now();
        
        const result = {
            search: searchTest.search,
            description: searchTest.description,
            performance: {
                execution_time_ms: endTime - startTime,
                results_count: resultsCount
            },
            sample_results: searchResults.length
        };
        
        results.push(result);
        
        print(`    Execution time: ${result.performance.execution_time_ms}ms`);
        print(`    Results found: ${result.performance.results_count}`);
    });
    
    return results;
}

// Helper function to add text fields for testing
function addTextFieldsForTesting() {
    // Add notes field to some documents for text search testing
    const updateCount = db[TEST_COLLECTION].updateMany(
        { metadata: { $exists: true } },
        [
            {
                $set: {
                    notes: {
                        $switch: {
                            branches: [
                                { 
                                    case: { $gt: ["$vital_value", 100] }, 
                                    then: "Critical reading requiring immediate attention" 
                                },
                                { 
                                    case: { $and: [
                                        { $gte: ["$vital_value", 80] },
                                        { $lte: ["$vital_value", 100] }
                                    ]}, 
                                    then: "Normal reading within acceptable range" 
                                },
                                { 
                                    case: { $lt: ["$vital_value", 80] }, 
                                    then: "Low reading monitor closely" 
                                }
                            ],
                            default: "Reading requires review"
                        }
                    }
                }
            }
        ]
    );
    
    print(`    Added text fields to ${updateCount.modifiedCount} documents`);
}

// Function to analyze index usage patterns
function analyzeIndexUsagePatterns() {
    print("\nAnalyzing index usage patterns...");
    
    const indexStats = db[TEST_COLLECTION].aggregate([{ $indexStats: {} }]).toArray();
    const collectionStats = db[TEST_COLLECTION].stats();
    
    const analysis = {
        total_indexes: indexStats.length,
        index_usage: [],
        recommendations: []
    };
    
    // Analyze each index
    indexStats.forEach(stat => {
        const indexName = stat.name;
        const indexSize = (collectionStats.indexSizes[indexName] || 0) / (1024 * 1024);
        
        const usage = {
            name: indexName,
            operations: stat.ops,
            since: stat.since,
            last_access: stat.accesses && stat.accesses.ops ? 
                new Date(stat.accesses.ops) : 'Never',
            size_mb: indexSize.toFixed(2),
            efficiency: stat.ops > 0 ? (stat.ops / indexSize) : 0
        };
        
        analysis.index_usage.push(usage);
        
        // Generate recommendations
        if (stat.ops === 0 && !indexName.startsWith('_id_')) {
            analysis.recommendations.push({
                type: 'unused_index',
                index: indexName,
                size_mb: indexSize.toFixed(2),
                message: `Index "${indexName}" has never been used`,
                recommendation: 'Consider removing this index'
            });
        }
        
        if (indexSize > 100 && stat.ops < 1000) {
            analysis.recommendations.push({
                type: 'large_low_usage_index',
                index: indexName,
                size_mb: indexSize.toFixed(2),
                operations: stat.ops,
                message: `Large index "${indexName}" (${indexSize.toFixed(2)} MB) has low usage`,
                recommendation: 'Evaluate if this index is necessary'
            });
        }
    });
    
    // Print analysis
    print("Index Usage Analysis:");
    print("-".repeat(80));
    
    print("Index\t\t\tSize (MB)\tOperations\tLast Access\t\tEfficiency");
    print("-".repeat(80));
    
    analysis.index_usage.forEach(usage => {
        const lastAccess = usage.last_access instanceof Date ? 
            usage.last_access.toISOString().split('T')[0] : usage.last_access;
        
        print(`${usage.name.padEnd(25)}${usage.size_mb.padStart(10)}${usage.operations.toString().padStart(15)}` +
              `${lastAccess.padStart(20)}${usage.efficiency.toFixed(2).padStart(15)}`);
    });
    
    if (analysis.recommendations.length > 0) {
        print("\nRecommendations:");
        analysis.recommendations.forEach(rec => {
            print(`  • ${rec.message}`);
            print(`    ${rec.recommendation}`);
        });
    }
    
    return analysis;
}

// Function to run comprehensive index performance tests
function runComprehensiveIndexTests() {
    print("=".repeat(60));
    print("MONGODB INDEX PERFORMANCE ANALYSIS");
    print("=".repeat(60));
    print(`Database: ${DATABASE_NAME}`);
    print(`Test Collection: ${TEST_COLLECTION}`);
    print(`Timestamp: ${new Date()}`);
    print("");
    
    // Create test environment
    createTestEnvironment();
    
    const testResults = {};
    
    // Run all tests
    print("\n" + "=".repeat(60));
    print("RUNNING INDEX PERFORMANCE TESTS");
    print("=".repeat(60));
    
    // Test 1: Single field indexes
    print("\nTEST 1: SINGLE FIELD INDEXES");
    print("-".repeat(40));
    testResults.singleFieldIndexes = testSingleFieldIndexes();
    
    // Test 2: Compound indexes
    print("\nTEST 2: COMPOUND INDEXES");
    print("-".repeat(40));
    testResults.compoundIndexes = testCompoundIndexes();
    
    // Test 3: Index selectivity
    print("\nTEST 3: INDEX SELECTIVITY");
    print("-".repeat(40));
    testResults.indexSelectivity = testIndexSelectivity();
    
    // Test 4: Index size vs performance
    print("\nTEST 4: INDEX SIZE VS PERFORMANCE");
    print("-".repeat(40));
    testResults.sizePerformance = testIndexSizePerformanceTradeoff();
    
    // Test 5: Partial indexes
    print("\nTEST 5: PARTIAL INDEXES");
    print("-".repeat(40));
    testResults.partialIndexes = testPartialIndexes();
    
    // Test 6: Text search indexes
    print("\nTEST 6: TEXT SEARCH INDEXES");
    print("-".repeat(40));
    testResults.textIndexes = testTextSearchIndexes();
    
    // Test 7: Index usage analysis
    print("\nTEST 7: INDEX USAGE ANALYSIS");
    print("-".repeat(40));
    testResults.indexUsage = analyzeIndexUsagePatterns();
    
    // Generate summary report
    print("\n" + "=".repeat(60));
    print("INDEX PERFORMANCE SUMMARY");
    print("=".repeat(60));
    
    const summary = {
        timestamp: new Date(),
        test_collection: TEST_COLLECTION,
        document_count: db[TEST_COLLECTION].countDocuments(),
        total_indexes: testResults.indexUsage.total_indexes,
        test_results: testResults
    };
    
    // Calculate overall statistics
    let totalImprovement = 0;
    let testCount = 0;
    
    // From single field indexes
    testResults.singleFieldIndexes.forEach(test => {
        if (test.improvement_factor) {
            totalImprovement += test.improvement_factor;
            testCount++;
        }
    });
    
    // From compound indexes
    testResults.compoundIndexes.forEach(test => {
        if (test.improvement_over_single) {
            totalImprovement += test.improvement_over_single;
            testCount++;
        }
    });
    
    const avgImprovement = testCount > 0 ? (totalImprovement / testCount).toFixed(2) : 0;
    
    print(`Average Performance Improvement: ${avgImprovement}x`);
    print(`Total Indexes Analyzed: ${summary.total_indexes}`);
    
    // Identify best performing indexes
    const bestIndexes = [];
    
    // Find indexes with highest efficiency
    testResults.sizePerformance.forEach(test => {
        if (test.efficiency_score > 0.1) { // Threshold for "good" efficiency
            bestIndexes.push({
                                name: test.index_name,
                efficiency: test.efficiency_score.toFixed(4),
                size_mb: test.size_mb,
                performance: test.performance.execution_time_ms
            });
        }
    });
    
    // Print best indexes
    if (bestIndexes.length > 0) {
        print("\nBest Performing Indexes:");
        print("Name\t\t\tEfficiency Score\tSize (MB)\tPerformance (ms)");
        print("-".repeat(80));
        bestIndexes.forEach(idx => {
            print(`${idx.name.padEnd(25)}${idx.efficiency.padStart(15)}${idx.size_mb.padStart(15)}${idx.performance.toString().padStart(20)}`);
        });
    }
    
    // Generate optimization recommendations
    generateOptimizationRecommendations(testResults);
    
    // Save results to database
    saveResultsToDatabase(summary);
    
    // Export results to file
    exportResultsToFile(summary);
    
    return summary;
}

// Function to generate optimization recommendations
function generateOptimizationRecommendations(testResults) {
    print("\n" + "=".repeat(60));
    print("OPTIMIZATION RECOMMENDATIONS");
    print("=".repeat(60));
    
    const recommendations = [];
    
    // 1. Index creation recommendations
    recommendations.push({
        category: "INDEX CREATION",
        items: [
            "1. Create compound indexes for frequently queried field combinations",
            "2. Use descending indexes for time-series data (measurement_time: -1)",
            "3. Consider partial indexes for filtered queries",
            "4. Ensure indexes cover frequently accessed fields"
        ]
    });
    
    // 2. Index maintenance recommendations
    recommendations.push({
        category: "INDEX MAINTENANCE",
        items: [
            "1. Regularly monitor index usage with $indexStats",
            "2. Remove unused indexes to save storage and improve write performance",
            "3. Rebuild fragmented indexes periodically",
            "4. Monitor index size growth"
        ]
    });
    
    // 3. Query optimization recommendations
    recommendations.push({
        category: "QUERY OPTIMIZATION",
        items: [
            "1. Use covered queries when possible (project only indexed fields)",
            "2. Avoid queries that cannot use indexes effectively",
            "3. Use explain() to analyze query plans",
            "4. Consider query selectivity when designing indexes"
        ]
    });
    
    // 4. Specific recommendations based on test results
    const specificRecs = [];
    
    // Analyze single field index results
    testResults.singleFieldIndexes.forEach(test => {
        if (test.improvement_factor && test.improvement_factor > 10) {
            specificRecs.push(`High impact index: ${test.field} (${test.improvement_factor.toFixed(2)}x improvement)`);
        }
    });
    
    // Analyze compound index results
    testResults.compoundIndexes.forEach(test => {
        if (test.improvement_over_single && test.improvement_over_single > 2) {
            specificRecs.push(`Effective compound index: ${test.index_name}`);
        }
    });
    
    // Analyze index usage
    if (testResults.indexUsage.recommendations) {
        testResults.indexUsage.recommendations.forEach(rec => {
            specificRecs.push(`${rec.type}: ${rec.index} - ${rec.message}`);
        });
    }
    
    if (specificRecs.length > 0) {
        recommendations.push({
            category: "SPECIFIC RECOMMENDATIONS",
            items: specificRecs
        });
    }
    
    // Print all recommendations
    recommendations.forEach(rec => {
        print(`\n${rec.category}:`);
        print("-".repeat(40));
        rec.items.forEach((item, index) => {
            print(`  ${item}`);
        });
    });
    
    return recommendations;
}

// Function to save results to database
function saveResultsToDatabase(summary) {
    print("\nSaving results to database...");
    
    try {
        // Clear previous results
        db[RESULTS_COLLECTION].deleteMany({});
        
        // Save summary
        db[RESULTS_COLLECTION].insertOne({
            type: "summary",
            data: summary,
            created_at: new Date()
        });
        
        // Save individual test results
        Object.keys(summary.test_results).forEach(testType => {
            if (testType !== 'indexUsage') { // Skip indexUsage as it's not an array
                const testData = summary.test_results[testType];
                if (Array.isArray(testData)) {
                    testData.forEach((result, index) => {
                        db[RESULTS_COLLECTION].insertOne({
                            type: "test_result",
                            test_type: testType,
                            test_index: index,
                            data: result,
                            created_at: new Date()
                        });
                    });
                } else {
                    db[RESULTS_COLLECTION].insertOne({
                        type: "test_result",
                        test_type: testType,
                        data: testData,
                        created_at: new Date()
                    });
                }
            }
        });
        
        print(`  Saved results to ${RESULTS_COLLECTION} collection`);
        print(`  Total documents saved: ${db[RESULTS_COLLECTION].countDocuments()}`);
        
    } catch (error) {
        print(`  Error saving results: ${error.message}`);
    }
}

// Function to export results to file
function exportResultsToFile(summary) {
    print("\nExporting results to files...");
    
    try {
        // Create output directory
        const fs = require('fs');
        const path = './outputs/mongodb_indexing/';
        
        if (!fs.existsSync(path)) {
            fs.mkdirSync(path, { recursive: true });
        }
        
        // 1. Export summary as JSON
        const summaryFile = `${path}indexing_summary_${Date.now()}.json`;
        fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2));
        print(`  Summary exported to: ${summaryFile}`);
        
        // 2. Export recommendations as text
        const recFile = `${path}indexing_recommendations_${Date.now()}.txt`;
        const recommendations = generateOptimizationRecommendations(summary.test_results);
        
        let recText = "MONGODB INDEXING OPTIMIZATION RECOMMENDATIONS\n";
        recText += "=".repeat(60) + "\n\n";
        recText += `Generated: ${new Date()}\n`;
        recText += `Database: ${DATABASE_NAME}\n`;
        recText += `Collection: ${TEST_COLLECTION}\n`;
        recText += `Document Count: ${summary.document_count.toLocaleString()}\n`;
        recText += `Total Indexes: ${summary.total_indexes}\n\n`;
        
        recommendations.forEach(rec => {
            recText += `${rec.category}:\n`;
            recText += "-".repeat(40) + "\n";
            rec.items.forEach(item => {
                recText += `• ${item}\n`;
            });
            recText += "\n";
        });
        
        fs.writeFileSync(recFile, recText);
        print(`  Recommendations exported to: ${recFile}`);
        
        // 3. Export detailed test results as CSV
        exportDetailedResultsToCSV(summary.test_results, path);
        
    } catch (error) {
        print(`  Error exporting files: ${error.message}`);
    }
}

// Function to export detailed results to CSV
function exportDetailedResultsToCSV(testResults, path) {
    try {
        const fs = require('fs');
        const timestamp = Date.now();
        
        // Export single field index results
        if (testResults.singleFieldIndexes && testResults.singleFieldIndexes.length > 0) {
            const singleFieldCSV = `${path}single_field_indexes_${timestamp}.csv`;
            let csvContent = "field,index_name,without_index_ms,with_index_ms,improvement_factor,docs_examined_without,keys_examined_without,docs_examined_with,keys_examined_with,stage_without,stage_with\n";
            
            testResults.singleFieldIndexes.forEach(test => {
                csvContent += `"${test.field}","${test.index_name}",${test.without_index.execution_time_ms},${test.with_index.execution_time_ms},${test.improvement_factor || 0},${test.without_index.docs_examined},${test.without_index.keys_examined},${test.with_index.docs_examined},${test.with_index.keys_examined},"${test.without_index.stage}","${test.with_index.stage}"\n`;
            });
            
            fs.writeFileSync(singleFieldCSV, csvContent);
            print(`  Single field indexes exported to: ${singleFieldCSV}`);
        }
        
        // Export compound index results
        if (testResults.compoundIndexes && testResults.compoundIndexes.length > 0) {
            const compoundCSV = `${path}compound_indexes_${timestamp}.csv`;
            let csvContent = "index_name,fields,single_field_ms,compound_index_ms,improvement_over_single,docs_examined,keys_examined,stage\n";
            
            testResults.compoundIndexes.forEach(test => {
                const fields = JSON.stringify(test.fields).replace(/"/g, '""');
                csvContent += `"${test.index_name}","${fields}",${test.single_field_performance},${test.compound_index_performance.execution_time_ms},${test.improvement_over_single || 0},${test.compound_index_performance.docs_examined},${test.compound_index_performance.keys_examined},"${test.compound_index_performance.stage}"\n`;
            });
            
            fs.writeFileSync(compoundCSV, csvContent);
            print(`  Compound indexes exported to: ${compoundCSV}`);
        }
        
        // Export size-performance results
        if (testResults.sizePerformance && testResults.sizePerformance.length > 0) {
            const sizePerfCSV = `${path}size_performance_${timestamp}.csv`;
            let csvContent = "index_name,description,size_mb,execution_time_ms,docs_examined,keys_examined,stage,covered,efficiency_score\n";
            
            testResults.sizePerformance.forEach(test => {
                csvContent += `"${test.index_name}","${test.description}",${test.size_mb},${test.performance.execution_time_ms},${test.performance.docs_examined},${test.performance.keys_examined},"${test.performance.stage}",${test.performance.covered},${test.efficiency_score || 0}\n`;
            });
            
            fs.writeFileSync(sizePerfCSV, csvContent);
            print(`  Size-performance results exported to: ${sizePerfCSV}`);
        }
        
    } catch (error) {
        print(`  Error exporting CSV files: ${error.message}`);
    }
}

// Function to clean up test environment
function cleanupTestEnvironment() {
    print("\nCleaning up test environment...");
    
    try {
        // Drop test collection
        db[TEST_COLLECTION].drop();
        print(`  Dropped test collection: ${TEST_COLLECTION}`);
        
        // Note: Not dropping results collection to preserve results
        
        print("  Cleanup completed");
    } catch (error) {
        print(`  Error during cleanup: ${error.message}`);
    }
}

// Function to run specific tests only
function runSpecificTests(testTypes) {
    const results = {};
    
    if (testTypes.includes('single')) {
        print("\nRunning single field index tests...");
        results.singleFieldIndexes = testSingleFieldIndexes();
    }
    
    if (testTypes.includes('compound')) {
        print("\nRunning compound index tests...");
        results.compoundIndexes = testCompoundIndexes();
    }
    
    if (testTypes.includes('selectivity')) {
        print("\nRunning index selectivity tests...");
        results.indexSelectivity = testIndexSelectivity();
    }
    
    if (testTypes.includes('size')) {
        print("\nRunning size-performance tests...");
        results.sizePerformance = testIndexSizePerformanceTradeoff();
    }
    
    if (testTypes.includes('partial')) {
        print("\nRunning partial index tests...");
        results.partialIndexes = testPartialIndexes();
    }
    
    if (testTypes.includes('text')) {
        print("\nRunning text search index tests...");
        results.textIndexes = testTextSearchIndexes();
    }
    
    if (testTypes.includes('usage')) {
        print("\nRunning index usage analysis...");
        results.indexUsage = analyzeIndexUsagePatterns();
    }
    
    return results;
}

// Main execution function
function main() {
    try {
        // Switch to target database
        db = db.getSiblingDB(DATABASE_NAME);
        
        print(`Connected to database: ${DATABASE_NAME}`);
        
        // Parse command line arguments
        const args = process.argv.slice(2);
        const testTypes = args.length > 0 ? args : ['all'];
        
        if (testTypes[0] === 'all') {
            // Run comprehensive tests
            const results = runComprehensiveIndexTests();
            return results;
        } else {
            // Run specific tests
            print(`Running specific tests: ${testTypes.join(', ')}`);
            const results = runSpecificTests(testTypes);
            
            // Save partial results
            const summary = {
                timestamp: new Date(),
                test_collection: TEST_COLLECTION,
                document_count: db[TEST_COLLECTION].countDocuments(),
                test_results: results
            };
            
            saveResultsToDatabase(summary);
            exportResultsToFile(summary);
            
            return results;
        }
        
    } catch (error) {
        print(`Error in main execution: ${error.message}`);
        print(error.stack);
        return null;
    }
}

// Cleanup function (optional)
function cleanup() {
    const args = process.argv.slice(2);
    
    if (args.includes('--cleanup')) {
        cleanupTestEnvironment();
        return true;
    }
    
    return false;
}

// Execute cleanup if requested
if (cleanup()) {
    quit();
}

// Execute main function
const results = main();

// Return success
print("\nIndexing performance analysis completed successfully!");
print(`Results saved in collection: ${RESULTS_COLLECTION}`);
print(`Output files saved in: ./outputs/mongodb_indexing/`);

quit();
