// MongoDB Query Performance Analysis
// Comprehensive analysis of query performance and optimization

// Configuration
const DATABASE_NAME = "health_iot_benchmark";
const COLLECTION_NAME = "patient_vitals";
const RESULTS_COLLECTION = "query_performance_results";

// Function to run query with timing and explain
function runQueryWithAnalysis(description, query, options = {}) {
    print(`Running query: ${description}`);
    
    const startTime = Date.now();
    const cursor = db[COLLECTION_NAME].find(query.filter || {}, query.projection || {});
    
    // Apply options
    if (options.sort) cursor.sort(options.sort);
    if (options.limit) cursor.limit(options.limit);
    if (options.skip) cursor.skip(options.skip);
    
    const results = cursor.toArray();
    const endTime = Date.now();
    
    const executionTime = endTime - startTime;
    const resultCount = results.length;
    
    // Get query execution plan
    const explainResult = db[COLLECTION_NAME].find(query.filter || {}, query.projection || {})
        .explain("executionStats");
    
    // Extract key metrics from explain
    const executionStats = explainResult.executionStats;
    const winningPlan = explainResult.queryPlanner.winningPlan;
    
    const analysis = {
        description: description,
        execution_time_ms: executionTime,
        result_count: resultCount,
        documents_examined: executionStats.totalDocsExamined,
        keys_examined: executionStats.totalKeysExamined,
        index_used: executionStats.executionStages.stage !== "COLLSCAN",
        stage: winningPlan.stage,
        input_stage: winningPlan.inputStage ? winningPlan.inputStage.stage : null
    };
    
    return analysis;
}

// Function to test different query types
function testQueryTypes() {
    print("Testing different query types...");
    
    const tests = [
        {
            description: "Simple equality query",
            query: {
                filter: { patient_department: "ICU" },
                projection: { patient_id: 1, vital_type: 1, vital_value: 1 }
            },
            options: { limit: 100 }
        },
        {
            description: "Range query with date filter",
            query: {
                filter: { 
                    measurement_time: { 
                        $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) 
                    } 
                }
            },
            options: { sort: { measurement_time: -1 }, limit: 100 }
        },
        {
            description: "Compound query with multiple conditions",
            query: {
                filter: { 
                    patient_department: "ICU",
                    vital_type: "heart_rate_bpm",
                    is_alert: true
                }
            }
        },
        {
            description: "Query with $in operator",
            query: {
                filter: { 
                    vital_type: { $in: ["heart_rate_bpm", "blood_pressure_systolic"] }
                }
            },
            options: { limit: 100 }
        },
        {
            description: "Query with $or operator",
            query: {
                filter: {
                    $or: [
                        { vital_value: { $gt: 100 } },
                        { is_alert: true }
                    ]
                }
            }
        },
        {
            description: "Text search query (if text index exists)",
            query: {
                filter: { $text: { $search: "heart" } }
            }
        }
    ];
    
    const results = [];
    
    tests.forEach(test => {
        try {
            const analysis = runQueryWithAnalysis(
                test.description,
                test.query,
                test.options
            );
            results.push(analysis);
            
            print(`  ${test.description}:`);
            print(`    Time: ${analysis.execution_time_ms} ms`);
            print(`    Results: ${analysis.result_count}`);
            print(`    Docs examined: ${analysis.documents_examined}`);
            print(`    Index used: ${analysis.index_used ? "Yes" : "No"}`);
        } catch (error) {
            print(`  Error in ${test.description}: ${error.message}`);
        }
    });
    
    return results;
}

// Function to test aggregation performance
function testAggregationPerformance() {
    print("Testing aggregation performance...");
    
    const aggregations = [
        {
            description: "Simple group by vital type",
            pipeline: [
                { 
                    $match: { 
                        measurement_time: { 
                            $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) 
                        } 
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
                { $sort: { avg_value: -1 } }
            ]
        },
        {
            description: "Complex aggregation with multiple stages",
            pipeline: [
                { 
                    $match: { 
                        measurement_time: { 
                            $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) 
                        } 
                    } 
                },
                { 
                    $group: { 
                        _id: {
                            department: "$patient_department",
                            hour: { $hour: "$measurement_time" }
                        },
                        avg_value: { $avg: "$vital_value" },
                        readings: { $sum: 1 }
                    } 
                },
                { 
                    $project: {
                        department: "$_id.department",
                        hour: "$_id.hour",
                        avg_value: 1,
                        readings: 1,
                        _id: 0
                    }
                },
                { $sort: { department: 1, hour: 1 } }
            ]
        },
        {
            description: "Aggregation with $lookup (simulated join)",
            pipeline: [
                { $match: { is_alert: true } },
                { $limit: 100 },
                {
                    $lookup: {
                        from: "audit_logs",
                        localField: "patient_id",
                        foreignField: "patient_id",
                        as: "audit_entries"
                    }
                },
                {
                    $project: {
                        patient_id: 1,
                        vital_type: 1,
                        vital_value: 1,
                        audit_count: { $size: "$audit_entries" }
                    }
                }
            ]
        },
        {
            description: "Aggregation with $facet for multiple aggregations",
            pipeline: [
                { 
                    $match: { 
                        measurement_time: { 
                            $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) 
                        } 
                    } 
                },
                {
                    $facet: {
                        "by_department": [
                            { $group: { _id: "$patient_department", count: { $sum: 1 } } },
                            { $sort: { count: -1 } }
                        ],
                        "by_vital_type": [
                            { $group: { _id: "$vital_type", avg_value: { $avg: "$vital_value" } } }
                        ],
                        "alerts_summary": [
                            { $match: { is_alert: true } },
                            { $group: { _id: null, alert_count: { $sum: 1 } } }
                        ]
                    }
                }
            ]
        }
    ];
    
    const results = [];
    
    aggregations.forEach(agg => {
        print(`  ${agg.description}...`);
        
        const startTime = Date.now();
        const cursor = db[COLLECTION_NAME].aggregate(agg.pipeline);
        const aggResults = cursor.toArray();
        const endTime = Date.now();
        
        const executionTime = endTime - startTime;
        const resultCount = aggResults.length;
        
        // Get explain for aggregation
        const explainResult = db[COLLECTION_NAME].aggregate(agg.pipeline).explain();
        
        const analysis = {
            description: agg.description,
            execution_time_ms: executionTime,
            result_count: resultCount,
            pipeline_stages: agg.pipeline.length,
            explain: explainResult
        };
        
        results.push(analysis);
        
        print(`    Time: ${executionTime} ms`);
        print(`    Results: ${resultCount}`);
        print(`    Pipeline stages: ${agg.pipeline.length}`);
    });
    
    return results;
}

// Function to test index effectiveness
function testIndexEffectiveness() {
    print("Testing index effectiveness...");
    
    const indexTests = [
        {
            description: "Query without index (collection scan)",
            query: { vital_value: { $gt: 100 } },
            createIndex: false
        },
        {
            description: "Query with single field index",
            query: { vital_value: { $gt: 100 } },
            createIndex: true,
            index: { vital_value: 1 }
        },
        {
            description: "Query with compound index",
            query: { 
                patient_department: "ICU",
                vital_type: "heart_rate_bpm" 
            },
            createIndex: true,
            index: { patient_department: 1, vital_type: 1 }
        },
        {
            description: "Query with covered index",
            query: { patient_id: "PATIENT_00001" },
            projection: { patient_id: 1, measurement_time: 1 },
            createIndex: true,
            index: { patient_id: 1, measurement_time: 1 }
        }
    ];
    
    const results = [];
    
    indexTests.forEach(test => {
        // Create index if specified
        if (test.createIndex && test.index) {
            const indexName = Object.keys(test.index).join('_');
            try {
                db[COLLECTION_NAME].createIndex(test.index, { name: `test_idx_${indexName}` });
            } catch (e) {
                // Index might already exist
            }
        }
        
        // Run query
        const startTime = Date.now();
        const cursor = db[COLLECTION_NAME].find(test.query, test.projection || {});
        const queryResults = cursor.toArray();
        const endTime = Date.now();
        
        const executionTime = endTime - startTime;
        
        // Get explain
        const explain = db[COLLECTION_NAME].find(test.query, test.projection || {}).explain("executionStats");
        const executionStats = explain.executionStats;
        
        const analysis = {
            description: test.description,
            execution_time_ms: executionTime,
            result_count: queryResults.length,
            documents_examined: executionStats.totalDocsExamined,
            keys_examined: executionStats.totalKeysExamined,
            index_used: executionStats.executionStages.stage !== "COLLSCAN",
            index_name: test.createIndex ? `test_idx_${Object.keys(test.index).join('_')}` : null
        };
        
        results.push(analysis);
        
        print(`  ${test.description}:`);
        print(`    Time: ${executionTime} ms`);
        print(`    Docs examined: ${executionStats.totalDocsExamined}`);
        print(`    Index used: ${analysis.index_used ? "Yes" : "No"}`);
        
        // Clean up test index
        if (test.createIndex && test.index) {
            try {
                db[COLLECTION_NAME].dropIndex(`test_idx_${Object.keys(test.index).join('_')}`);
            } catch (e) {
                // Ignore errors
            }
        }
    });
    
    return results;
}

// Function to test query performance with different data volumes
function testScalability() {
    print("Testing query scalability with different data volumes...");
    
    // Create test collections with different data sizes
    const testCollections = [
        { name: "test_small", size: 1000 },
        { name: "test_medium", size: 10000 },
        { name: "test_large", size: 100000 }
    ];
    
    const results = [];
    
    testCollections.forEach(testColl => {
        print(`  Testing with ${testColl.size.toLocaleString()} documents...`);
        
        // Create test collection
        if (!db.getCollectionNames().includes(testColl.name)) {
            // Generate test data
            const batch = [];
            for (let i = 0; i < testColl.size; i++) {
                batch.push({
                    patient_id: `SCALABILITY_PATIENT_${i}`,
                    measurement_time: new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000),
                    vital_type: i % 2 === 0 ? "heart_rate_bpm" : "blood_pressure_systolic",
                    vital_value: 50 + Math.random() * 100,
                    is_alert: i % 100 === 0,
                    patient_department: i % 3 === 0 ? "ICU" : "WARD"
                });
            }
            
            db[testColl.name].insertMany(batch);
            
            // Create indexes
            db[testColl.name].createIndex({ patient_id: 1 });
            db[testColl.name].createIndex({ measurement_time: -1 });
            db[testColl.name].createIndex({ vital_type: 1 });
        }
        
        // Run test queries
        const queries = [
            {
                desc: "Simple find with equality",
                query: { vital_type: "heart_rate_bpm" },
                options: { limit: 100 }
            },
            {
                desc: "Range query",
                query: { 
                    measurement_time: { 
                        $gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) 
                    } 
                },
                options: { limit: 100 }
            },
            {
                desc: "Aggregation",
                pipeline: [
                    { $match: { is_alert: true } },
                    { $group: { _id: "$patient_department", count: { $sum: 1 } } }
                ]
            }
        ];
        
        queries.forEach(testQuery => {
            const startTime = Date.now();
            
            if (testQuery.pipeline) {
                // Aggregation query
                const cursor = db[testColl.name].aggregate(testQuery.pipeline);
                cursor.toArray();
            } else {
                // Find query
                const cursor = db[testColl.name].find(testQuery.query);
                if (testQuery.options && testQuery.options.limit) {
                    cursor.limit(testQuery.options.limit);
                }
                cursor.toArray();
            }
            
            const endTime = Date.now();
            const executionTime = endTime - startTime;
            
            results.push({
                collection_size: testColl.size,
                query_type: testQuery.desc,
                execution_time_ms: executionTime
            });
        });
        
        // Clean up test collection
        db[testColl.name].drop();
    });
    
    // Analyze scalability
    const scalabilityAnalysis = {};
    results.forEach(result => {
        const key = `${result.query_type}_${result.collection_size}`;
        scalabilityAnalysis[key] = result.execution_time_ms;
    });
    
    return { results, scalabilityAnalysis };
}

// Function to test concurrent query performance
function testConcurrentQueries() {
    print("Testing concurrent query performance...");
    
    // This is a simplified simulation
    const concurrentTests = 5;
    const queriesPerTest = 100;
    const testQuery = { patient_department: "ICU" };
    
    const results = [];
    const startTime = Date.now();
    
    // Simulate concurrent queries
    for (let i = 0; i < concurrentTests; i++) {
        let testStart = Date.now();
        let queriesExecuted = 0;
        
        for (let j = 0; j < queriesPerTest; j++) {
            db[COLLECTION_NAME].find(testQuery).limit(10).toArray();
            queriesExecuted++;
        }
        
        let testEnd = Date.now();
        let testDuration = testEnd - testStart;
        
        results.push({
            test_id: i,
            queries_executed: queriesExecuted,
            duration_ms: testDuration,
            queries_per_second: (queriesExecuted / (testDuration / 1000)).toFixed(2)
        });
    }
    
    const totalTime = Date.now() - startTime;
    const totalQueries = concurrentTests * queriesPerTest;
    const overallQPS = (totalQueries / (totalTime / 1000)).toFixed(2);
    
    print(`  Total queries: ${totalQueries}`);
    print(`  Total time: ${totalTime} ms`);
    print(`  Overall QPS: ${overallQPS}`);
    
    return {
        concurrent_tests: concurrentTests,
        queries_per_test: queriesPerTest,
        total_queries: totalQueries,
        total_time_ms: totalTime,
        overall_qps: overallQPS,
        test_results: results
    };
}

// Function to analyze query patterns and generate recommendations
function analyzeQueryPatterns() {
    print("Analyzing query patterns and generating recommendations...");
    
    // Get current indexes
    const indexes = db[COLLECTION_NAME].getIndexes();
    
    // Analyze slow queries from system profile (if enabled)
    let slowQueries = [];
    try {
        const profileData = db.system.profile.find({ millis: { $gt: 100 } }).limit(10).toArray();
        slowQueries = profileData.map(entry => ({
            query: entry.query || entry.command,
            duration_ms: entry.millis,
            namespace: entry.ns
        }));
    } catch (e) {
        print("  Note: Profiling not enabled. Enable with: db.setProfilingLevel(2)");
    }
    
    // Analyze index usage
    const indexStats = db[COLLECTION_NAME].aggregate([{ $indexStats: {} }]).toArray();
    
    const analysis = {
        timestamp: new Date(),
        total_indexes: indexes.length,
        index_sizes: indexes.map(idx => ({
            name: idx.name,
            size_kb: (idx.size / 1024).toFixed(2),
            spec: idx.key
        })),
        index_usage: indexStats,
        slow_queries: slowQueries,
        recommendations: []
    };
    
    // Generate recommendations
    
    // 1. Check for unused indexes
    const unusedIndexes = indexStats.filter(stat => stat.ops === 0);
    if (unusedIndexes.length > 0) {
        analysis.recommendations.push({
            type: "unused_index",
            severity: "medium",
            message: `Found ${unusedIndexes.length} unused indexes. Consider removing them.`,
            details: unusedIndexes.map(idx => idx.name)
        });
    }
    
    // 2. Check for missing indexes on frequently queried fields
    // (This would require query pattern analysis)
    
    // 3. Check index size vs data size
    const collStats = db[COLLECTION_NAME].stats();
    const indexSizeMB = collStats.totalIndexSize / 1024 / 1024;
    const dataSizeMB = collStats.size / 1024 / 1024;
    const indexRatio = (indexSizeMB / dataSizeMB) * 100;
    
    if (indexRatio > 50) {
        analysis.recommendations.push({
            type: "large_indexes",
            severity: "info",
            message: `Index size is ${indexRatio.toFixed(2)}% of data size. Consider index optimization.`,
            details: `Index size: ${indexSizeMB.toFixed(2)}MB, Data size: ${dataSizeMB.toFixed(2)}MB`
        });
    }
    
    // Print analysis
    print("Query Pattern Analysis:");
    print(`  Total indexes: ${indexes.length}`);
    print(`  Index size: ${indexSizeMB.toFixed(2)} MB`);
    print(`  Data size: ${dataSizeMB.toFixed(2)} MB`);
    print(`  Index/Data ratio: ${indexRatio.toFixed(2)}%`);
    
    if (slowQueries.length > 0) {
        print(`  Slow queries found: ${slowQueries.length}`);
    }
    
    if (analysis.recommendations.length > 0) {
        print("\nRecommendations:");
        analysis.recommendations.forEach(rec => {
            print(`  • [${rec.severity.toUpperCase()}] ${rec.message}`);
        });
    }
    
    return analysis;
}

// Function to run comprehensive query performance tests
function runComprehensiveQueryTests() {
    print("=".repeat(60));
    print("MONGODB QUERY PERFORMANCE TESTS");
    print("=".repeat(60));
    print(`Timestamp: ${new Date()}`);
    print("");
    
    const allResults = {};
    
    // Test 1: Different query types
    print("TEST 1: QUERY TYPES PERFORMANCE");
    print("-".repeat(40));
    allResults.queryTypes = testQueryTypes();
    print("");
    
    // Test 2: Aggregation performance
    print("TEST 2: AGGREGATION PERFORMANCE");
    print("-".repeat(40));
    allResults.aggregations = testAggregationPerformance();
    print("");
    
    // Test 3: Index effectiveness
    print("TEST 3: INDEX EFFECTIVENESS");
    print("-".repeat(40));
    allResults.indexEffectiveness = testIndexEffectiveness();
    print("");
    
    // Test 4: Scalability
    print("TEST 4: QUERY SCALABILITY");
    print("-".repeat(40));
    allResults.scalability = testScalability();
    print("");
    
    // Test 5: Concurrent queries
    print("TEST 5: CONCURRENT QUERY PERFORMANCE");
    print("-".repeat(40));
    allResults.concurrent = testConcurrentQueries();
    print("");
    
    // Test 6: Query pattern analysis
    print("TEST 6: QUERY PATTERN ANALYSIS");
    print("-".repeat(40));
    allResults.patternAnalysis = analyzeQueryPatterns();
    print("");
    
    // Generate summary report
    print("=".repeat(60));
    print("QUERY PERFORMANCE SUMMARY");
    print("=".repeat(60));
    
    // Calculate statistics
    let totalQueryTime = 0;
    let totalQueries = 0;
    
    // Query types statistics
    if (allResults.queryTypes) {
        const queryTimes = allResults.queryTypes.map(q => q.execution_time_ms);
        const avgQueryTime = queryTimes.reduce((a, b) => a + b, 0) / queryTimes.length;
        
        print("Query Types Performance:");
        print(`  Average query time: ${avgQueryTime.toFixed(2)} ms`);
        print(`  Fastest query: ${Math.min(...queryTimes)} ms`);
        print(`  Slowest query: ${Math.max(...queryTimes)} ms`);
        
        totalQueryTime += queryTimes.reduce((a, b) => a + b, 0);
        totalQueries += queryTimes.length;
    }
    
    // Aggregation statistics
    if (allResults.aggregations) {
        const aggTimes = allResults.aggregations.map(a => a.execution_time_ms);
        const avgAggTime = aggTimes.reduce((a, b) => a + b, 0) / aggTimes.length;
        
        print("\nAggregation Performance:");
        print(`  Average aggregation time: ${avgAggTime.toFixed(2)} ms`);
        print(`  Fastest aggregation: ${Math.min(...aggTimes)} ms`);
        print(`  Slowest aggregation: ${Math.max(...aggTimes)} ms`);
        
        totalQueryTime += aggTimes.reduce((a, b) => a + b, 0);
        totalQueries += aggTimes.length;
    }
    
    // Index effectiveness
    if (allResults.indexEffectiveness) {
        const withIndex = allResults.indexEffectiveness.filter(q => q.index_used);
        const withoutIndex = allResults.indexEffectiveness.filter(q => !q.index_used);
        
        if (withIndex.length > 0 && withoutIndex.length > 0) {
            const avgWithIndex = withIndex.reduce((sum, q) => sum + q.execution_time_ms, 0) / withIndex.length;
            const avgWithoutIndex = withoutIndex.reduce((sum, q) => sum + q.execution_time_ms, 0) / withoutIndex.length;
            const improvement = ((avgWithoutIndex - avgWithIndex) / avgWithoutIndex * 100).toFixed(2);
            
            print("\nIndex Effectiveness:");
            print(`  Average with index: ${avgWithIndex.toFixed(2)} ms`);
            print(`  Average without index: ${avgWithoutIndex.toFixed(2)} ms`);
            print(`  Improvement: ${improvement}%`);
        }
    }
    
    // Concurrent performance
    if (allResults.concurrent) {
        print("\nConcurrent Performance:");
        print(`  Overall QPS: ${allResults.concurrent.overall_qps}`);
        print(`  Total queries: ${allResults.concurrent.total_queries}`);
    }
    
    // Save all results
    const summary = {
        timestamp: new Date(),
        database: DATABASE_NAME,
        collection: COLLECTION_NAME,
        test_summary: {
            total_tests: Object.keys(allResults).length,
            total_queries: totalQueries,
            total_query_time_ms: totalQueryTime,
            avg_query_time_ms: totalQueries > 0 ? totalQueryTime / totalQueries : 0
        },
        detailed_results: allResults
    };
    
    db[RESULTS_COLLECTION].insertOne(summary);
    
    print("\nResults saved to: " + RESULTS_COLLECTION);
    print("=".repeat(60));
    
    return summary;
}

// Main execution
function main() {
    try {
        // Check if we're in the right database
        if (db.getName() !== DATABASE_NAME) {
            print(`Switching to database: ${DATABASE_NAME}`);
            db = db.getSiblingDB(DATABASE_NAME);
        }
        
        // Create results collection if it doesn't exist
        if (!db.getCollectionNames().includes(RESULTS_COLLECTION)) {
            db.createCollection(RESULTS_COLLECTION);
        }
        
        // Run comprehensive tests
        const results = runComprehensiveQueryTests();
        
        // Generate optimization recommendations
        print("\n" + "=".repeat(60));
        print("OPTIMIZATION RECOMMENDATIONS");
        print("=".repeat(60));
        
        const recommendations = [];
        
        // Based on query types performance
        if (results.detailed_results.queryTypes) {
            const slowQueries = results.detailed_results.queryTypes.filter(q => q.execution_time_ms > 100);
            if (slowQueries.length > 0) {
                recommendations.push({
                    type: "slow_queries",
                    message: `Found ${slowQueries.length} queries taking >100ms. Consider adding indexes.`,
                    queries: slowQueries.map(q => q.description)
                });
            }
        }
        
        // Based on index effectiveness
        if (results.detailed_results.indexEffectiveness) {
            const collectionScans = results.detailed_results.indexEffectiveness.filter(q => !q.index_used);
            if (collectionScans.length > 0) {
                recommendations.push({
                    type: "collection_scans",
                    message: `Found ${collectionScans.length} queries performing collection scans. Add appropriate indexes.`,
                    queries: collectionScans.map(q => q.description)
                });
            }
        }
        
        // Based on aggregation performance
        if (results.detailed_results.aggregations) {
            const slowAggregations = results.detailed_results.aggregations.filter(a => a.execution_time_ms > 500);
            if (slowAggregations.length > 0) {
                recommendations.push({
                    type: "slow_aggregations",
                    message: `Found ${slowAggregations.length} slow aggregations. Consider pipeline optimization.`,
                    aggregations: slowAggregations.map(a => a.description)
                });
            }
        }
        
        // Print recommendations
        if (recommendations.length > 0) {
            recommendations.forEach((rec, index) => {
                print(`${index + 1}. ${rec.message}`);
                if (rec.queries) {
                    rec.queries.forEach(query => print(`   • ${query}`));
                }
            });
        } else {
            print("No major optimization issues found. Current configuration appears optimal.");
        }
        
        print("\n" + "=".repeat(60));
        
        return results;
        
    } catch (error) {
        print("Error running query performance tests: " + error.message);
        return null;
    }
}

// Run the main function
main();