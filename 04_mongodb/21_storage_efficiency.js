// MongoDB Storage Efficiency Analysis
// Comprehensive storage analysis and optimization

// Configuration
const DATABASE_NAME = "health_iot_benchmark";
const COLLECTION_NAME = "patient_vitals";

// Function to analyze storage utilization
function analyzeStorageUtilization() {
    print("Analyzing storage utilization...");
    
    const dbStats = db.stats();
    const collStats = db[COLLECTION_NAME].stats();
    
    const analysis = {
        timestamp: new Date(),
        database: dbStats.db,
        storage_metrics: {
            // Database level metrics
            data_size_mb: dbStats.dataSize / (1024 * 1024),
            storage_size_mb: dbStats.storageSize / (1024 * 1024),
            index_size_mb: dbStats.indexSize / (1024 * 1024),
            total_size_mb: (dbStats.storageSize + dbStats.indexSize) / (1024 * 1024),
            
            // Collection level metrics
            collection_data_size_mb: collStats.size / (1024 * 1024),
            collection_storage_size_mb: collStats.storageSize / (1024 * 1024),
            collection_index_size_mb: collStats.totalIndexSize / (1024 * 1024),
            collection_total_size_mb: (collStats.storageSize + collStats.totalIndexSize) / (1024 * 1024),
            
            // Efficiency metrics
            storage_efficiency: (collStats.size / collStats.storageSize * 100).toFixed(2),
            index_overhead: (collStats.totalIndexSize / collStats.size * 100).toFixed(2),
            fragmentation: ((collStats.storageSize - collStats.size) / collStats.storageSize * 100).toFixed(2)
        },
        collection_stats: {
            count: collStats.count,
            avg_obj_size: collStats.avgObjSize,
            num_indexes: collStats.nindexes,
            index_details: collStats.indexSizes
        }
    };
    
    // Print analysis
    print("Storage Analysis Results:");
    print("-".repeat(60));
    
    print("Database Level:");
    print(`  Data Size: ${analysis.storage_metrics.data_size_mb.toFixed(2)} MB`);
    print(`  Storage Size: ${analysis.storage_metrics.storage_size_mb.toFixed(2)} MB`);
    print(`  Index Size: ${analysis.storage_metrics.index_size_mb.toFixed(2)} MB`);
    print(`  Total Size: ${analysis.storage_metrics.total_size_mb.toFixed(2)} MB`);
    print("");
    
    print("Collection Level (" + COLLECTION_NAME + "):");
    print(`  Document Count: ${analysis.collection_stats.count.toLocaleString()}`);
    print(`  Average Object Size: ${analysis.collection_stats.avg_obj_size.toFixed(2)} bytes`);
    print(`  Data Size: ${analysis.storage_metrics.collection_data_size_mb.toFixed(2)} MB`);
    print(`  Storage Size: ${analysis.storage_metrics.collection_storage_size_mb.toFixed(2)} MB`);
    print(`  Index Size: ${analysis.storage_metrics.collection_index_size_mb.toFixed(2)} MB`);
    print(`  Total Collection Size: ${analysis.storage_metrics.collection_total_size_mb.toFixed(2)} MB`);
    print("");
    
    print("Efficiency Metrics:");
    print(`  Storage Efficiency: ${analysis.storage_metrics.storage_efficiency}%`);
    print(`  Index Overhead: ${analysis.storage_metrics.index_overhead}%`);
    print(`  Fragmentation: ${analysis.storage_metrics.fragmentation}%`);
    print("");
    
    // Analyze index sizes
    print("Index Details:");
    Object.keys(analysis.collection_stats.index_details).forEach(indexName => {
        const sizeMB = analysis.collection_stats.index_details[indexName] / (1024 * 1024);
        print(`  ${indexName}: ${sizeMB.toFixed(2)} MB`);
    });
    
    return analysis;
}

// Function to analyze data distribution and compression opportunities
function analyzeDataDistribution() {
    print("\nAnalyzing data distribution for compression opportunities...");
    
    const analysis = {
        field_analysis: {},
        value_distribution: {},
        compression_opportunities: []
    };
    
    // Sample documents for analysis
    const sampleSize = 1000;
    const sample = db[COLLECTION_NAME].find().limit(sampleSize).toArray();
    
    if (sample.length === 0) {
        print("No documents found for analysis.");
        return analysis;
    }
    
    // Analyze field types and sizes
    const fieldStats = {};
    
    sample.forEach(doc => {
        Object.keys(doc).forEach(field => {
            if (!fieldStats[field]) {
                fieldStats[field] = {
                    type: typeof doc[field],
                    values: new Set(),
                    totalSize: 0,
                    count: 0
                };
            }
            
            const value = doc[field];
            fieldStats[field].values.add(JSON.stringify(value));
            fieldStats[field].totalSize += Buffer.byteLength(JSON.stringify(value), 'utf8');
            fieldStats[field].count++;
        });
    });
    
    // Calculate statistics
    Object.keys(fieldStats).forEach(field => {
        const stats = fieldStats[field];
        analysis.field_analysis[field] = {
            type: stats.type,
            unique_values: stats.values.size,
            avg_value_size: stats.totalSize / stats.count,
            cardinality: (stats.values.size / sampleSize * 100).toFixed(2) + '%'
        };
        
        // Identify compression opportunities
        if (stats.values.size < 10 && stats.type === 'string') {
            analysis.compression_opportunities.push({
                field: field,
                reason: `Low cardinality (${stats.values.size} unique values)`,
                suggestion: "Consider using integer codes or enum values"
            });
        }
        
        if (stats.avg_value_size > 100) {
            analysis.compression_opportunities.push({
                field: field,
                reason: `Large average value size (${stats.avg_value_size.toFixed(2)} bytes)`,
                suggestion: "Consider compression or data normalization"
            });
        }
    });
    
    // Analyze value distribution for specific fields
    const fieldsToAnalyze = ['vital_type', 'patient_department', 'data_classification'];
    
    fieldsToAnalyze.forEach(field => {
        if (analysis.field_analysis[field]) {
            const distribution = db[COLLECTION_NAME].aggregate([
                { $group: { _id: `$${field}`, count: { $sum: 1 } } },
                { $sort: { count: -1 } },
                { $limit: 10 }
            ]).toArray();
            
            analysis.value_distribution[field] = distribution;
        }
    });
    
    // Print analysis
    print("Field Analysis:");
    print("-".repeat(60));
    
    Object.keys(analysis.field_analysis).forEach(field => {
        const stats = analysis.field_analysis[field];
        print(`${field}:`);
        print(`  Type: ${stats.type}`);
        print(`  Unique Values: ${stats.unique_values}`);
        print(`  Avg Size: ${stats.avg_value_size.toFixed(2)} bytes`);
        print(`  Cardinality: ${stats.cardinality}`);
    });
    
    print("\nValue Distribution:");
    print("-".repeat(60));
    
    Object.keys(analysis.value_distribution).forEach(field => {
        print(`${field}:`);
        analysis.value_distribution[field].forEach(item => {
            const percentage = (item.count / analysis.collection_stats.count * 100).toFixed(2);
            print(`  ${item._id}: ${item.count.toLocaleString()} (${percentage}%)`);
        });
    });
    
    if (analysis.compression_opportunities.length > 0) {
        print("\nCompression Opportunities:");
        print("-".repeat(60));
        analysis.compression_opportunities.forEach(opp => {
            print(`${opp.field}: ${opp.reason}`);
            print(`  Suggestion: ${opp.suggestion}`);
        });
    }
    
    return analysis;
}

// Function to analyze index efficiency
function analyzeIndexEfficiency() {
    print("\nAnalyzing index efficiency...");
    
    const indexStats = db[COLLECTION_NAME].aggregate([{ $indexStats: {} }]).toArray();
    const collStats = db[COLLECTION_NAME].stats();
    const indexSizes = collStats.indexSizes;
    
    const analysis = {
        total_indexes: indexStats.length,
        indexes: [],
        recommendations: []
    };
    
    // Calculate index efficiency metrics
    let totalIndexOps = 0;
    let unusedIndexes = 0;
    
    indexStats.forEach(stat => {
        const indexName = stat.name;
        const sizeMB = (indexSizes[indexName] || 0) / (1024 * 1024);
        const ops = stat.ops;
        totalIndexOps += ops;
        
        const efficiency = {
            name: indexName,
            size_mb: sizeMB.toFixed(2),
            operations: ops.toLocaleString(),
            since: stat.since,
            last_access: stat.accesses && stat.accesses.ops ? 
                new Date(stat.accesses.ops) : 'Never',
            efficiency_score: ops > 0 ? Math.log10(ops + 1) * sizeMB : 0
        };
        
        analysis.indexes.push(efficiency);
        
        // Check for unused indexes
        if (ops === 0 && !indexName.startsWith('_id_')) {
            unusedIndexes++;
            analysis.recommendations.push({
                type: 'unused_index',
                index: indexName,
                size_mb: sizeMB.toFixed(2),
                message: `Index "${indexName}" has never been used (${sizeMB.toFixed(2)} MB)`,
                recommendation: 'Consider removing this index'
            });
        }
        
        // Check for large indexes with low usage
        if (sizeMB > 100 && ops < 100) {
            analysis.recommendations.push({
                type: 'large_low_usage_index',
                index: indexName,
                size_mb: sizeMB.toFixed(2),
                operations: ops,
                message: `Large index "${indexName}" (${sizeMB.toFixed(2)} MB) has low usage (${ops} ops)`,
                recommendation: 'Evaluate if this index is necessary'
            });
        }
    });
    
    // Check index to data ratio
    const dataSizeMB = collStats.size / (1024 * 1024);
    const totalIndexSizeMB = collStats.totalIndexSize / (1024 * 1024);
    const indexToDataRatio = (totalIndexSizeMB / dataSizeMB * 100).toFixed(2);
    
    if (indexToDataRatio > 50) {
        analysis.recommendations.push({
            type: 'high_index_overhead',
            ratio: indexToDataRatio,
            message: `Index size is ${indexToDataRatio}% of data size`,
            recommendation: 'Consider index optimization or removal of redundant indexes'
        });
    }
    
    // Print analysis
    print("Index Efficiency Analysis:");
    print("-".repeat(80));
    
    print("Index\t\tSize (MB)\tOperations\tLast Access\t\tEfficiency Score");
    print("-".repeat(80));
    
    analysis.indexes.forEach(idx => {
        const lastAccess = idx.last_access instanceof Date ? 
            idx.last_access.toISOString().split('T')[0] : idx.last_access;
        
        print(`${idx.name.padEnd(20)}${idx.size_mb.padStart(10)}${idx.operations.padStart(15)}` +
              `${lastAccess.padStart(20)}${idx.efficiency_score.toFixed(2).padStart(15)}`);
    });
    
    print("\nSummary:");
    print(`  Total Indexes: ${analysis.total_indexes}`);
    print(`  Unused Indexes: ${unusedIndexes}`);
    print(`  Total Index Operations: ${totalIndexOps.toLocaleString()}`);
    print(`  Index to Data Ratio: ${indexToDataRatio}%`);
    
    if (analysis.recommendations.length > 0) {
        print("\nRecommendations:");
        analysis.recommendations.forEach(rec => {
            print(`  • ${rec.message}`);
            print(`    ${rec.recommendation}`);
        });
    }
    
    return analysis;
}

// Function to analyze storage engine performance
function analyzeStorageEngine() {
    print("\nAnalyzing storage engine performance...");
    
    const serverStatus = db.serverStatus();
    const storageEngine = serverStatus.storageEngine || { name: 'wiredTiger' };
    
    const analysis = {
        storage_engine: storageEngine.name,
        metrics: {},
        recommendations: []
    };
    
    if (storageEngine.name === 'wiredTiger') {
        const wtStats = storageEngine.wiredTiger || {};
        
        analysis.metrics = {
            cache_usage: {
                max_gb: (wtStats['cache maximum bytes configured'] || 0) / (1024 * 1024 * 1024),
                current_gb: (wtStats['cache bytes currently in the cache'] || 0) / (1024 * 1024 * 1024),
                dirty_gb: (wtStats['tracked dirty bytes in the cache'] || 0) / (1024 * 1024 * 1024)
            },
            transaction_stats: {
                begins: wtStats['transaction begins'] || 0,
                checkpoints: wtStats['transaction checkpoint currently running'] || 0,
                fsyncs: wtStats['fsync calls'] || 0
            },
            block_manager: {
                bytes_read: (wtStats['block manager bytes read'] || 0) / (1024 * 1024),
                bytes_written: (wtStats['block manager bytes written'] || 0) / (1024 * 1024),
                bytes_cached: (wtStats['bytes belonging to page images in the cache'] || 0) / (1024 * 1024)
            }
        };
        
        // Calculate cache hit ratio
        const bytesRead = analysis.metrics.block_manager.bytes_read;
        const bytesCached = analysis.metrics.block_manager.bytes_cached;
        const cacheHitRatio = bytesRead > 0 ? (bytesCached / bytesRead * 100).toFixed(2) : 0;
        
        analysis.metrics.cache_hit_ratio = cacheHitRatio;
        
        // Generate recommendations
        if (cacheHitRatio < 80) {
            analysis.recommendations.push({
                type: 'low_cache_hit',
                ratio: cacheHitRatio,
                message: `Cache hit ratio is low (${cacheHitRatio}%)`,
                recommendation: 'Consider increasing wiredTiger cache size'
            });
        }
        
        const cacheUsage = (analysis.metrics.cache_usage.current_gb / 
                          analysis.metrics.cache_usage.max_gb * 100).toFixed(2);
        
        if (cacheUsage > 90) {
            analysis.recommendations.push({
                type: 'high_cache_usage',
                usage: cacheUsage,
                message: `Cache usage is high (${cacheUsage}%)`,
                recommendation: 'Monitor cache usage and consider increasing cache size if needed'
            });
        }
    }
    
    // Print analysis
    print("Storage Engine Analysis:");
    print("-".repeat(60));
    print(`Engine: ${analysis.storage_engine}`);
    
    if (analysis.storage_engine === 'wiredTiger') {
        print("\nCache Statistics:");
        print(`  Max Cache Size: ${analysis.metrics.cache_usage.max_gb.toFixed(2)} GB`);
        print(`  Current Cache Usage: ${analysis.metrics.cache_usage.current_gb.toFixed(2)} GB`);
        print(`  Dirty Cache: ${analysis.metrics.cache_usage.dirty_gb.toFixed(2)} GB`);
        print(`  Cache Hit Ratio: ${analysis.metrics.cache_hit_ratio}%`);
        
        print("\nTransaction Statistics:");
        print(`  Transaction Begins: ${analysis.metrics.transaction_stats.begins.toLocaleString()}`);
        print(`  Active Checkpoints: ${analysis.metrics.transaction_stats.checkpoints}`);
        print(`  Fsync Calls: ${analysis.metrics.transaction_stats.fsyncs.toLocaleString()}`);
        
        print("\nBlock Manager Statistics:");
        print(`  Bytes Read: ${analysis.metrics.block_manager.bytes_read.toFixed(2)} MB`);
        print(`  Bytes Written: ${analysis.metrics.block_manager.bytes_written.toFixed(2)} MB`);
        print(`  Bytes Cached: ${analysis.metrics.block_manager.bytes_cached.toFixed(2)} MB`);
    }
    
    if (analysis.recommendations.length > 0) {
        print("\nRecommendations:");
        analysis.recommendations.forEach(rec => {
            print(`  • ${rec.message}`);
            print(`    ${rec.recommendation}`);
        });
    }
    
    return analysis;
}

// Function to analyze document structure and schema
function analyzeDocumentStructure() {
    print("\nAnalyzing document structure and schema...");
    
    const sampleSize = 1000;
    const sample = db[COLLECTION_NAME].find().limit(sampleSize).toArray();
    
    const schemaAnalysis = {
        total_documents_analyzed: sample.length,
        field_frequency: {},
        field_types: {},
        schema_violations: [],
        recommendations: []
    };
    
    // Analyze field frequency and types
    sample.forEach((doc, index) => {
        const docFields = Object.keys(doc);
        
        docFields.forEach(field => {
            // Count field frequency
            if (!schemaAnalysis.field_frequency[field]) {
                schemaAnalysis.field_frequency[field] = 0;
            }
            schemaAnalysis.field_frequency[field]++;
            
            // Track field types
            const fieldType = Array.isArray(doc[field]) ? 'array' : typeof doc[field];
            if (!schemaAnalysis.field_types[field]) {
                schemaAnalysis.field_types[field] = {};
            }
            if (!schemaAnalysis.field_types[field][fieldType]) {
                schemaAnalysis.field_types[field][fieldType] = 0;
            }
            schemaAnalysis.field_types[field][fieldType]++;
        });
        
        // Check for schema violations (inconsistent field types)
        docFields.forEach(field => {
            const types = Object.keys(schemaAnalysis.field_types[field] || {});
            if (types.length > 1) {
                const violation = {
                    field: field,
                    document_index: index,
                    types: types,
                    value: doc[field]
                };
                
                // Check if this violation already reported
                const existingViolation = schemaAnalysis.schema_violations.find(
                    v => v.field === field && JSON.stringify(v.types) === JSON.stringify(types)
                );
                
                if (!existingViolation) {
                    schemaAnalysis.schema_violations.push(violation);
                }
            }
        });
    });
    
    // Calculate field presence percentage
    Object.keys(schemaAnalysis.field_frequency).forEach(field => {
        const presence = (schemaAnalysis.field_frequency[field] / sampleSize * 100).toFixed(2);
        
        // Identify optional fields
        if (presence < 100) {
            schemaAnalysis.recommendations.push({
                type: 'optional_field',
                field: field,
                presence: presence + '%',
                message: `Field "${field}" is optional (present in ${presence}% of documents)`,
                recommendation: 'Consider making this field required or using default values'
            });
        }
        
        // Identify fields with multiple types
        const typeCount = Object.keys(schemaAnalysis.field_types[field] || {}).length;
        if (typeCount > 1) {
            schemaAnalysis.recommendations.push({
                type: 'inconsistent_types',
                field: field,
                types: Object.keys(schemaAnalysis.field_types[field]),
                message: `Field "${field}" has ${typeCount} different types`,
                recommendation: 'Standardize field types for better compression and query performance'
            });
        }
    });
    
    // Print analysis
    print("Document Structure Analysis:");
    print("-".repeat(60));
    print(`Documents Analyzed: ${schemaAnalysis.total_documents_analyzed}`);
    
    print("\nField Frequency:");
    print("Field\t\t\tPresence\tTypes");
    print("-".repeat(60));
    
    Object.keys(schemaAnalysis.field_frequency)
        .sort((a, b) => schemaAnalysis.field_frequency[b] - schemaAnalysis.field_frequency[a])
        .forEach(field => {
            const presence = (schemaAnalysis.field_frequency[field] / sampleSize * 100).toFixed(2);
            const types = Object.keys(schemaAnalysis.field_types[field] || {}).join(', ');
            print(`${field.padEnd(20)}${presence.padStart(10)}%${types.padStart(20)}`);
        });
    
    if (schemaAnalysis.schema_violations.length > 0) {
        print("\nSchema Violations Found:");
        schemaAnalysis.schema_violations.forEach(violation => {
            print(`  Field "${violation.field}" has types: ${violation.types.join(', ')}`);
        });
    }
    
    if (schemaAnalysis.recommendations.length > 0) {
        print("\nRecommendations:");
        schemaAnalysis.recommendations.forEach(rec => {
            print(`  • ${rec.message}`);
            print(`    ${rec.recommendation}`);
        });
    }
    
    return schemaAnalysis;
}

// Function to implement storage optimizations
function implementStorageOptimizations() {
    print("\nImplementing storage optimizations...");
    
    const optimizations = {
        implemented: [],
        failed: [],
        results: {}
    };
    
    // Optimization 1: Create compressed collection (if supported)
    try {
        const compressedCollectionName = `${COLLECTION_NAME}_compressed`;
        
        // Check if collection already exists
        const collections = db.getCollectionNames();
        if (!collections.includes(compressedCollectionName)) {
            // Create new collection with compression
            db.createCollection(compressedCollectionName, {
                storageEngine: {
                    wiredTiger: {
                        configString: 'block_compressor=snappy'
                    }
                }
            });
            
            // Copy data with optimized schema
            print("  Creating compressed collection...");
            
            const pipeline = [
                {
                    $project: {
                        // Remove unnecessary fields and optimize types
                        patient_id: 1,
                        measurement_time: 1,
                        vital_type: 1,
                        vital_value: { $toDouble: "$vital_value" },
                        is_alert: { $toBool: "$is_alert" },
                        patient_department: 1,
                        device_id: 1,
                        data_classification: 1,
                        confidence: { $toDouble: "$confidence" },
                        // Remove metadata if not needed
                        // metadata: 1
                    }
                },
                { $out: compressedCollectionName }
            ];
            
            db[COLLECTION_NAME].aggregate(pipeline);
            
            // Create indexes on compressed collection
            db[compressedCollectionName].createIndex({ patient_id: 1 });
            db[compressedCollectionName].createIndex({ measurement_time: -1 });
            db[compressedCollectionName].createIndex({ vital_type: 1 });
            
            const originalStats = db[COLLECTION_NAME].stats();
            const compressedStats = db[compressedCollectionName].stats();
            
            const compressionRatio = (originalStats.size / compressedStats.size).toFixed(2);
            
            optimizations.implemented.push({
                name: 'compressed_collection',
                collection: compressedCollectionName,
                compression_ratio: compressionRatio,
                original_size_mb: originalStats.size / (1024 * 1024),
                compressed_size_mb: compressedStats.size / (1024 * 1024),
                savings_mb: (originalStats.size - compressedStats.size) / (1024 * 1024)
            });
            
            print(`    Created compressed collection with ${compressionRatio}x compression`);
        } else {
            print("  Compressed collection already exists");
        }
    } catch (error) {
        optimizations.failed.push({
            name: 'compressed_collection',
            error: error.message
        });
        print(`  Failed to create compressed collection: ${error.message}`);
    }
    
    // Optimization 2: Implement TTL for old data
    try {
        // Create TTL index for data older than 90 days
        const ttlIndexName = 'idx_ttl_90days';
        const existingIndexes = db[COLLECTION_NAME].getIndexes();
        const ttlExists = existingIndexes.some(idx => idx.name === ttlIndexName);
        
        if (!ttlExists) {
            db[COLLECTION_NAME].createIndex(
                { measurement_time: 1 },
                { 
                    name: ttlIndexName,
                    expireAfterSeconds: 90 * 24 * 60 * 60 // 90 days
                }
            );
            
            optimizations.implemented.push({
                name: 'ttl_index',
                index: ttlIndexName,
                expire_after_days: 90
            });
            
            print("  Created TTL index for 90-day data retention");
        } else {
            print("  TTL index already exists");
        }
    } catch (error) {
        optimizations.failed.push({
            name: 'ttl_index',
            error: error.message
        });
        print(`  Failed to create TTL index: ${error.message}`);
    }
    
    // Optimization 3: Archive old data to separate collection
    try {
        const archiveCollectionName = `${COLLECTION_NAME}_archive`;
        const archiveThreshold = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000); // 180 days
        
        // Check if archive collection exists
        const collections = db.getCollectionNames();
        if (!collections.includes(archiveCollectionName)) {
            db.createCollection(archiveCollectionName);
            
            // Move old data to archive
            print("  Archiving old data...");
            
            const oldData = db[COLLECTION_NAME].find({
                measurement_time: { $lt: archiveThreshold }
            }).toArray();
            
            if (oldData.length > 0) {
                db[archiveCollectionName].insertMany(oldData);
                
                // Delete archived data from main collection
                db[COLLECTION_NAME].deleteMany({
                    measurement_time: { $lt: archiveThreshold }
                });
                
                optimizations.implemented.push({
                    name: 'data_archiving',
                    collection: archiveCollectionName,
                    documents_archived: oldData.length,
                    archive_threshold_days: 180
                });
                
                print(`    Archived ${oldData.length} documents older than 180 days`);
            } else {
                print("    No data found for archiving");
            }
        } else {
            print("  Archive collection already exists");
        }
    } catch (error) {
        optimizations.failed.push({
            name: 'data_archiving',
            error: error.message
        });
        print(`  Failed to archive data: ${error.message}`);
    }
    
    // Optimization 4: Compact collection (if WiredTiger)
    try {
        print("  Running collection compact...");
        
        const beforeStats = db[COLLECTION_NAME].stats();
        
        // Run compact command
        db.runCommand({ compact: COLLECTION_NAME });
        
        // Note: compact runs in background, so we just note it
        optimizations.implemented.push({
            name: 'collection_compact',
            collection: COLLECTION_NAME,
            note: 'Compact command issued (runs in background)'
        });
        
        print("    Compact command issued (runs in background)");
    } catch (error) {
        optimizations.failed.push({
            name: 'collection_compact',
            error: error.message
        });
        print(`    Failed to compact collection: ${error.message}`);
    }
    
    // Print optimization results
    print("\nOptimization Results:");
    print("-".repeat(60));
    
    if (optimizations.implemented.length > 0) {
        print("Implemented Optimizations:");
        optimizations.implemented.forEach(opt => {
            print(`  ✓ ${opt.name}`);
            if (opt.compression_ratio) {
                print(`    Compression ratio: ${opt.compression_ratio}x`);
                print(`    Space saved: ${opt.savings_mb.toFixed(2)} MB`);
            }
            if (opt.documents_archived) {
                print(`    Documents archived: ${opt.documents_archived}`);
            }
        });
    }
    
    if (optimizations.failed.length > 0) {
        print("\nFailed Optimizations:");
        optimizations.failed.forEach(opt => {
            print(`  ✗ ${opt.name}: ${opt.error}`);
        });
    }
    
    return optimizations;
}

// Function to generate storage efficiency report
function generateStorageEfficiencyReport() {
    print("\n" + "=".repeat(60));
    print("STORAGE EFFICIENCY REPORT");
    print("=".repeat(60));
    print(`Database: ${DATABASE_NAME}`);
    print(`Collection: ${COLLECTION_NAME}`);
    print(`Timestamp: ${new Date()}`);
    print("");
    
    const report = {
        timestamp: new Date(),
        database: DATABASE_NAME,
        collection: COLLECTION_NAME,
        analyses: {},
        recommendations: [],
        summary: {}
    };
    
    // Run all analyses
    print("Running comprehensive storage analysis...");
    print("");
    
    report.analyses.storage_utilization = analyzeStorageUtilization();
    report.analyses.data_distribution = analyzeDataDistribution();
    report.analyses.index_efficiency = analyzeIndexEfficiency();
    report.analyses.storage_engine = analyzeStorageEngine();
    report.analyses.document_structure = analyzeDocumentStructure();
    
    // Implement optimizations
    print("\n" + "=".repeat(60));
    print("IMPLEMENTING OPTIMIZATIONS");
    print("=".repeat(60));
    report.optimizations = implementStorageOptimizations();
    
    // Generate summary
    const storageMetrics = report.analyses.storage_utilization.storage_metrics;
    
    report.summary = {
        total_size_mb: storageMetrics.total_size_mb.toFixed(2),
        data_size_mb: storageMetrics.data_size_mb.toFixed(2),
        index_size_mb: storageMetrics.index_size_mb.toFixed(2),
        storage_efficiency: storageMetrics.storage_efficiency + '%',
        fragmentation: storageMetrics.fragmentation + '%',
        index_overhead: storageMetrics.index_overhead + '%',
        document_count: report.analyses.storage_utilization.collection_stats.count,
        avg_document_size: report.analyses.storage_utilization.collection_stats.avg_obj_size.toFixed(2)
    };
    
    // Generate recommendations
    if (parseFloat(storageMetrics.fragmentation) > 20) {
        report.recommendations.push({
            priority: 'high',
            recommendation: 'High fragmentation detected. Run compaction regularly.',
            impact: 'Improves storage efficiency and query performance'
        });
    }
    
    if (parseFloat(storageMetrics.index_overhead) > 50) {
        report.recommendations.push({
            priority: 'high',
            recommendation: 'High index overhead. Review and optimize indexes.',
            impact: 'Reduces storage usage and improves write performance'
        });
    }
    
    if (report.analyses.index_efficiency.unusedIndexes > 0) {
        report.recommendations.push({
            priority: 'medium',
            recommendation: `Remove ${report.analyses.index_efficiency.unusedIndexes} unused indexes.`,
            impact: 'Reduces storage and improves write performance'
        });
    }
    
    // Print final report
    print("\n" + "=".repeat(60));
    print("FINAL STORAGE EFFICIENCY SUMMARY");
    print("=".repeat(60));
    
    print("\nStorage Summary:");
    print(`  Total Storage: ${report.summary.total_size_mb} MB`);
    print(`  Data Size: ${report.summary.data_size_mb} MB`);
    print(`  Index Size: ${report.summary.index_size_mb} MB`);
    print(`  Storage Efficiency: ${report.summary.storage_efficiency}`);
    print(`  Fragmentation: ${report.summary.fragmentation}`);
    print(`  Index Overhead: ${report.summary.index_overhead}`);
    print(`  Document Count: ${report.summary.document_count.toLocaleString()}`);
    print(`  Avg Document Size: ${report.summary.avg_document_size} bytes`);
    
    print("\nOptimization Results:");
    print(`  Implemented: ${report.optimizations.implemented.length}`);
    print(`  Failed: ${report.optimizations.failed.length}`);
    
    if (report.recommendations.length > 0) {
        print("\nKey Recommendations:");
        report.recommendations.forEach(rec => {
            print(`  [${rec.priority.toUpperCase()}] ${rec.recommendation}`);
            print(`    Impact: ${rec.impact}`);
        });
    }
    
    // Save report to collection
    db.storage_efficiency_reports.insertOne(report);
    
    print("\nDetailed report saved to storage_efficiency_reports collection.");
    print("=".repeat(60));
    
    return report;
}

// Function to monitor storage growth
function monitorStorageGrowth(days = 30) {
    print(`\nMonitoring storage growth over last ${days} days...`);
    
    // Create monitoring collection if it doesn't exist
    const monitoringCollection = 'storage_growth_monitoring';
    
    if (!db.getCollectionNames().includes(monitoringCollection)) {
        db.createCollection(monitoringCollection, {
            timeseries: {
                timeField: 'timestamp',
                metaField: 'database',
                granularity: 'hours'
            }
        });
        
        // Create index for efficient queries
        db[monitoringCollection].createIndex({ timestamp: -1 });
    }
    
    // Get current storage metrics
    const currentStats = db.stats();
    const collStats = db[COLLECTION_NAME].stats();
    
    const monitoringRecord = {
        timestamp: new Date(),
        database: DATABASE_NAME,
        metrics: {
            data_size_mb: currentStats.dataSize / (1024 * 1024),
            storage_size_mb: currentStats.storageSize / (1024 * 1024),
            index_size_mb: currentStats.indexSize / (1024 * 1024),
            collection_count: currentStats.collections,
            object_count: currentStats.objects,
            patient_vitals_count: collStats.count,
            patient_vitals_size_mb: collStats.size / (1024 * 1024),
            patient_vitals_avg_obj_size: collStats.avgObjSize
        }
    };
    
    // Insert monitoring record
    db[monitoringCollection].insertOne(monitoringRecord);
    
    // Get historical data for trend analysis
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const historicalData = db[monitoringCollection].find({
        timestamp: { $gte: startDate }
    }).sort({ timestamp: 1 }).toArray();
    
    if (historicalData.length > 1) {
        // Calculate growth rates
        const firstRecord = historicalData[0];
        const lastRecord = historicalData[historicalData.length - 1];
        
        const timeDiffDays = (lastRecord.timestamp - firstRecord.timestamp) / (1000 * 60 * 60 * 24);
        const dataGrowthMB = lastRecord.metrics.data_size_mb - firstRecord.metrics.data_size_mb;
        const dailyGrowthMB = dataGrowthMB / timeDiffDays;
        
        const growthAnalysis = {
            period_days: timeDiffDays.toFixed(2),
            total_growth_mb: dataGrowthMB.toFixed(2),
            daily_growth_mb: dailyGrowthMB.toFixed(2),
            projected_30d_growth_mb: (dailyGrowthMB * 30).toFixed(2),
            projected_90d_growth_mb: (dailyGrowthMB * 90).toFixed(2),
            growth_rate_percent: (dataGrowthMB / firstRecord.metrics.data_size_mb * 100).toFixed(2)
        };
        
        // Print growth analysis
        print("Storage Growth Analysis:");
        print("-".repeat(60));
        print(`Analysis Period: ${growthAnalysis.period_days} days`);
        print(`Total Growth: ${growthAnalysis.total_growth_mb} MB`);
        print(`Daily Growth: ${growthAnalysis.daily_growth_mb} MB/day`);
        print(`Projected 30-day Growth: ${growthAnalysis.projected_30d_growth_mb} MB`);
        print(`Projected 90-day Growth: ${growthAnalysis.projected_90d_growth_mb} MB`);
        print(`Growth Rate: ${growthAnalysis.growth_rate_percent}%`);
        
        // Generate alerts if growth is high
        if (dailyGrowthMB > 1000) {
            print("\n⚠️  ALERT: High storage growth detected!");
            print("   Consider implementing data retention policies or archiving.");
        }
        
        if (growthAnalysis.projected_90d_growth_mb > 100000) {
            print("\n⚠️  ALERT: Projected storage growth exceeds 100GB in 90 days!");
            print("   Review data ingestion patterns and storage planning.");
        }
        
        return growthAnalysis;
    } else {
        print("Insufficient historical data for trend analysis.");
        print("Monitoring record saved. Check back in a few days for growth analysis.");
        return null;
    }
}

// Main function to run comprehensive storage analysis
function runComprehensiveStorageAnalysis() {
    print("=".repeat(60));
    print("MONGODB STORAGE EFFICIENCY ANALYSIS");
    print("=".repeat(60));
    print(`Database: ${DATABASE_NAME}`);
    print(`Collection: ${COLLECTION_NAME}`);
    print(`Timestamp: ${new Date()}`);
    print("");
    
    // Run comprehensive analysis
    const report = generateStorageEfficiencyReport();
    
    // Monitor storage growth
    print("\n" + "=".repeat(60));
    print("STORAGE GROWTH MONITORING");
    print("=".repeat(60));
    const growthAnalysis = monitorStorageGrowth(30);
    
    // Generate final recommendations
    print("\n" + "=".repeat(60));
    print("FINAL RECOMMENDATIONS");
    print("=".repeat(60));
    
    const finalRecommendations = [];
    
    // Based on storage efficiency
    if (parseFloat(report.summary.fragmentation) > 20) {
        finalRecommendations.push({
            priority: 1,
            action: "Schedule regular compaction",
            frequency: "Weekly",
            impact: "Reduces fragmentation, improves performance"
        });
    }
    
    // Based on index efficiency
    if (report.analyses.index_efficiency.unusedIndexes > 0) {
        finalRecommendations.push({
            priority: 2,
            action: "Remove unused indexes",
            indexes: report.analyses.index_efficiency.indexes
                .filter(idx => idx.operations === "0")
                .map(idx => idx.name),
            impact: "Reduces storage, improves write performance"
        });
    }
    
    // Based on growth analysis
    if (growthAnalysis && parseFloat(growthAnalysis.daily_growth_mb) > 500) {
        finalRecommendations.push({
            priority: 3,
            action: "Implement data retention policy",
            suggestion: "Archive data older than 90 days",
            impact: "Controls storage growth"
        });
    }
    
    // Based on document structure
    const optionalFields = report.analyses.document_structure.recommendations
        .filter(rec => rec.type === 'optional_field');
    
    if (optionalFields.length > 0) {
        finalRecommendations.push({
            priority: 4,
            action: "Standardize document schema",
            fields: optionalFields.map(f => f.field),
            impact: "Improves compression and query performance"
        });
    }
    
    // Print final recommendations
    if (finalRecommendations.length > 0) {
        finalRecommendations
            .sort((a, b) => a.priority - b.priority)
            .forEach(rec => {
                print(`${rec.priority}. ${rec.action}`);
                if (rec.frequency) print(`   Frequency: ${rec.frequency}`);
                if (rec.indexes) print(`   Indexes: ${rec.indexes.join(', ')}`);
                if (rec.fields) print(`   Fields: ${rec.fields.join(', ')}`);
                if (rec.suggestion) print(`   Suggestion: ${rec.suggestion}`);
                print(`   Impact: ${rec.impact}`);
                print("");
            });
    } else {
        print("No major recommendations. Storage configuration appears optimal.");
    }
    
    print("\nAnalysis complete!");
    print("=".repeat(60));
    
    return {
        report: report,
        growth_analysis: growthAnalysis,
        recommendations: finalRecommendations
    };
}

// Execute the comprehensive analysis
runComprehensiveStorageAnalysis();