// InfluxDB Storage Efficiency Analysis
// Analyzes storage usage, compression, and optimization opportunities


// 1. STORAGE UTILIZATION ANALYSIS
// ============================================

// Query 1: Total storage usage by bucket
bucketStorage = from(bucket: "_monitoring")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "disk")
  |> filter(fn: (r) => r._field == "used_percent")
  |> last()
  |> map(fn: (r) => ({r with 
    metric: "disk_usage_percent",
    value: r._value,
    unit: "percent"
  }))

// Query 2: Data volume by measurement
dataByMeasurement = from(bucket: "health_iot_metrics")
  |> range(start: -30d)
  |> group(columns: ["_measurement"])
  |> count()
  |> map(fn: (r) => ({r with 
    metric: "data_volume_by_measurement",
    measurement: r._measurement,
    count: r._value
  }))

// Query 3: Time-series data distribution
timeDistribution = from(bucket: "health_iot_metrics")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> window(every: 1d)
  |> count()
  |> map(fn: (r) => ({r with 
    metric: "daily_data_volume",
    date: string(v: r._start),
    count: r._value
  }))


// 2. DATA COMPRESSION ANALYSIS
// ============================================

// Query 4: Field value distribution analysis
fieldValueAnalysis = from(bucket: "health_iot_metrics")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> filter(fn: (r) => r._field == "vital_value")
  |> group(columns: ["vital_type"])
  |> histogram(bins: [0, 50, 100, 150, 200])
  |> map(fn: (r) => ({r with 
    metric: "value_distribution",
    vital_type: r.vital_type,
    bin: r.le,
    count: r._value
  }))

// Query 5: Tag cardinality analysis
tagCardinality = union(
  tables: [
    from(bucket: "health_iot_metrics")
      |> range(start: -7d)
      |> filter(fn: (r) => r._measurement == "patient_vitals")
      |> group(columns: ["patient_id"])
      |> count()
      |> map(fn: (r) => ({r with tag: "patient_id", count: r._value})),
    
    from(bucket: "health_iot_metrics")
      |> range(start: -7d)
      |> filter(fn: (r) => r._measurement == "patient_vitals")
      |> group(columns: ["vital_type"])
      |> count()
      |> map(fn: (r) => ({r with tag: "vital_type", count: r._value})),
    
    from(bucket: "health_iot_metrics")
      |> range(start: -7d)
      |> filter(fn: (r) => r._measurement == "patient_vitals")
      |> group(columns: ["patient_department"])
      |> count()
      |> map(fn: (r) => ({r with tag: "patient_department", count: r._value})),
    
    from(bucket: "health_iot_metrics")
      |> range(start: -7d)
      |> filter(fn: (r) => r._measurement == "patient_vitals")
      |> group(columns: ["device_id"])
      |> count()
      |> map(fn: (r) => ({r with tag: "device_id", count: r._value}))
  ]
)
|> group(columns: ["tag"])
|> sum(column: "count")
|> map(fn: (r) => ({r with 
  metric: "tag_cardinality",
  tag_name: r.tag,
  unique_values: r.count
}))


// 3. RETENTION POLICY ANALYSIS
// ============================================

// Query 6: Data age distribution
dataAgeDistribution = from(bucket: "health_iot_metrics")
  |> range(start: -90d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> map(fn: (r) => ({
    r with 
      age_days: float(v: int(v: now()) - int(v: r._time)) / 86400000000000.0
  }))
  |> histogram(bins: [0, 7, 30, 90])
  |> map(fn: (r) => ({r with 
    metric: "data_age_distribution",
    age_bucket: r.le,
    count: r._value
  }))

// Query 7: Retention policy compliance
retentionCompliance = dataAgeDistribution
  |> filter(fn: (r) => r.age_bucket == "90")
  |> map(fn: (r) => ({
      metric: "retention_compliance",
      status: if r.count > 0 then "data_older_than_90d" else "compliant",
      message: if r.count > 0 then 
        "Data older than 90 days found. Consider adjusting retention policy." 
        else "All data within 90 day retention period.",
      count: r.count
    }))


// 4. STORAGE OPTIMIZATION OPPORTUNITIES
// ============================================

// Query 8: Low cardinality tag identification
lowCardinalityTags = tagCardinality
  |> filter(fn: (r) => r.unique_values < 10)
  |> map(fn: (r) => ({
      metric: "low_cardinality_tags",
      tag: r.tag_name,
      unique_values: r.unique_values,
      recommendation: "Consider removing as separate tag if not needed for querying"
    }))

// Query 9: High cardinality tag identification
highCardinalityTags = tagCardinality
  |> filter(fn: (r) => r.unique_values > 1000)
  |> map(fn: (r) => ({
      metric: "high_cardinality_tags",
      tag: r.tag_name,
      unique_values: r.unique_values,
      recommendation: "High cardinality may impact performance. Consider using as field instead."
    }))

// Query 10: Sparse data detection
sparseDataDetection = from(bucket: "health_iot_metrics")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> filter(fn: (r) => r._field == "vital_value")
  |> group(columns: ["patient_id"])
  |> count()
  |> filter(fn: (r) => r._value < 10)  // Patients with less than 10 readings
  |> map(fn: (r) => ({
      metric: "sparse_data",
      patient_id: r.patient_id,
      reading_count: r._value,
      recommendation: "Consider consolidating or archiving sparse data"
    }))


// 5. COMPRESSION EFFECTIVENESS
// ============================================

// Query 11: Data type analysis for compression
dataTypeAnalysis = union(
  tables: [
    from(bucket: "health_iot_metrics")
      |> range(start: -1d)
      |> filter(fn: (r) => r._measurement == "patient_vitals")
      |> filter(fn: (r) => r._field == "vital_value")
      |> map(fn: (r) => ({
          data_type: "float",
          count: 1,
          avg_value: r._value,
          min_value: r._value,
          max_value: r._value
        }))
      |> group()
      |> map(fn: (r) => ({
          metric: "data_type_analysis",
          type: r.data_type,
          count: r.count,
          value_range: string(v: r.max_value - r.min_value),
          compression_potential: if (r.max_value - r.min_value) < 1000 then "high" else "medium"
        })),
    
    from(bucket: "health_iot_metrics")
      |> range(start: -1d)
      |> filter(fn: (r) => r._measurement == "patient_vitals")
      |> filter(fn: (r) => r._field == "is_alert")
      |> map(fn: (r) => ({data_type: "boolean", count: 1}))
      |> group()
      |> map(fn: (r) => ({
          metric: "data_type_analysis",
          type: r.data_type,
          count: r.count,
          compression_potential: "very_high"
        }))
  ]
)


// 6. STORAGE GROWTH FORECASTING
// ============================================

// Query 12: Storage growth trend
storageGrowth = from(bucket: "_monitoring")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "disk")
  |> filter(fn: (r) => r._field == "used")
  |> aggregateWindow(every: 1d, fn: last, createEmpty: false)
  |> map(fn: (r) => ({r with 
    metric: "daily_storage_usage",
    date: string(v: r._time),
    used_bytes: r._value
  }))
  |> difference(columns: ["used_bytes"])
  |> map(fn: (r) => ({r with 
    daily_growth: r.used_bytes,
    growth_mb: float(v: r.used_bytes) / 1048576.0
  }))

// Query 13: Growth rate calculation
growthRate = storageGrowth
  |> filter(fn: (r) => exists r.daily_growth)
  |> mean(column: "growth_mb")
  |> map(fn: (r) => ({r with 
    metric: "storage_growth_rate",
    avg_daily_growth_mb: r.growth_mb,
    projected_30d_growth_mb: r.growth_mb * 30.0,
    projected_90d_growth_mb: r.growth_mb * 90.0
  }))


// 7. CLEANUP RECOMMENDATIONS
// ============================================

// Query 14: Duplicate data detection
duplicateDetection = from(bucket: "health_iot_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> filter(fn: (r) => r._field == "vital_value")
  |> duplicate(column: "_value")
  |> count()
  |> map(fn: (r) => ({r with 
    metric: "duplicate_values",
    count: r._value,
    recommendation: if r._value > 100 then 
      "Consider adding deduplication logic" 
      else "Duplicate level acceptable"
  }))

// Query 15: Orphaned data identification
// (Data without recent activity)
orphanedData = from(bucket: "health_iot_metrics")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> group(columns: ["patient_id"])
  |> last()
  |> map(fn: (r) => ({r with 
    days_since_last_reading: float(v: int(v: now()) - int(v: r._time)) / 86400000000000.0
  }))
  |> filter(fn: (r) => r.days_since_last_reading > 14)
  |> map(fn: (r) => ({r with 
    metric: "orphaned_data",
    patient_id: r.patient_id,
    days_inactive: r.days_since_last_reading,
    recommendation: "Consider archiving or removing inactive patient data"
  }))


// 8. PERFORMANCE VS STORAGE TRADEOFFS
// ============================================

// Query 16: Query performance by data volume
queryPerformance = from(bucket: "_monitoring")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "http_request_duration_seconds")
  |> filter(fn: (r) => r._field == "mean")
  |> map(fn: (r) => ({r with 
    metric: "query_performance",
    endpoint: r.endpoint,
    avg_response_time: r._value
  }))

// Query 17: Storage vs performance correlation
storagePerformance = join(
  tables: {
    storage: storageGrowth 
      |> filter(fn: (r) => exists r.growth_mb)
      |> mean(column: "growth_mb")
      |> map(fn: (r) => ({avg_growth: r.growth_mb})),
    
    performance: queryPerformance 
      |> filter(fn: (r) => r.endpoint == "/api/v2/query")
      |> mean(column: "avg_response_time")
      |> map(fn: (r) => ({avg_query_time: r.avg_response_time}))
  },
  on: []
)
|> map(fn: (r) => ({r with 
  metric: "storage_performance_correlation",
  avg_daily_growth_mb: r.avg_growth,
  avg_query_time_ms: r.avg_query_time * 1000.0,
  analysis: if r.avg_growth > 1000 and r.avg_query_time > 1.0 then 
    "High growth correlated with slower queries" 
    else "Performance within acceptable range"
}))


// 9. OPTIMIZATION RECOMMENDATIONS
// ============================================

// Query 18: Generate storage optimization recommendations
optimizationRecommendations = union(
  tables: [
    lowCardinalityTags
      |> map(fn: (r) => ({
          category: "tag_optimization",
          recommendation: r.recommendation,
          details: "Tag: " + r.tag + " has only " + string(v: r.unique_values) + " unique values",
          priority: "low"
        })),
    
    highCardinalityTags
      |> map(fn: (r) => ({
          category: "tag_optimization", 
          recommendation: r.recommendation,
          details: "Tag: " + r.tag + " has " + string(v: r.unique_values) + " unique values",
          priority: "high"
        })),
    
    retentionCompliance
      |> filter(fn: (r) => r.status == "data_older_than_90d")
      |> map(fn: (r) => ({
          category: "retention_policy",
          recommendation: "Review and adjust retention policies",
          details: "Found " + string(v: r.count) + " data points older than 90 days",
          priority: "medium"
        })),
    
    sparseDataDetection
      |> limit(n: 5)
      |> map(fn: (r) => ({
          category: "data_consolidation",
          recommendation: r.recommendation,
          details: "Patient " + r.patient_id + " has only " + string(v: r.reading_count) + " readings",
          priority: "low"
        }))
  ]
)
|> map(fn: (r) => ({r with 
  timestamp: now(),
  database: "InfluxDB",
  component: "storage"
}))


// 10. SUMMARY REPORT
// ============================================

// Combine key metrics into summary report
storageSummary = union(
  tables: [
    bucketStorage |> keep(columns: ["metric", "value", "unit"]),
    
    dataByMeasurement 
      |> filter(fn: (r) => r.measurement == "patient_vitals")
      |> map(fn: (r) => ({
          metric: "total_data_points",
          value: float(v: r.count),
          unit: "points"
        })),
    
    growthRate 
      |> map(fn: (r) => ({
          metric: "avg_daily_growth",
          value: r.avg_daily_growth_mb,
          unit: "MB/day"
        })),
    
    tagCardinality 
      |> filter(fn: (r) => r.tag_name == "patient_id")
      |> map(fn: (r) => ({
          metric: "unique_patients",
          value: float(v: r.unique_values),
          unit: "patients"
        })),
    
    dataAgeDistribution 
      |> filter(fn: (r) => r.age_bucket == "7")
      |> map(fn: (r) => ({
          metric: "recent_data_percentage",
          value: (float(v: r.count) / float(v: sum(table: {}))) * 100.0,
          unit: "percent"
        }))
  ]
)
|> map(fn: (r) => ({r with 
  report_time: now(),
  analysis_period: "-30d"
}))


// 11. EXPORT AND YIELD RESULTS
// ============================================

// Export summary to benchmark bucket
storageSummary
  |> map(fn: (r) => ({r with 
    _measurement: "storage_efficiency",
    _field: "value",
    _value: r.value
  }))
  |> drop(columns: ["value"])
  |> to(bucket: "benchmark_results", org: "HealthIoT")

// Export recommendations
optimizationRecommendations
  |> map(fn: (r) => ({r with 
    _measurement: "storage_recommendations",
    _field: "priority",
    _value: 1.0
  }))
  |> to(bucket: "benchmark_results", org: "HealthIoT")

// Yield results for display
bucketStorage |> yield(name: "disk_usage")
dataByMeasurement |> yield(name: "data_by_measurement")
timeDistribution |> yield(name: "time_distribution")
tagCardinality |> yield(name: "tag_cardinality")
dataAgeDistribution |> yield(name: "data_age_distribution")
retentionCompliance |> yield(name: "retention_compliance")
lowCardinalityTags |> yield(name: "low_cardinality_tags")
highCardinalityTags |> yield(name: "high_cardinality_tags")
sparseDataDetection |> yield(name: "sparse_data_detection")
storageGrowth |> yield(name: "storage_growth")
growthRate |> yield(name: "growth_rate")
duplicateDetection |> yield(name: "duplicate_detection")
orphanedData |> yield(name: "orphaned_data")
storagePerformance |> yield(name: "storage_performance")
optimizationRecommendations |> yield(name: "optimization_recommendations")
storageSummary |> yield(name: "storage_summary")


// 12. ALERTING CONDITIONS
// ============================================

// Alert on high disk usage
highDiskUsageAlert = bucketStorage
  |> filter(fn: (r) => r.value > 80.0)
  |> map(fn: (r) => ({r with 
    alert: "high_disk_usage",
    severity: "critical",
    message: "Disk usage at " + string(v: r.value) + "% - consider cleanup or expansion",
    timestamp: now()
  }))

// Alert on rapid growth
rapidGrowthAlert = growthRate
  |> filter(fn: (r) => r.avg_daily_growth_mb > 1000.0)
  |> map(fn: (r) => ({r with 
    alert: "rapid_storage_growth",
    severity: "warning",
    message: "Storage growing at " + string(v: r.avg_daily_growth_mb) + " MB/day",
    timestamp: now()
  }))

// Alert on retention policy violation
retentionAlert = retentionCompliance
  |> filter(fn: (r) => r.status == "data_older_than_90d")
  |> map(fn: (r) => ({r with 
    alert: "retention_policy_violation",
    severity: "warning",
    message: "Data older than retention policy found",
    timestamp: now()
  }))

// Yield alerts
highDiskUsageAlert |> yield(name: "high_disk_usage_alerts")
rapidGrowthAlert |> yield(name: "rapid_growth_alerts")
retentionAlert |> yield(name: "retention_alerts")