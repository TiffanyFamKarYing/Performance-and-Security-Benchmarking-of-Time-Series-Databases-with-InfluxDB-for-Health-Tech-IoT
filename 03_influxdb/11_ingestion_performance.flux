// InfluxDB Ingestion Performance Analysis
// This Flux script analyzes ingestion performance and metrics


// 1. INGESTION RATE ANALYSIS
// ============================================

// Query 1: Overall ingestion rate
ingestionRate = from(bucket: "health_iot_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> aggregateWindow(every: 1m, fn: count, createEmpty: false)
  |> map(fn: (r) => ({r with _value: float(v: r._value) / 60.0}))
  |> mean()
  |> map(fn: (r) => ({r with 
    metric: "ingestion_rate",
    value: r._value,
    unit: "points/second"
  }))

// Query 2: Ingestion rate by vital type
ingestionByType = from(bucket: "health_iot_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> group(columns: ["vital_type"])
  |> aggregateWindow(every: 5m, fn: count, createEmpty: false)
  |> map(fn: (r) => ({r with _value: float(v: r._value) / 300.0}))
  |> mean()
  |> map(fn: (r) => ({r with 
    metric: "ingestion_rate_by_type",
    type: r.vital_type,
    value: r._value,
    unit: "points/second"
  }))

// Query 3: Ingestion rate by department
ingestionByDept = from(bucket: "health_iot_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> group(columns: ["patient_department"])
  |> aggregateWindow(every: 5m, fn: count, createEmpty: false)
  |> map(fn: (r) => ({r with _value: float(v: r._value) / 300.0}))
  |> mean()
  |> map(fn: (r) => ({r with 
    metric: "ingestion_rate_by_dept",
    department: r.patient_department,
    value: r._value,
    unit: "points/second"
  }))


// 2. DATA DISTRIBUTION ANALYSIS
// ============================================

// Query 4: Data volume over time
dataVolume = from(bucket: "health_iot_metrics")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> aggregateWindow(every: 1h, fn: count, createEmpty: false)
  |> map(fn: (r) => ({r with 
    metric: "hourly_data_volume",
    hour: r._stop,
    count: r._value
  }))

// Query 5: Data distribution by time of day
hourlyDistribution = from(bucket: "health_iot_metrics")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> map(fn: (r) => ({r with hour: string(v: hour(t: r._time))}))
  |> group(columns: ["hour"])
  |> count()
  |> map(fn: (r) => ({r with 
    metric: "data_distribution_by_hour",
    hour_of_day: r.hour,
    percentage: float(v: r._value) / float(v: sum(table: {}) / 100.0)
  }))


// 3. WRITE PERFORMANCE METRICS
// ============================================

// Note: InfluxDB doesn't directly expose write performance metrics
// We need to infer from timestamps and data distribution

// Query 6: Write latency estimation (based on timestamp distribution)
writeLatency = from(bucket: "health_iot_metrics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> map(fn: (r) => ({r with 
    latency: float(v: int(v: now()) - int(v: r._time)) / 1000000000.0
  }))
  |> mean(column: "latency")
  |> map(fn: (r) => ({r with 
    metric: "estimated_write_latency",
    value: r.latency,
    unit: "seconds"
  }))

// Query 7: Batch size distribution (inferred from timestamp clustering)
batchSizeAnalysis = from(bucket: "health_iot_metrics")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> window(every: 1s)
  |> count()
  |> group(columns: ["_start", "_stop"], mode: "by")
  |> map(fn: (r) => ({r with 
    metric: "batch_size_distribution",
    batch_size: r._value
  }))
  |> mean()
  |> map(fn: (r) => ({r with 
    avg_batch_size: r.batch_size,
    unit: "points/batch"
  }))


// 4. SYSTEM PERFORMANCE METRICS
// ============================================

// Query 8: Memory usage (from system metrics)
memoryUsage = from(bucket: "_monitoring")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "mem")
  |> filter(fn: (r) => r._field == "used_percent")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({r with 
    metric: "memory_usage",
    value: r._value,
    unit: "percent"
  }))

// Query 9: CPU usage
cpuUsage = from(bucket: "_monitoring")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "cpu")
  |> filter(fn: (r) => r._field == "usage_user")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({r with 
    metric: "cpu_usage",
    value: r._value,
    unit: "percent"
  }))

// Query 10: Disk I/O
diskIO = from(bucket: "_monitoring")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "diskio")
  |> filter(fn: (r) => r._field == "write_bytes")
  |> aggregateWindow(every: 1m, fn: sum, createEmpty: false)
  |> map(fn: (r) => ({r with 
    metric: "disk_write_throughput",
    value: r._value / 1024.0 / 1024.0,  // Convert to MB
    unit: "MB/minute"
  }))


// 5. PERFORMANCE ALERTS AND THRESHOLDS
// ============================================

// Query 11: High ingestion rate alert
highIngestionAlert = from(bucket: "health_iot_metrics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> aggregateWindow(every: 1m, fn: count, createEmpty: false)
  |> filter(fn: (r) => r._value > 10000)  // Alert if > 10k points/min
  |> map(fn: (r) => ({r with 
    alert: "high_ingestion_rate",
    severity: "warning",
    message: "High ingestion rate detected: " + string(v: r._value) + " points/min",
    timestamp: r._stop
  }))

// Query 12: Low ingestion rate alert
lowIngestionAlert = from(bucket: "health_iot_metrics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> aggregateWindow(every: 1m, fn: count, createEmpty: false)
  |> filter(fn: (r) => r._value < 100)  // Alert if < 100 points/min
  |> map(fn: (r) => ({r with 
    alert: "low_ingestion_rate",
    severity: "warning",
    message: "Low ingestion rate detected: " + string(v: r._value) + " points/min",
    timestamp: r._stop
  }))

// Query 13: High latency alert
highLatencyAlert = from(bucket: "health_iot_metrics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "patient_vitals")
  |> map(fn: (r) => ({r with 
    latency: float(v: int(v: now()) - int(v: r._time)) / 1000000000.0
  }))
  |> mean(column: "latency")
  |> filter(fn: (r) => r.latency > 5.0)  // Alert if latency > 5 seconds
  |> map(fn: (r) => ({r with 
    alert: "high_write_latency",
    severity: "critical",
    message: "High write latency detected: " + string(v: r.latency) + " seconds",
    timestamp: now()
  }))


// 6. PERFORMANCE SUMMARY REPORT
// ============================================

// Combine all metrics into a summary
summaryReport = union(
  tables: [
    ingestionRate |> keep(columns: ["metric", "value", "unit"]),
    ingestionByType |> keep(columns: ["metric", "type", "value", "unit"]),
    ingestionByDept |> keep(columns: ["metric", "department", "value", "unit"]),
    writeLatency |> keep(columns: ["metric", "value", "unit"]),
    batchSizeAnalysis |> keep(columns: ["metric", "avg_batch_size", "unit"]),
    memoryUsage |> keep(columns: ["metric", "value", "unit"]),
    cpuUsage |> keep(columns: ["metric", "value", "unit"])
  ]
)
|> map(fn: (r) => ({r with 
  report_time: now(),
  analysis_period: "-1h"
}))


// 7. EXPORT RESULTS
// ============================================

// Export summary report to a bucket for persistence
summaryReport
  |> map(fn: (r) => ({r with 
    _measurement: "ingestion_performance",
    _field: "value",
    _value: r.value
  }))
  |> drop(columns: ["value"])
  |> to(bucket: "benchmark_results", org: "HealthIoT")

// Yield results for display
ingestionRate |> yield(name: "overall_ingestion_rate")
ingestionByType |> yield(name: "ingestion_by_type")
ingestionByDept |> yield(name: "ingestion_by_department")
dataVolume |> yield(name: "data_volume_over_time")
hourlyDistribution |> yield(name: "hourly_distribution")
writeLatency |> yield(name: "write_latency")
batchSizeAnalysis |> yield(name: "batch_size_analysis")
memoryUsage |> yield(name: "memory_usage")
cpuUsage |> yield(name: "cpu_usage")
diskIO |> yield(name: "disk_io")

// Yield alerts
highIngestionAlert |> yield(name: "high_ingestion_alerts")
lowIngestionAlert |> yield(name: "low_ingestion_alerts")
highLatencyAlert |> yield(name: "high_latency_alerts")

// Yield summary report
summaryReport |> yield(name: "performance_summary")


// 8. PERFORMANCE RECOMMENDATIONS
// ============================================

// Generate recommendations based on performance metrics
performanceRecommendations = union(
  tables: [
    ingestionRate 
      |> map(fn: (r) => ({
          recommendation: "Consider increasing batch size if rate < 1000 points/sec",
          condition: r._value < 1000,
          priority: "medium"
        }))
      |> filter(fn: (r) => r.condition),
    
    writeLatency 
      |> map(fn: (r) => ({
          recommendation: "Optimize write path if latency > 2 seconds",
          condition: r._value > 2.0,
          priority: "high"
        }))
      |> filter(fn: (r) => r.condition),
    
    memoryUsage 
      |> map(fn: (r) => ({
          recommendation: "Increase memory allocation if usage > 80%",
          condition: r._value > 80.0,
          priority: "high"
        }))
      |> filter(fn: (r) => r.condition),
    
    cpuUsage 
      |> map(fn: (r) => ({
          recommendation: "Consider scaling if CPU usage > 70%",
          condition: r._value > 70.0,
          priority: "medium"
        }))
      |> filter(fn: (r) => r.condition)
  ]
)
|> map(fn: (r) => ({r with 
  analysis_time: now(),
  database: "InfluxDB",
  component: "ingestion"
}))

performanceRecommendations |> yield(name: "performance_recommendations")