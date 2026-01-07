-- POSTGRESQL DATA IMPORT SCRIPT
-- Health IoT Dataset Import with Performance Measurement

\c health_iot_benchmark;

-- 1. PREPARE FOR DATA IMPORT
-- ============================================
DO $$
DECLARE
    import_start_time TIMESTAMPTZ;
    import_end_time TIMESTAMPTZ;
    import_duration INTERVAL;
    records_imported BIGINT;
    import_rate NUMERIC;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'HEALTH IOT DATA IMPORT STARTING';
    RAISE NOTICE '============================================';
    
    -- Clear existing data for fresh import
    RAISE NOTICE 'Clearing existing data...';
    TRUNCATE TABLE patient_vitals CASCADE;
    TRUNCATE TABLE audit_logs CASCADE;
    
    -- Refresh materialized views
    PERFORM refresh_materialized_views();
    
    -- 2. MEASURED DATA IMPORT USING COPY
    -- ============================================
    RAISE NOTICE 'Starting data import...';
    import_start_time := clock_timestamp();
    
    -- Import data using COPY command (fastest method)
    COPY patient_vitals (
        measurement_time,
        patient_id,
        patient_department,
        device_id,
        vital_type,
        vital_value,
        is_alert,
        data_classification
    )
    FROM 'C:/Users/Tiffany/Desktop/DBMS Sample/health_iot_database_benchmarking/01_dataset/health_iot_dataset.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ',',
        NULL '',
        ENCODING 'UTF8'
    );
    
    import_end_time := clock_timestamp();
    import_duration := import_end_time - import_start_time;
    
    -- 3. CALCULATE IMPORT STATISTICS
    -- ============================================
    SELECT COUNT(*) INTO records_imported FROM patient_vitals;
    import_rate := records_imported / EXTRACT(EPOCH FROM import_duration);
    
    RAISE NOTICE '============================================';
    RAISE NOTICE 'IMPORT COMPLETE - STATISTICS';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Records imported: %', records_imported;
    RAISE NOTICE 'Import duration: %', import_duration;
    RAISE NOTICE 'Import rate: % records/second', ROUND(import_rate, 2);
    RAISE NOTICE 'Average latency: % ms/record', 
        ROUND((EXTRACT(EPOCH FROM import_duration) * 1000) / records_imported, 4);
    
    -- 4. POST-IMPORT DATA VALIDATION
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'DATA VALIDATION CHECKS:';
    RAISE NOTICE '-----------------------';
    
    -- Check 1: Total record count
    DECLARE
        expected_count INTEGER := 50000; -- Adjust based on your dataset
    BEGIN
        IF records_imported = expected_count THEN
            RAISE NOTICE '✓ Record count matches expected: %', records_imported;
        ELSE
            RAISE WARNING '✗ Record count mismatch. Expected: %, Got: %', 
                expected_count, records_imported;
        END IF;
    END;
    
    -- Check 2: Time range
    DECLARE
        min_time TIMESTAMPTZ;
        max_time TIMESTAMPTZ;
        time_span INTERVAL;
    BEGIN
        SELECT MIN(measurement_time), MAX(measurement_time)
        INTO min_time, max_time
        FROM patient_vitals;
        
        time_span := max_time - min_time;
        RAISE NOTICE '✓ Time range: % to %', min_time, max_time;
        RAISE NOTICE '  Time span: %', time_span;
    END;
    
    -- Check 3: Patient distribution
    DECLARE
        patient_count INTEGER;
        avg_readings NUMERIC;
    BEGIN
        SELECT COUNT(DISTINCT patient_id),
               AVG(patient_readings)
        INTO patient_count, avg_readings
        FROM (
            SELECT patient_id, COUNT(*) as patient_readings
            FROM patient_vitals
            GROUP BY patient_id
        ) sub;
        
        RAISE NOTICE '✓ Unique patients: %', patient_count;
        RAISE NOTICE '  Avg readings per patient: %', ROUND(avg_readings, 1);
    END;
    
    -- Check 4: Vital type distribution
    DECLARE
        vital_stats RECORD;
    BEGIN
        RAISE NOTICE '✓ Vital type distribution:';
        FOR vital_stats IN 
            SELECT 
                vital_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / records_imported, 1) as percentage,
                ROUND(AVG(vital_value), 2) as avg_value,
                MIN(vital_value) as min_value,
                MAX(vital_value) as max_value
            FROM patient_vitals
            GROUP BY vital_type
            ORDER BY count DESC
        LOOP
            RAISE NOTICE '  %: % records (%), avg: %, range: %-%',
                vital_stats.vital_type,
                vital_stats.count,
                vital_stats.percentage || '%',
                vital_stats.avg_value,
                vital_stats.min_value,
                vital_stats.max_value;
        END LOOP;
    END;
    
    -- Check 5: Alert statistics
    DECLARE
        alert_count INTEGER;
        alert_percentage NUMERIC;
    BEGIN
        SELECT COUNT(*), 
               COUNT(*) * 100.0 / records_imported
        INTO alert_count, alert_percentage
        FROM patient_vitals
        WHERE is_alert = TRUE;
        
        RAISE NOTICE '✓ Alert statistics:';
        RAISE NOTICE '  Total alerts: %', alert_count;
        RAISE NOTICE '  Alert rate: %', ROUND(alert_percentage, 2) || '%';
    END;
    
    -- Check 6: Department distribution
    DECLARE
        dept_stats RECORD;
    BEGIN
        RAISE NOTICE '✓ Department distribution:';
        FOR dept_stats IN
            SELECT 
                patient_department,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / records_imported, 1) as percentage,
                SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alerts
            FROM patient_vitals
            GROUP BY patient_department
            ORDER BY count DESC
        LOOP
            RAISE NOTICE '  %: % records (%), alerts: %',
                dept_stats.patient_department,
                dept_stats.count,
                dept_stats.percentage || '%',
                dept_stats.alerts;
        END LOOP;
    END;
    
    -- 5. UPDATE DERIVED DATA
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'Updating derived data...';
    
    -- Update alert severity based on values
    UPDATE patient_vitals
    SET alert_severity = 
        CASE 
            WHEN vital_type = 'heart_rate_bpm' AND vital_value > 160 THEN 5
            WHEN vital_type = 'heart_rate_bpm' AND vital_value < 40 THEN 5
            WHEN vital_type = 'spo2_percent' AND vital_value < 85 THEN 5
            WHEN vital_type = 'blood_pressure_sys_mmhg' AND vital_value > 180 THEN 4
            WHEN vital_type = 'blood_pressure_dia_mmhg' AND vital_value > 110 THEN 4
            WHEN is_alert = TRUE THEN 3
            ELSE 1
        END
    WHERE is_alert = TRUE;
    
    -- Set vital units
    UPDATE patient_vitals
    SET vital_unit = 
        CASE vital_type
            WHEN 'heart_rate_bpm' THEN 'bpm'
            WHEN 'blood_pressure_sys_mmhg' THEN 'mmHg'
            WHEN 'blood_pressure_dia_mmhg' THEN 'mmHg'
            WHEN 'spo2_percent' THEN '%'
            WHEN 'temperature_c' THEN '°C'
            WHEN 'respiratory_rate_bpm' THEN 'bpm'
            WHEN 'blood_glucose_mgdl' THEN 'mg/dL'
            ELSE 'units'
        END;
    
    -- 6. REFRESH MATERIALIZED VIEWS
    -- ============================================
    RAISE NOTICE 'Refreshing materialized views...';
    PERFORM refresh_materialized_views();
    
    -- 7. CREATE IMPORT SUMMARY TABLE
    -- ============================================
    CREATE TABLE IF NOT EXISTS import_history (
        import_id SERIAL PRIMARY KEY,
        import_timestamp TIMESTAMPTZ DEFAULT NOW(),
        dataset_name VARCHAR(100),
        records_imported INTEGER,
        import_duration INTERVAL,
        import_rate NUMERIC,
        avg_latency_ms NUMERIC,
        min_timestamp TIMESTAMPTZ,
        max_timestamp TIMESTAMPTZ,
        unique_patients INTEGER,
        unique_devices INTEGER,
        alert_count INTEGER,
        alert_percentage NUMERIC
    );
    
    INSERT INTO import_history (
        dataset_name,
        records_imported,
        import_duration,
        import_rate,
        avg_latency_ms,
        min_timestamp,
        max_timestamp,
        unique_patients,
        unique_devices,
        alert_count,
        alert_percentage
    )
    SELECT 
        'Health IoT Dataset',
        records_imported,
        import_duration,
        import_rate,
        ROUND((EXTRACT(EPOCH FROM import_duration) * 1000) / records_imported, 4),
        (SELECT MIN(measurement_time) FROM patient_vitals),
        (SELECT MAX(measurement_time) FROM patient_vitals),
        (SELECT COUNT(DISTINCT patient_id) FROM patient_vitals),
        (SELECT COUNT(DISTINCT device_id) FROM patient_vitals),
        (SELECT COUNT(*) FROM patient_vitals WHERE is_alert = TRUE),
        (SELECT COUNT(*) * 100.0 / records_imported 
         FROM patient_vitals WHERE is_alert = TRUE)
    ;
    
    -- 8. FINAL REPORT
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'IMPORT FINAL REPORT';
    RAISE NOTICE '============================================';
    
    -- Display import history
    RAISE NOTICE 'Recent imports:';
    FOR hist IN 
        SELECT 
            import_timestamp,
            records_imported,
            ROUND(EXTRACT(EPOCH FROM import_duration)::NUMERIC, 2) as duration_sec,
            ROUND(import_rate::NUMERIC, 2) as rate_per_sec,
            ROUND(avg_latency_ms::NUMERIC, 3) as latency_ms
        FROM import_history
        ORDER BY import_timestamp DESC
        LIMIT 5
    LOOP
        RAISE NOTICE '  %: % records in %s (%/s, % ms/record)',
            hist.import_timestamp,
            hist.records_imported,
            hist.duration_sec || 's',
            hist.rate_per_sec,
            hist.latency_ms;
    END LOOP;
    
    -- Table statistics
    RAISE NOTICE '';
    RAISE NOTICE 'Table statistics:';
    
    DECLARE
        table_size TEXT;
        index_size TEXT;
        total_size TEXT;
    BEGIN
        SELECT 
            pg_size_pretty(pg_relation_size('patient_vitals')),
            pg_size_pretty(pg_indexes_size('patient_vitals')),
            pg_size_pretty(pg_total_relation_size('patient_vitals'))
        INTO table_size, index_size, total_size;
        
        RAISE NOTICE '  Table size: %', table_size;
        RAISE NOTICE '  Index size: %', index_size;
        RAISE NOTICE '  Total size: %', total_size;
    END;
    
    -- Index usage statistics
    RAISE NOTICE '';
    RAISE NOTICE 'Index usage (since last reset):';
    
    DECLARE
        idx_stats RECORD;
    BEGIN
        FOR idx_stats IN
            SELECT 
                indexrelname as index_name,
                idx_scan as scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_size_pretty(pg_relation_size(indexrelid)) as size
            FROM pg_stat_user_indexes 
            WHERE relname = 'patient_vitals'
            ORDER BY idx_scan DESC
            LIMIT 10
        LOOP
            RAISE NOTICE '  %: % scans, % reads, size: %',
                idx_stats.index_name,
                idx_stats.scans,
                idx_stats.tuples_read,
                idx_stats.size;
        END LOOP;
    END;
    
    -- 9. PERFORMANCE RECOMMENDATIONS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PERFORMANCE RECOMMENDATIONS:';
    RAISE NOTICE '---------------------------';
    
    -- Check for potential issues
    DECLARE
        seq_scans BIGINT;
        idx_scans BIGINT;
        seq_ratio NUMERIC;
    BEGIN
        SELECT 
            COALESCE(seq_scan, 0),
            COALESCE(idx_scan, 0)
        INTO seq_scans, idx_scans
        FROM pg_stat_user_tables 
        WHERE relname = 'patient_vitals';
        
        IF seq_scans > 0 THEN
            seq_ratio := seq_scans::NUMERIC / GREATEST(seq_scans + idx_scans, 1);
            IF seq_ratio > 0.1 THEN
                RAISE NOTICE '⚠ High sequential scan ratio: %', ROUND(seq_ratio * 100, 1) || '%';
                RAISE NOTICE '  Consider adding missing indexes';
            END IF;
        END IF;
    END;
    
    -- Check table bloat
    DECLARE
        bloat_percentage NUMERIC;
    BEGIN
        SELECT 
            ROUND((n_dead_tup::NUMERIC / n_live_tup) * 100, 1)
        INTO bloat_percentage
        FROM pg_stat_user_tables 
        WHERE relname = 'patient_vitals';
        
        IF bloat_percentage > 20 THEN
            RAISE NOTICE '⚠ Table bloat detected: %', bloat_percentage || '%';
            RAISE NOTICE '  Consider running VACUUM ANALYZE';
        END IF;
    END;
    
    -- 10. CLEANUP AND MAINTENANCE
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'Running maintenance...';
    
    -- Analyze table for query planner
    ANALYZE patient_vitals;
    
    -- Update statistics
    RAISE NOTICE 'Statistics updated for query planner';
    
    -- 11. FINAL MESSAGE
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'DATA IMPORT COMPLETE SUCCESSFULLY';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'The Health IoT dataset has been imported and is ready for benchmarking.';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Test security policies with different user roles';
    RAISE NOTICE '2. Run performance benchmarks';
    RAISE NOTICE '3. Execute query performance tests';
    RAISE NOTICE '============================================';
    
END $$;