-- POSTGRESQL INGESTION PERFORMANCE TEST
-- Measures data ingestion performance with/without RLS

\c health_iot_benchmark;

-- 1. TEST SETUP AND CONFIGURATION
-- ============================================
DO $$
DECLARE
    test_start TIMESTAMPTZ;
    test_end TIMESTAMPTZ;
    test_duration INTERVAL;
    batch_sizes INTEGER[] := ARRAY[100, 500, 1000, 5000, 10000];
    current_batch_size INTEGER;
    test_results JSONB := '[]'::JSONB;
    rls_enabled BOOLEAN;
    security_context VARCHAR;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'INGESTION PERFORMANCE BENCHMARK';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Testing various batch sizes with/without RLS';
    RAISE NOTICE 'Batch sizes: %', batch_sizes;
    RAISE NOTICE '';
    
    -- Create test table
    DROP TABLE IF EXISTS ingestion_performance_test;
    CREATE TABLE ingestion_performance_test (
        test_id SERIAL PRIMARY KEY,
        batch_size INTEGER NOT NULL,
        test_timestamp TIMESTAMPTZ NOT NULL,
        test_patient_id VARCHAR(20) NOT NULL,
        test_device_id VARCHAR(20),
        test_vital_type VARCHAR(30) NOT NULL,
        test_value DECIMAL(10,2) NOT NULL,
        is_alert BOOLEAN DEFAULT FALSE,
        test_department VARCHAR(30),
        security_context VARCHAR(50),
        rls_enabled BOOLEAN,
        ingestion_time TIMESTAMPTZ DEFAULT NOW()
    );
    
    -- Create indexes for test table
    CREATE INDEX idx_test_timestamp ON ingestion_performance_test(test_timestamp);
    CREATE INDEX idx_test_patient ON ingestion_performance_test(test_patient_id, test_timestamp);
    
    -- 2. TEST WITHOUT RLS (BASELINE)
    -- ============================================
    RAISE NOTICE 'PHASE 1: BASELINE (NO RLS)';
    RAISE NOTICE '-------------------------';
    
    -- Disable RLS on test table
    ALTER TABLE ingestion_performance_test DISABLE ROW LEVEL SECURITY;
    rls_enabled := FALSE;
    security_context := 'NO_SECURITY';
    
    FOREACH current_batch_size IN ARRAY batch_sizes LOOP
        RAISE NOTICE 'Testing batch size: %', current_batch_size;
        
        -- Clear test data
        DELETE FROM ingestion_performance_test 
        WHERE batch_size = current_batch_size;
        
        -- Start timing
        test_start := clock_timestamp();
        
        -- Insert test data
        INSERT INTO ingestion_performance_test (
            batch_size,
            test_timestamp,
            test_patient_id,
            test_device_id,
            test_vital_type,
            test_value,
            is_alert,
            test_department,
            security_context,
            rls_enabled
        )
        SELECT 
            current_batch_size,
            NOW() - (random() * INTERVAL '30 days'),
            'TEST_PATIENT_' || LPAD((seq % 100)::TEXT, 5, '0'),
            'TEST_DEVICE_' || LPAD((seq % 20)::TEXT, 3, '0'),
            CASE (seq % 5)
                WHEN 0 THEN 'heart_rate_bpm'
                WHEN 1 THEN 'blood_pressure_sys_mmhg'
                WHEN 2 THEN 'blood_pressure_dia_mmhg'
                WHEN 3 THEN 'spo2_percent'
                ELSE 'temperature_c'
            END,
            50 + random() * 150,
            random() < 0.05, -- 5% alert rate
            CASE (seq % 3)
                WHEN 0 THEN 'ICU'
                WHEN 1 THEN 'WARD'
                ELSE 'OUTPATIENT'
            END,
            security_context,
            rls_enabled
        FROM generate_series(1, current_batch_size) seq;
        
        -- End timing
        test_end := clock_timestamp();
        test_duration := test_end - test_start;
        
        -- Calculate metrics
        DECLARE
            ingestion_rate NUMERIC;
            avg_latency NUMERIC;
            records_inserted INTEGER;
        BEGIN
            SELECT COUNT(*) INTO records_inserted
            FROM ingestion_performance_test 
            WHERE batch_size = current_batch_size;
            
            ingestion_rate := current_batch_size / EXTRACT(EPOCH FROM test_duration);
            avg_latency := (EXTRACT(EPOCH FROM test_duration) * 1000) / current_batch_size;
            
            -- Store result
            test_results := test_results || jsonb_build_object(
                'test_id', currval('ingestion_performance_test_test_id_seq'),
                'batch_size', current_batch_size,
                'rls_enabled', rls_enabled,
                'security_context', security_context,
                'records_inserted', records_inserted,
                'duration_ms', EXTRACT(EPOCH FROM test_duration) * 1000,
                'ingestion_rate', ROUND(ingestion_rate, 2),
                'avg_latency_ms', ROUND(avg_latency, 3),
                'phase', 'baseline'
            );
            
            RAISE NOTICE '  Inserted % records in % ms', 
                current_batch_size, 
                ROUND(EXTRACT(EPOCH FROM test_duration) * 1000, 2);
            RAISE NOTICE '  Rate: % records/sec', ROUND(ingestion_rate, 2);
            RAISE NOTICE '  Avg latency: % ms/record', ROUND(avg_latency, 3);
        END;
        
        -- Small delay between tests
        PERFORM pg_sleep(0.5);
    END LOOP;
    
    -- 3. TEST WITH RLS ENABLED
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PHASE 2: WITH RLS ENABLED';
    RAISE NOTICE '------------------------';
    
    -- Enable RLS and create policy
    ALTER TABLE ingestion_performance_test ENABLE ROW LEVEL SECURITY;
    
    -- Create RLS policy (similar to main table)
    DROP POLICY IF EXISTS test_rls_policy ON ingestion_performance_test;
    CREATE POLICY test_rls_policy ON ingestion_performance_test
        FOR ALL TO PUBLIC
        USING (test_department = current_setting('app.test_department', true))
        WITH CHECK (test_department = current_setting('app.test_department', true));
    
    rls_enabled := TRUE;
    
    -- Test with different security contexts
    DECLARE
        security_contexts VARCHAR[] := ARRAY['ICU', 'WARD', 'OUTPATIENT'];
        current_context VARCHAR;
    BEGIN
        FOREACH current_context IN ARRAY security_contexts LOOP
            RAISE NOTICE '';
            RAISE NOTICE 'Security context: %', current_context;
            RAISE NOTICE '-------------------';
            
            -- Set security context
            PERFORM set_config('app.test_department', current_context, false);
            security_context := current_context;
            
            FOREACH current_batch_size IN ARRAY batch_sizes LOOP
                RAISE NOTICE '  Batch size: %', current_batch_size;
                
                -- Clear previous test data for this batch
                DELETE FROM ingestion_performance_test 
                WHERE batch_size = current_batch_size 
                AND security_context = current_context;
                
                -- Start timing
                test_start := clock_timestamp();
                
                -- Insert with RLS context
                INSERT INTO ingestion_performance_test (
                    batch_size,
                    test_timestamp,
                    test_patient_id,
                    test_device_id,
                    test_vital_type,
                    test_value,
                    is_alert,
                    test_department,
                    security_context,
                    rls_enabled
                )
                SELECT 
                    current_batch_size,
                    NOW() - (random() * INTERVAL '30 days'),
                    'TEST_PATIENT_' || LPAD((seq % 100)::TEXT, 5, '0'),
                    'TEST_DEVICE_' || LPAD((seq % 20)::TEXT, 3, '0'),
                    CASE (seq % 5)
                        WHEN 0 THEN 'heart_rate_bpm'
                        WHEN 1 THEN 'blood_pressure_sys_mmhg'
                        WHEN 2 THEN 'blood_pressure_dia_mmhg'
                        WHEN 3 THEN 'spo2_percent'
                        ELSE 'temperature_c'
                    END,
                    50 + random() * 150,
                    random() < 0.05,
                    current_context, -- All records for current department
                    security_context,
                    rls_enabled
                FROM generate_series(1, current_batch_size) seq;
                
                -- End timing
                test_end := clock_timestamp();
                test_duration := test_end - test_start;
                
                -- Calculate metrics
                DECLARE
                    ingestion_rate NUMERIC;
                    avg_latency NUMERIC;
                    records_inserted INTEGER;
                    security_overhead NUMERIC;
                BEGIN
                    SELECT COUNT(*) INTO records_inserted
                    FROM ingestion_performance_test 
                    WHERE batch_size = current_batch_size 
                    AND security_context = current_context;
                    
                    ingestion_rate := current_batch_size / EXTRACT(EPOCH FROM test_duration);
                    avg_latency := (EXTRACT(EPOCH FROM test_duration) * 1000) / current_batch_size;
                    
                    -- Find baseline for comparison
                    SELECT (duration_ms * 1000) INTO security_overhead
                    FROM jsonb_array_elements(test_results) r
                    WHERE (r->>'batch_size')::INTEGER = current_batch_size
                    AND r->>'phase' = 'baseline';
                    
                    -- Calculate overhead
                    security_overhead := (EXTRACT(EPOCH FROM test_duration) * 1000) - security_overhead;
                    
                    -- Store result
                    test_results := test_results || jsonb_build_object(
                        'test_id', currval('ingestion_performance_test_test_id_seq'),
                        'batch_size', current_batch_size,
                        'rls_enabled', rls_enabled,
                        'security_context', security_context,
                        'records_inserted', records_inserted,
                        'duration_ms', EXTRACT(EPOCH FROM test_duration) * 1000,
                        'ingestion_rate', ROUND(ingestion_rate, 2),
                        'avg_latency_ms', ROUND(avg_latency, 3),
                        'security_overhead_ms', ROUND(security_overhead, 3),
                        'phase', 'rls_enabled'
                    );
                    
                    RAISE NOTICE '    Duration: % ms', 
                        ROUND(EXTRACT(EPOCH FROM test_duration) * 1000, 2);
                    RAISE NOTICE '    Rate: % records/sec', ROUND(ingestion_rate, 2);
                    RAISE NOTICE '    Overhead: % ms', ROUND(security_overhead, 3);
                END;
                
                -- Small delay
                PERFORM pg_sleep(0.3);
            END LOOP;
        END LOOP;
    END;
    
    -- 4. TEST WITH COMPLEX RLS POLICIES
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PHASE 3: COMPLEX RLS POLICIES';
    RAISE NOTICE '----------------------------';
    
    -- Add more complex policies
    DROP POLICY IF EXISTS test_complex_policy ON ingestion_performance_test;
    CREATE POLICY test_complex_policy ON ingestion_performance_test
        FOR ALL TO PUBLIC
        USING (
            test_department = current_setting('app.test_department', true)
            AND (
                current_setting('app.user_role', true) = 'doctor_role'
                OR (
                    current_setting('app.user_role', true) = 'nurse_role'
                    AND test_vital_type NOT IN ('psychiatric_notes', 'sensitive_data')
                )
            )
        );
    
    -- Test with different user roles
    DECLARE
        user_roles VARCHAR[] := ARRAY['doctor_role', 'nurse_role', 'patient_role'];
        current_role VARCHAR;
    BEGIN
        FOREACH current_role IN ARRAY user_roles LOOP
            RAISE NOTICE '';
            RAISE NOTICE 'User role: %', current_role;
            RAISE NOTICE 'Department: ICU';
            RAISE NOTICE '-------------------';
            
            -- Set context
            PERFORM set_config('app.test_department', 'ICU', false);
            PERFORM set_config('app.user_role', current_role, false);
            
            -- Test medium batch size
            current_batch_size := 1000;
            
            -- Clear data
            DELETE FROM ingestion_performance_test 
            WHERE batch_size = current_batch_size 
            AND security_context = 'COMPLEX_RLS';
            
            -- Start timing
            test_start := clock_timestamp();
            
            -- Insert with complex RLS
            BEGIN
                INSERT INTO ingestion_performance_test (
                    batch_size,
                    test_timestamp,
                    test_patient_id,
                    test_device_id,
                    test_vital_type,
                    test_value,
                    is_alert,
                    test_department,
                    security_context,
                    rls_enabled
                )
                SELECT 
                    current_batch_size,
                    NOW() - (random() * INTERVAL '30 days'),
                    'TEST_PATIENT_' || LPAD((seq % 100)::TEXT, 5, '0'),
                    'TEST_DEVICE_' || LPAD((seq % 20)::TEXT, 3, '0'),
                    CASE 
                        WHEN current_role = 'nurse_role' AND seq % 10 = 0 THEN 'regular_vital'
                        ELSE CASE (seq % 5)
                            WHEN 0 THEN 'heart_rate_bpm'
                            WHEN 1 THEN 'blood_pressure_sys_mmhg'
                            WHEN 2 THEN 'blood_pressure_dia_mmhg'
                            WHEN 3 THEN 'spo2_percent'
                            ELSE 'temperature_c'
                        END
                    END,
                    50 + random() * 150,
                    random() < 0.05,
                    'ICU',
                    'COMPLEX_RLS',
                    TRUE
                FROM generate_series(1, current_batch_size) seq;
                
                test_end := clock_timestamp();
                test_duration := test_end - test_start;
                
                -- Calculate and store
                DECLARE
                    ingestion_rate NUMERIC;
                    avg_latency NUMERIC;
                    records_inserted INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO records_inserted
                    FROM ingestion_performance_test 
                    WHERE batch_size = current_batch_size 
                    AND security_context = 'COMPLEX_RLS';
                    
                    ingestion_rate := current_batch_size / EXTRACT(EPOCH FROM test_duration);
                    avg_latency := (EXTRACT(EPOCH FROM test_duration) * 1000) / current_batch_size;
                    
                    test_results := test_results || jsonb_build_object(
                        'test_id', currval('ingestion_performance_test_test_id_seq'),
                        'batch_size', current_batch_size,
                        'rls_enabled', TRUE,
                        'security_context', 'COMPLEX_RLS',
                        'user_role', current_role,
                        'records_inserted', records_inserted,
                        'duration_ms', EXTRACT(EPOCH FROM test_duration) * 1000,
                        'ingestion_rate', ROUND(ingestion_rate, 2),
                        'avg_latency_ms', ROUND(avg_latency, 3),
                        'phase', 'complex_rls'
                    );
                    
                    RAISE NOTICE '  Batch: % records', current_batch_size;
                    RAISE NOTICE '  Duration: % ms', 
                        ROUND(EXTRACT(EPOCH FROM test_duration) * 1000, 2);
                    RAISE NOTICE '  Rate: % records/sec', ROUND(ingestion_rate, 2);
                END;
                
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE '  Failed: %', SQLERRM;
            END;
            
            PERFORM pg_sleep(0.5);
        END LOOP;
    END;
    
    -- 5. RESULTS ANALYSIS AND REPORTING
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'INGESTION PERFORMANCE ANALYSIS';
    RAISE NOTICE '============================================';
    
    -- Create results table
    DROP TABLE IF EXISTS ingestion_performance_results;
    CREATE TABLE ingestion_performance_results (
        result_id SERIAL PRIMARY KEY,
        test_timestamp TIMESTAMPTZ DEFAULT NOW(),
        batch_size INTEGER,
        rls_enabled BOOLEAN,
        security_context VARCHAR(50),
        user_role VARCHAR(50),
        records_inserted INTEGER,
        duration_ms NUMERIC(10,3),
        ingestion_rate NUMERIC(10,2),
        avg_latency_ms NUMERIC(10,3),
        security_overhead_ms NUMERIC(10,3),
        test_phase VARCHAR(50)
    );
    
    -- Insert all results
    INSERT INTO ingestion_performance_results (
        batch_size,
        rls_enabled,
        security_context,
        user_role,
        records_inserted,
        duration_ms,
        ingestion_rate,
        avg_latency_ms,
        security_overhead_ms,
        test_phase
    )
    SELECT 
        (r->>'batch_size')::INTEGER,
        (r->>'rls_enabled')::BOOLEAN,
        r->>'security_context',
        r->>'user_role',
        (r->>'records_inserted')::INTEGER,
        (r->>'duration_ms')::NUMERIC,
        (r->>'ingestion_rate')::NUMERIC,
        (r->>'avg_latency_ms')::NUMERIC,
        COALESCE((r->>'security_overhead_ms')::NUMERIC, 0),
        r->>'phase'
    FROM jsonb_array_elements(test_results) r;
    
    -- 6. GENERATE SUMMARY REPORT
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SUMMARY BY BATCH SIZE:';
    RAISE NOTICE '----------------------';
    
    FOR summary IN
        SELECT 
            batch_size,
            test_phase,
            COUNT(*) as test_count,
            ROUND(AVG(ingestion_rate), 2) as avg_rate,
            ROUND(MIN(ingestion_rate), 2) as min_rate,
            ROUND(MAX(ingestion_rate), 2) as max_rate,
            ROUND(AVG(avg_latency_ms), 3) as avg_latency,
            ROUND(AVG(security_overhead_ms), 3) as avg_overhead
        FROM ingestion_performance_results
        GROUP BY batch_size, test_phase
        ORDER BY batch_size, test_phase
    LOOP
        RAISE NOTICE 'Batch % - %: Avg rate=%/s, Latency=%ms, Overhead=%ms',
            summary.batch_size,
            summary.test_phase,
            summary.avg_rate,
            summary.avg_latency,
            summary.avg_overhead;
    END LOOP;
    
    -- 7. SECURITY OVERHEAD ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECURITY OVERHEAD ANALYSIS:';
    RAISE NOTICE '--------------------------';
    
    FOR overhead IN
        WITH baseline AS (
            SELECT 
                batch_size,
                AVG(duration_ms) as baseline_duration
            FROM ingestion_performance_results
            WHERE test_phase = 'baseline'
            GROUP BY batch_size
        ),
        secured AS (
            SELECT 
                batch_size,
                security_context,
                AVG(duration_ms) as secured_duration
            FROM ingestion_performance_results
            WHERE test_phase IN ('rls_enabled', 'complex_rls')
            GROUP BY batch_size, security_context
        )
        SELECT 
            s.batch_size,
            s.security_context,
            ROUND(s.secured_duration, 2) as secured_ms,
            ROUND(b.baseline_duration, 2) as baseline_ms,
            ROUND(s.secured_duration - b.baseline_duration, 2) as overhead_ms,
            ROUND((s.secured_duration - b.baseline_duration) / b.baseline_duration * 100, 2) as overhead_pct
        FROM secured s
        JOIN baseline b ON s.batch_size = b.batch_size
        ORDER BY s.batch_size, s.security_context
    LOOP
        RAISE NOTICE 'Batch % - %: Baseline=%ms, Secured=%ms, Overhead=%ms (%)',
            overhead.batch_size,
            overhead.security_context,
            overhead.baseline_ms,
            overhead.secured_ms,
            overhead.overhead_ms,
            overhead.overhead_pct || '%';
    END LOOP;
    
    -- 8. PERFORMANCE RECOMMENDATIONS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PERFORMANCE RECOMMENDATIONS:';
    RAISE NOTICE '---------------------------';
    
    -- Analyze optimal batch size
    DECLARE
        optimal_batch_size INTEGER;
        max_ingestion_rate NUMERIC;
    BEGIN
        SELECT 
            batch_size,
            MAX(ingestion_rate)
        INTO optimal_batch_size, max_ingestion_rate
        FROM ingestion_performance_results
        WHERE test_phase = 'baseline'
        GROUP BY batch_size
        ORDER BY MAX(ingestion_rate) DESC
        LIMIT 1;
        
        RAISE NOTICE '1. Optimal batch size: % records', optimal_batch_size;
        RAISE NOTICE '   Maximum ingestion rate: % records/sec', ROUND(max_ingestion_rate, 2);
    END;
    
    -- RLS impact analysis
    DECLARE
        avg_rls_overhead NUMERIC;
    BEGIN
        SELECT AVG(security_overhead_ms)
        INTO avg_rls_overhead
        FROM ingestion_performance_results
        WHERE security_overhead_ms > 0;
        
        IF avg_rls_overhead > 10 THEN
            RAISE NOTICE '2. RLS overhead is significant: % ms average', ROUND(avg_rls_overhead, 2);
            RAISE NOTICE '   Consider simplifying RLS policies for high-throughput systems';
        ELSE
            RAISE NOTICE '2. RLS overhead is minimal: % ms average', ROUND(avg_rls_overhead, 2);
            RAISE NOTICE '   RLS can be used without significant performance impact';
        END IF;
    END;
    
    -- Batch size vs overhead analysis
    RAISE NOTICE '3. Batch size impact on RLS overhead:';
    FOR batch_impact IN
        SELECT 
            batch_size,
            ROUND(AVG(security_overhead_ms), 2) as avg_overhead,
            ROUND(AVG(ingestion_rate), 2) as avg_rate
        FROM ingestion_performance_results
        WHERE security_overhead_ms > 0
        GROUP BY batch_size
        ORDER BY batch_size
    LOOP
        RAISE NOTICE '   Batch %: Overhead=%ms, Rate=%/sec',
            batch_impact.batch_size,
            batch_impact.avg_overhead,
            batch_impact.avg_rate;
    END LOOP;
    
    -- 9. CLEANUP
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'Cleaning up test data...';
    
    -- Drop test table
    DROP TABLE ingestion_performance_test;
    
    -- Reset configuration
    RESET app.test_department;
    RESET app.user_role;
    
    -- 10. FINAL REPORT
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'INGESTION PERFORMANCE TEST COMPLETE';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Results saved in: ingestion_performance_results';
    RAISE NOTICE '';
    RAISE NOTICE 'Key findings:';
    RAISE NOTICE '1. Baseline performance (no RLS) establishes maximum throughput';
    RAISE NOTICE '2. RLS adds predictable overhead based on policy complexity';
    RAISE NOTICE '3. Optimal batch size balances throughput and latency';
    RAISE NOTICE '4. Complex security policies have higher overhead';
    RAISE NOTICE '';
    RAISE NOTICE 'Use these results to:';
    RAISE NOTICE '• Tune batch sizes for production systems';
    RAISE NOTICE '• Evaluate RLS policy performance impact';
    RAISE NOTICE '• Size infrastructure based on expected load';
    RAISE NOTICE '============================================';
    
END $$;

-- 11. QUERY RESULTS FOR VERIFICATION
-- ============================================
-- Uncomment to view results

/*
-- View all results
SELECT 
    test_timestamp,
    batch_size,
    test_phase,
    security_context,
    user_role,
    records_inserted,
    duration_ms,
    ingestion_rate,
    avg_latency_ms,
    security_overhead_ms
FROM ingestion_performance_results
ORDER BY test_timestamp DESC, batch_size, test_phase;

-- Summary statistics
SELECT 
    batch_size,
    test_phase,
    COUNT(*) as test_count,
    ROUND(AVG(ingestion_rate), 2) as avg_rate,
    ROUND(STDDEV(ingestion_rate), 2) as stddev_rate,
    ROUND(AVG(avg_latency_ms), 3) as avg_latency,
    ROUND(AVG(security_overhead_ms), 3) as avg_overhead
FROM ingestion_performance_results
GROUP BY batch_size, test_phase
ORDER BY batch_size, test_phase;

-- Export to CSV (for reporting)
-- COPY (
--     SELECT * FROM ingestion_performance_results
-- ) TO '/tmp/ingestion_performance_results.csv' WITH CSV HEADER;
*/