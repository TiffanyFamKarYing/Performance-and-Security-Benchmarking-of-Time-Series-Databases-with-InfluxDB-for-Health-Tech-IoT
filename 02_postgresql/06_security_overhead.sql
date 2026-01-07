-- POSTGRESQL SECURITY OVERHEAD ANALYSIS
-- Detailed analysis of RLS performance impact

\c health_iot_benchmark;

-- 1. COMPREHENSIVE SECURITY OVERHEAD ANALYSIS
-- ============================================
DO $$
DECLARE
    analysis_start TIMESTAMPTZ;
    analysis_end TIMESTAMPTZ;
    test_results JSONB := '[]'::JSONB;
    total_tests INTEGER := 0;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'SECURITY OVERHEAD ANALYSIS';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Detailed analysis of RLS performance impact';
    RAISE NOTICE '';
    
    -- Create analysis results table
    DROP TABLE IF EXISTS security_overhead_results;
    CREATE TABLE security_overhead_results (
        result_id SERIAL PRIMARY KEY,
        analysis_timestamp TIMESTAMPTZ DEFAULT NOW(),
        test_category VARCHAR(100),
        test_scenario VARCHAR(200),
        security_level VARCHAR(50),
        rls_enabled BOOLEAN,
        policy_complexity VARCHAR(50),
        execution_count INTEGER,
        total_time_ms NUMERIC(12,3),
        avg_time_ms NUMERIC(10,3),
        min_time_ms NUMERIC(10,3),
        max_time_ms NUMERIC(10,3),
        stddev_time_ms NUMERIC(10,3),
        overhead_ms NUMERIC(10,3),
        overhead_percentage NUMERIC(6,2),
        rows_processed INTEGER,
        cache_hit_ratio NUMERIC(6,2),
        index_utilization VARCHAR(50),
        recommendations TEXT
    );
    
    analysis_start := clock_timestamp();
    
    -- 2. BASELINE PERFORMANCE MEASUREMENT (NO SECURITY)
    -- ============================================
    RAISE NOTICE 'SECTION 1: BASELINE PERFORMANCE (NO SECURITY)';
    RAISE NOTICE '---------------------------------------------';
    
    -- Disable RLS for baseline tests
    ALTER TABLE patient_vitals DISABLE ROW LEVEL SECURITY;
    
    -- Test 1.1: Simple SELECT performance
    RAISE NOTICE 'Test 1.1: Simple SELECT without RLS';
    PERFORM measure_query_performance(
        'SELECT * FROM patient_vitals WHERE patient_department = ''ICU'' LIMIT 1000',
        'simple_select',
        'no_security',
        FALSE,
        'simple'
    );
    
    -- Test 1.2: Aggregation performance
    RAISE NOTICE 'Test 1.2: Aggregation without RLS';
    PERFORM measure_query_performance(
        'SELECT patient_department, COUNT(*), AVG(vital_value) 
         FROM patient_vitals 
         WHERE measurement_time >= NOW() - INTERVAL ''7 days''
         GROUP BY patient_department',
        'aggregation',
        'no_security',
        FALSE,
        'simple'
    );
    
    -- Test 1.3: JOIN performance
    RAISE NOTICE 'Test 1.3: JOIN without RLS';
    PERFORM measure_query_performance(
        'SELECT pv.*, al.event_time 
         FROM patient_vitals pv 
         LEFT JOIN audit_logs al ON pv.patient_id = al.patient_id 
         WHERE pv.measurement_time >= NOW() - INTERVAL ''1 day''
         LIMIT 500',
        'join_query',
        'no_security',
        FALSE,
        'simple'
    );
    
    -- 3. SIMPLE RLS POLICY OVERHEAD
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 2: SIMPLE RLS POLICY OVERHEAD';
    RAISE NOTICE '-------------------------------------';
    
    -- Enable RLS and create simple policy
    ALTER TABLE patient_vitals ENABLE ROW LEVEL SECURITY;
    
    DROP POLICY IF EXISTS simple_test_policy ON patient_vitals;
    CREATE POLICY simple_test_policy ON patient_vitals
        FOR ALL TO PUBLIC
        USING (patient_department = current_setting('app.test_dept', true));
    
    -- Test with different departments
    DECLARE
        departments VARCHAR[] := ARRAY['ICU', 'WARD', 'OUTPATIENT'];
        current_dept VARCHAR;
    BEGIN
        FOREACH current_dept IN ARRAY departments LOOP
            RAISE NOTICE 'Department: %', current_dept;
            PERFORM set_config('app.test_dept', current_dept, false);
            
            -- Test 2.1: Simple SELECT with RLS
            PERFORM measure_query_performance(
                'SELECT * FROM patient_vitals LIMIT 1000',
                'simple_select',
                'simple_rls',
                TRUE,
                'simple'
            );
            
            -- Test 2.2: Range query with RLS
            PERFORM measure_query_performance(
                'SELECT * FROM patient_vitals 
                 WHERE measurement_time >= NOW() - INTERVAL ''1 hour''
                 AND vital_type = ''heart_rate_bpm''',
                'range_query',
                'simple_rls',
                TRUE,
                'simple'
            );
        END LOOP;
    END;
    
    -- 4. COMPLEX RLS POLICY OVERHEAD
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 3: COMPLEX RLS POLICY OVERHEAD';
    RAISE NOTICE '--------------------------------------';
    
    -- Create complex policy with multiple conditions
    DROP POLICY IF EXISTS complex_test_policy ON patient_vitals;
    CREATE POLICY complex_test_policy ON patient_vitals
        FOR ALL TO PUBLIC
        USING (
            patient_department = current_setting('app.test_dept', true)
            AND (
                current_setting('app.user_role', true) = 'doctor_role'
                OR (
                    current_setting('app.user_role', true) = 'nurse_role'
                    AND vital_type NOT IN ('psychiatric_notes', 'sensitive_data')
                    AND EXTRACT(HOUR FROM measurement_time) BETWEEN 6 AND 22
                )
                OR (
                    patient_id = current_setting('app.patient_id', true)
                    AND data_classification IN ('PHI', 'PUBLIC')
                )
            )
        );
    
    -- Test different user roles with complex policy
    DECLARE
        user_roles VARCHAR[] := ARRAY['doctor_role', 'nurse_role', 'patient_role'];
        current_role VARCHAR;
    BEGIN
        FOREACH current_role IN ARRAY user_roles LOOP
            RAISE NOTICE 'User role: %', current_role;
            
            PERFORM set_config('app.test_dept', 'ICU', false);
            PERFORM set_config('app.user_role', current_role, false);
            
            IF current_role = 'patient_role' THEN
                PERFORM set_config('app.patient_id', 'PATIENT_00001', false);
            END IF;
            
            -- Test 3.1: Complex policy SELECT
            PERFORM measure_query_performance(
                'SELECT * FROM patient_vitals 
                 WHERE measurement_time >= NOW() - INTERVAL ''4 hours''',
                'complex_select',
                'complex_rls',
                TRUE,
                'complex'
            );
            
            -- Test 3.2: Aggregation with complex policy
            IF current_role != 'patient_role' THEN
                PERFORM measure_query_performance(
                    'SELECT vital_type, COUNT(*), AVG(vital_value)
                     FROM patient_vitals
                     WHERE measurement_time >= NOW() - INTERVAL ''1 day''
                     GROUP BY vital_type',
                    'complex_aggregation',
                    'complex_rls',
                    TRUE,
                    'complex'
                );
            END IF;
        END LOOP;
    END;
    
    -- 5. MULTIPLE POLICY OVERHEAD
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 4: MULTIPLE POLICY OVERHEAD';
    RAISE NOTICE '-----------------------------------';
    
    -- Add additional policies to test cumulative overhead
    CREATE POLICY additional_policy_1 ON patient_vitals
        FOR SELECT TO PUBLIC
        USING (data_classification != 'RESTRICTED');
    
    CREATE POLICY additional_policy_2 ON patient_vitals
        FOR SELECT TO PUBLIC
        USING (vital_value BETWEEN 0 AND 1000);
    
    CREATE POLICY additional_policy_3 ON patient_vitals
        FOR SELECT TO PUBLIC
        USING (device_id IS NOT NULL);
    
    -- Test with multiple active policies
    PERFORM set_config('app.test_dept', 'WARD', false);
    PERFORM set_config('app.user_role', 'doctor_role', false);
    
    -- Test 4.1: Query with multiple policies
    PERFORM measure_query_performance(
        'SELECT * FROM patient_vitals 
         WHERE measurement_time >= NOW() - INTERVAL ''2 hours''
         ORDER BY measurement_time DESC
         LIMIT 200',
        'multi_policy_select',
        'multiple_policies',
        TRUE,
        'multiple'
    );
    
    -- Test 4.2: Complex query with multiple policies
    PERFORM measure_query_performance(
        'SELECT 
            patient_department,
            vital_type,
            COUNT(*) as readings,
            AVG(vital_value) as avg_value,
            SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alerts
         FROM patient_vitals
         WHERE measurement_time >= NOW() - INTERVAL ''12 hours''
         GROUP BY patient_department, vital_type
         HAVING COUNT(*) > 10',
        'multi_policy_aggregation',
        'multiple_policies',
        TRUE,
        'multiple'
    );
    
    -- 6. POLICY COMBINATION ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 5: POLICY COMBINATION ANALYSIS';
    RAISE NOTICE '--------------------------------------';
    
    -- Test different policy combinations
    DECLARE
        policy_combinations VARCHAR[][] := ARRAY[
            ARRAY['simple'],
            ARRAY['simple', 'data_class'],
            ARRAY['simple', 'value_range'],
            ARRAY['simple', 'data_class', 'value_range'],
            ARRAY['complex', 'data_class'],
            ARRAY['complex', 'value_range', 'device_check']
        ];
        current_combo VARCHAR[];
        combo_name VARCHAR;
        policies_to_enable VARCHAR[];
    BEGIN
        FOREACH current_combo SLICE 1 IN ARRAY policy_combinations LOOP
            combo_name := array_to_string(current_combo, '_');
            RAISE NOTICE 'Policy combination: %', combo_name;
            
            -- Enable/disable policies based on combination
            PERFORM manage_test_policies(current_combo);
            
            -- Test the combination
            PERFORM measure_query_performance(
                'SELECT * FROM patient_vitals 
                 WHERE measurement_time >= NOW() - INTERVAL ''1 hour''
                 AND patient_department = ''ICU''',
                'combo_select',
                combo_name,
                TRUE,
                combo_name
            );
        END LOOP;
    END;
    
    -- 7. CACHE AND INDEX IMPACT ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 6: CACHE AND INDEX IMPACT';
    RAISE NOTICE '---------------------------------';
    
    -- Clear cache for cold start tests
    RAISE NOTICE 'Clearing cache for cold start tests...';
    PERFORM pg_stat_reset();
    
    -- Cold start tests (empty cache)
    PERFORM set_config('app.test_dept', 'ICU', false);
    PERFORM set_config('app.user_role', 'doctor_role', false);
    
    RAISE NOTICE 'Cold start test (empty cache)';
    PERFORM measure_query_performance(
        'SELECT * FROM patient_vitals 
         WHERE measurement_time >= NOW() - INTERVAL ''30 minutes''',
        'cold_start',
        'empty_cache',
        TRUE,
        'simple',
        TRUE  -- cold_start flag
    );
    
    -- Warm cache tests
    RAISE NOTICE 'Warm cache test';
    PERFORM measure_query_performance(
        'SELECT * FROM patient_vitals 
         WHERE measurement_time >= NOW() - INTERVAL ''30 minutes''',
        'warm_cache',
        'warm_cache',
        TRUE,
        'simple',
        FALSE  -- warm cache
    );
    
    -- Index impact analysis
    RAISE NOTICE 'Testing index impact...';
    
    -- Test with different index scenarios
    PERFORM test_index_impact('idx_vitals_measurement_time');
    PERFORM test_index_impact('idx_vitals_patient_time');
    PERFORM test_index_impact('idx_vitals_vital_type_time');
    
    -- 8. CONCURRENT ACCESS OVERHEAD
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 7: CONCURRENT ACCESS OVERHEAD';
    RAISE NOTICE '-------------------------------------';
    
    -- Test concurrent access with RLS
    RAISE NOTICE 'Testing concurrent access patterns...';
    
    -- Test 7.1: Single user, multiple queries
    PERFORM test_concurrent_access(1, 10, 'single_user');
    
    -- Test 7.2: Multiple users, concurrent queries
    PERFORM test_concurrent_access(5, 20, 'multi_user');
    
    -- Test 7.3: Mixed workload
    PERFORM test_mixed_workload(3, 15, 'mixed_workload');
    
    -- 9. DATA VOLUME IMPACT ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 8: DATA VOLUME IMPACT';
    RAISE NOTICE '------------------------------';
    
    -- Test with different data volumes
    DECLARE
        data_sizes VARCHAR[] := ARRAY['small', 'medium', 'large'];
        current_size VARCHAR;
        sample_size INTEGER;
    BEGIN
        FOREACH current_size IN ARRAY data_sizes LOOP
            CASE current_size
                WHEN 'small' THEN sample_size := 1000;
                WHEN 'medium' THEN sample_size := 10000;
                WHEN 'large' THEN sample_size := 50000;
            END CASE;
            
            RAISE NOTICE 'Data volume: % (% rows)', current_size, sample_size;
            
            -- Create test table with sample data
            PERFORM create_test_data_sample(sample_size);
            
            -- Test with current data volume
            PERFORM measure_query_performance(
                'SELECT * FROM test_data_sample 
                 WHERE measurement_time >= NOW() - INTERVAL ''1 day''',
                'volume_test',
                current_size || '_data',
                TRUE,
                'simple'
            );
            
            -- Cleanup
            DROP TABLE IF EXISTS test_data_sample;
        END LOOP;
    END;
    
    -- 10. COMPREHENSIVE ANALYSIS
    -- ============================================
    analysis_end := clock_timestamp();
    
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'COMPREHENSIVE ANALYSIS RESULTS';
    RAISE NOTICE '============================================';
    
    -- Calculate overall statistics
    DECLARE
        total_tests_completed INTEGER;
        total_execution_time NUMERIC;
        avg_test_time NUMERIC;
    BEGIN
        SELECT 
            COUNT(*),
            SUM(total_time_ms),
            AVG(avg_time_ms)
        INTO total_tests_completed, total_execution_time, avg_test_time
        FROM security_overhead_results;
        
        RAISE NOTICE 'Total tests completed: %', total_tests_completed;
        RAISE NOTICE 'Total analysis time: % seconds', 
            ROUND(EXTRACT(EPOCH FROM (analysis_end - analysis_start)), 2);
        RAISE NOTICE 'Average test execution time: % ms', ROUND(avg_test_time, 3);
    END;
    
    -- Generate summary report
    PERFORM generate_security_overhead_report();
    
    -- 11. RECOMMENDATIONS AND OPTIMIZATIONS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'SECURITY OPTIMIZATION RECOMMENDATIONS';
    RAISE NOTICE '============================================';
    
    -- Generate recommendations based on analysis
    PERFORM generate_security_recommendations();
    
    -- 12. CLEANUP
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'Cleaning up test policies...';
    
    -- Drop test policies
    DROP POLICY IF EXISTS simple_test_policy ON patient_vitals;
    DROP POLICY IF EXISTS complex_test_policy ON patient_vitals;
    DROP POLICY IF EXISTS additional_policy_1 ON patient_vitals;
    DROP POLICY IF EXISTS additional_policy_2 ON patient_vitals;
    DROP POLICY IF EXISTS additional_policy_3 ON patient_vitals;
    
    -- Reset configuration
    RESET app.test_dept;
    RESET app.user_role;
    RESET app.patient_id;
    
    -- 13. FINAL REPORT
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'SECURITY OVERHEAD ANALYSIS COMPLETE';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Key metrics saved in: security_overhead_results';
    RAISE NOTICE '';
    RAISE NOTICE 'Analysis covered:';
    RAISE NOTICE '• Baseline performance without security';
    RAISE NOTICE '• Simple vs complex RLS policy overhead';
    RAISE NOTICE '• Multiple policy cumulative impact';
    RAISE NOTICE '• Cache and index effects on security';
    RAISE NOTICE '• Concurrent access patterns';
    RAISE NOTICE '• Data volume impact';
    RAISE NOTICE '';
    RAISE NOTICE 'Use this analysis to:';
    RAISE NOTICE '• Optimize RLS policies for performance';
    RAISE NOTICE '• Design appropriate security architecture';
    RAISE NOTICE '• Size database resources correctly';
    RAISE NOTICE '• Plan for security performance requirements';
    RAISE NOTICE '============================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during analysis: %', SQLERRM;
        RAISE NOTICE 'Rolling back changes...';
        
        -- Cleanup on error
        DROP POLICY IF EXISTS simple_test_policy ON patient_vitals;
        DROP POLICY IF EXISTS complex_test_policy ON patient_vitals;
        DROP POLICY IF EXISTS additional_policy_1 ON patient_vitals;
        DROP POLICY IF EXISTS additional_policy_2 ON patient_vitals;
        DROP POLICY IF EXISTS additional_policy_3 ON patient_vitals;
        
        RESET ALL;
        
        RAISE;
END $$;

-- ============================================
-- SUPPORTING FUNCTIONS
-- ============================================

-- Function to measure query performance
CREATE OR REPLACE FUNCTION measure_query_performance(
    query_text TEXT,
    test_category VARCHAR,
    security_level VARCHAR,
    rls_enabled BOOLEAN,
    policy_complexity VARCHAR,
    cold_start BOOLEAN DEFAULT FALSE
)
RETURNS VOID AS $$
DECLARE
    exec_start TIMESTAMPTZ;
    exec_end TIMESTAMPTZ;
    total_time NUMERIC;
    exec_times NUMERIC[] := '{}';
    i INTEGER;
    explain_result JSONB;
    plan_data JSONB;
    rows_processed INTEGER;
    cache_hits BIGINT;
    cache_reads BIGINT;
    hit_ratio NUMERIC;
    index_used BOOLEAN;
    index_name VARCHAR;
    avg_time NUMERIC;
    min_time NUMERIC;
    max_time NUMERIC;
    stddev_time NUMERIC;
    overhead_ms NUMERIC := 0;
    overhead_pct NUMERIC := 0;
BEGIN
    -- Run query multiple times for statistical significance
    FOR i IN 1..5 LOOP
        exec_start := clock_timestamp();
        EXECUTE query_text;
        exec_end := clock_timestamp();
        
        exec_times := array_append(
            exec_times, 
            EXTRACT(EPOCH FROM (exec_end - exec_start)) * 1000
        );
        
        -- Small delay between runs
        PERFORM pg_sleep(0.1);
    END LOOP;
    
    -- Calculate statistics
    SELECT 
        AVG(val),
        MIN(val),
        MAX(val),
        STDDEV(val)
    INTO avg_time, min_time, max_time, stddev_time
    FROM unnest(exec_times) val;
    
    -- Get execution plan for detailed analysis
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || query_text 
    INTO explain_result;
    
    plan_data := explain_result->0->'Plan';
    rows_processed := COALESCE((plan_data->>'Actual Rows')::INTEGER, 0);
    
    -- Extract buffer cache information
    IF plan_data ? 'Buffers' THEN
        cache_hits := COALESCE((plan_data->'Buffers'->>'Shared Hit Blocks')::BIGINT, 0);
        cache_reads := COALESCE((plan_data->'Buffers'->>'Shared Read Blocks')::BIGINT, 0);
        
        IF cache_hits + cache_reads > 0 THEN
            hit_ratio := (cache_hits::NUMERIC / (cache_hits + cache_reads)) * 100;
        ELSE
            hit_ratio := 100;
        END IF;
    ELSE
        hit_ratio := NULL;
    END IF;
    
    -- Check if index was used
    index_used := plan_data->>'Node Type' IN ('Index Scan', 'Index Only Scan');
    IF index_used THEN
        index_name := plan_data->>'Index Name';
    ELSE
        index_name := 'Seq Scan';
    END IF;
    
    -- Calculate overhead if baseline exists
    IF rls_enabled THEN
        SELECT 
            baseline.avg_time_ms - avg_time
        INTO overhead_ms
        FROM (
            SELECT avg_time_ms
            FROM security_overhead_results
            WHERE test_category = measure_query_performance.test_category
            AND security_level = 'no_security'
            ORDER BY analysis_timestamp DESC
            LIMIT 1
        ) baseline;
        
        IF overhead_ms IS NOT NULL AND overhead_ms != 0 THEN
            overhead_pct := (overhead_ms / avg_time) * 100;
        END IF;
    END IF;
    
    -- Insert result
    INSERT INTO security_overhead_results (
        test_category,
        test_scenario,
        security_level,
        rls_enabled,
        policy_complexity,
        execution_count,
        total_time_ms,
        avg_time_ms,
        min_time_ms,
        max_time_ms,
        stddev_time_ms,
        overhead_ms,
        overhead_percentage,
        rows_processed,
        cache_hit_ratio,
        index_utilization
    ) VALUES (
        test_category,
        query_text,
        security_level,
        rls_enabled,
        policy_complexity,
        5, -- execution_count
        array_sum(exec_times),
        COALESCE(avg_time, 0),
        COALESCE(min_time, 0),
        COALESCE(max_time, 0),
        COALESCE(stddev_time, 0),
        COALESCE(overhead_ms, 0),
        COALESCE(overhead_pct, 0),
        rows_processed,
        COALESCE(hit_ratio, 0),
        index_name
    );
    
    -- Output result
    RAISE NOTICE '  Avg: % ms, Range: %-% ms, Rows: %, Cache: %, Index: %',
        ROUND(avg_time, 3),
        ROUND(min_time, 3),
        ROUND(max_time, 3),
        rows_processed,
        CASE 
            WHEN hit_ratio IS NULL THEN 'N/A' 
            ELSE ROUND(hit_ratio, 1) || '%' 
        END,
        index_name;
        
END;
$$ LANGUAGE plpgsql;

-- Helper function to sum array
CREATE OR REPLACE FUNCTION array_sum(arr NUMERIC[])
RETURNS NUMERIC AS $$
DECLARE
    total NUMERIC := 0;
    val NUMERIC;
BEGIN
    FOREACH val IN ARRAY arr LOOP
        total := total + val;
    END LOOP;
    RETURN total;
END;
$$ LANGUAGE plpgsql;

-- Function to manage test policies
CREATE OR REPLACE FUNCTION manage_test_policies(policy_list VARCHAR[])
RETURNS VOID AS $$
BEGIN
    -- Disable all test policies first
    DROP POLICY IF EXISTS simple_test_policy ON patient_vitals;
    DROP POLICY IF EXISTS complex_test_policy ON patient_vitals;
    DROP POLICY IF EXISTS data_class_policy ON patient_vitals;
    DROP POLICY IF EXISTS value_range_policy ON patient_vitals;
    DROP POLICY IF EXISTS device_check_policy ON patient_vitals;
    
    -- Create requested policies
    IF 'simple' = ANY(policy_list) THEN
        CREATE POLICY simple_test_policy ON patient_vitals
            FOR ALL TO PUBLIC
            USING (patient_department = current_setting('app.test_dept', true));
    END IF;
    
    IF 'complex' = ANY(policy_list) THEN
        CREATE POLICY complex_test_policy ON patient_vitals
            FOR ALL TO PUBLIC
            USING (
                patient_department = current_setting('app.test_dept', true)
                AND current_setting('app.user_role', true) = 'doctor_role'
            );
    END IF;
    
    IF 'data_class' = ANY(policy_list) THEN
        CREATE POLICY data_class_policy ON patient_vitals
            FOR SELECT TO PUBLIC
            USING (data_classification != 'RESTRICTED');
    END IF;
    
    IF 'value_range' = ANY(policy_list) THEN
        CREATE POLICY value_range_policy ON patient_vitals
            FOR SELECT TO PUBLIC
            USING (vital_value BETWEEN 0 AND 1000);
    END IF;
    
    IF 'device_check' = ANY(policy_list) THEN
        CREATE POLICY device_check_policy ON patient_vitals
            FOR SELECT TO PUBLIC
            USING (device_id IS NOT NULL);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to test index impact
CREATE OR REPLACE FUNCTION test_index_impact(index_name VARCHAR)
RETURNS VOID AS $$
DECLARE
    index_exists BOOLEAN;
    test_query TEXT;
BEGIN
    -- Check if index exists
    SELECT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE tablename = 'patient_vitals' 
        AND indexname = test_index_impact.index_name
    ) INTO index_exists;
    
    IF NOT index_exists THEN
        RAISE NOTICE '  Index % not found', index_name;
        RETURN;
    END IF;
    
    RAISE NOTICE '  Testing index: %', index_name;
    
    -- Create query that should use this index
    test_query := format(
        'SELECT * FROM patient_vitals 
         WHERE measurement_time >= NOW() - INTERVAL ''1 hour''
         ORDER BY measurement_time DESC
         LIMIT 100'
    );
    
    -- Test with index hint
    EXECUTE format('SET enable_seqscan = off');
    
    PERFORM measure_query_performance(
        test_query,
        'index_test',
        index_name || '_enabled',
        TRUE,
        'simple'
    );
    
    -- Test without index (force seqscan)
    EXECUTE format('SET enable_seqscan = on');
    EXECUTE format('SET enable_indexscan = off');
    
    PERFORM measure_query_performance(
        test_query,
        'index_test',
        index_name || '_disabled',
        TRUE,
        'simple'
    );
    
    -- Reset settings
    EXECUTE format('RESET enable_seqscan');
    EXECUTE format('RESET enable_indexscan');
    
END;
$$ LANGUAGE plpgsql;

-- Function to test concurrent access
CREATE OR REPLACE FUNCTION test_concurrent_access(
    user_count INTEGER,
    queries_per_user INTEGER,
    test_name VARCHAR
)
RETURNS VOID AS $$
DECLARE
    user_id INTEGER;
    query_id INTEGER;
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    total_time NUMERIC;
    successful_queries INTEGER := 0;
    failed_queries INTEGER := 0;
BEGIN
    RAISE NOTICE '  Concurrent test: % users, % queries/user', 
        user_count, queries_per_user;
    
    start_time := clock_timestamp();
    
    -- Simulate concurrent users (in reality, would use multiple connections)
    FOR user_id IN 1..user_count LOOP
        -- Set user context
        PERFORM set_config('app.test_dept', 'ICU', false);
        PERFORM set_config('app.user_role', 
            CASE WHEN user_id % 3 = 0 THEN 'nurse_role' ELSE 'doctor_role' END, 
            false);
        
        -- Execute queries
        FOR query_id IN 1..queries_per_user LOOP
            BEGIN
                -- Vary queries slightly
                EXECUTE format(
                    'SELECT COUNT(*) FROM patient_vitals 
                     WHERE measurement_time >= NOW() - INTERVAL ''%s minutes''
                     AND patient_department = ''ICU''',
                    (query_id % 60) + 1
                );
                
                successful_queries := successful_queries + 1;
            EXCEPTION WHEN OTHERS THEN
                failed_queries := failed_queries + 1;
            END;
            
            -- Small delay between queries
            PERFORM pg_sleep(0.01);
        END LOOP;
    END LOOP;
    
    end_time := clock_timestamp();
    total_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    -- Store results
    INSERT INTO security_overhead_results (
        test_category,
        test_scenario,
        security_level,
        rls_enabled,
        policy_complexity,
        execution_count,
        total_time_ms,
        avg_time_ms,
        rows_processed,
        recommendations
    ) VALUES (
        'concurrent_access',
        test_name,
        'concurrent_' || user_count || '_users',
        TRUE,
        'simple',
        successful_queries + failed_queries,
        total_time,
        total_time / (successful_queries + failed_queries),
        successful_queries,
        format('Success rate: %s/%s (%s%%)', 
               successful_queries, 
               successful_queries + failed_queries,
               ROUND(successful_queries::NUMERIC / (successful_queries + failed_queries) * 100, 1))
    );
    
    RAISE NOTICE '    Total time: % ms, Queries: % (failed: %), Rate: % qps',
        ROUND(total_time, 2),
        successful_queries + failed_queries,
        failed_queries,
        ROUND((successful_queries + failed_queries) / (total_time / 1000), 2);
    
END;
$$ LANGUAGE plpgsql;

-- Function to test mixed workload
CREATE OR REPLACE FUNCTION test_mixed_workload(
    user_count INTEGER,
    duration_seconds INTEGER,
    test_name VARCHAR
)
RETURNS VOID AS $$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    queries_executed INTEGER := 0;
    query_types TEXT[] := ARRAY[
        'SELECT COUNT(*) FROM patient_vitals WHERE patient_department = ''ICU''',
        'SELECT AVG(vital_value) FROM patient_vitals WHERE vital_type = ''heart_rate_bpm''',
        'SELECT * FROM patient_vitals WHERE is_alert = TRUE ORDER BY measurement_time DESC LIMIT 10''',
                'INSERT INTO audit_logs (patient_id, user_role, event_type) VALUES (''PATIENT_001'', ''doctor_role'', ''data_access'')',
        'SELECT COUNT(*) FROM patient_vitals WHERE measurement_time > NOW() - INTERVAL ''5 minutes''',
        'SELECT patient_id, COUNT(*) as alert_count FROM patient_vitals WHERE is_alert = TRUE GROUP BY patient_id'
    ];
    query_start TIMESTAMPTZ;
    query_end TIMESTAMPTZ;
    total_time NUMERIC := 0;
    successful_queries INTEGER := 0;
BEGIN
    RAISE NOTICE '  Mixed workload test: % users, % seconds', 
        user_count, duration_seconds;
    
    -- Set base configuration
    PERFORM set_config('app.test_dept', 'ICU', false);
    PERFORM set_config('app.user_role', 'doctor_role', false);
    
    start_time := clock_timestamp();
    end_time := start_time + (duration_seconds || ' seconds')::INTERVAL;
    
    WHILE clock_timestamp() < end_time LOOP
        -- Simulate different users
        FOR user_id IN 1..user_count LOOP
            IF clock_timestamp() >= end_time THEN
                EXIT;
            END IF;
            
            -- Randomly select query type
            query_start := clock_timestamp();
            
            BEGIN
                -- Execute random query
                EXECUTE query_types[1 + floor(random() * array_length(query_types, 1))];
                successful_queries := successful_queries + 1;
            EXCEPTION WHEN OTHERS THEN
                -- Log but continue
                NULL;
            END;
            
            query_end := clock_timestamp();
            total_time := total_time + EXTRACT(EPOCH FROM (query_end - query_start)) * 1000;
            queries_executed := queries_executed + 1;
            
            -- Small random delay between queries
            PERFORM pg_sleep(random() * 0.1);
        END LOOP;
    END WHILE;
    
    -- Store results
    INSERT INTO security_overhead_results (
        test_category,
        test_scenario,
        security_level,
        rls_enabled,
        policy_complexity,
        execution_count,
        total_time_ms,
        avg_time_ms,
        rows_processed,
        recommendations
    ) VALUES (
        'mixed_workload',
        test_name,
        'mixed_' || user_count || '_users',
        TRUE,
        'mixed',
        queries_executed,
        total_time,
        CASE WHEN queries_executed > 0 THEN total_time / queries_executed ELSE 0 END,
        successful_queries,
        format('Throughput: %s qps', 
               ROUND(queries_executed::NUMERIC / duration_seconds, 2))
    );
    
    RAISE NOTICE '    Duration: %s s, Queries: %, Avg time: % ms, Throughput: % qps',
        duration_seconds,
        queries_executed,
        CASE WHEN queries_executed > 0 THEN ROUND(total_time / queries_executed, 3) ELSE 0 END,
        ROUND(queries_executed::NUMERIC / duration_seconds, 2);
    
END;
$$ LANGUAGE plpgsql;

-- Function to create test data sample
CREATE OR REPLACE FUNCTION create_test_data_sample(sample_size INTEGER)
RETURNS VOID AS $$
BEGIN
    DROP TABLE IF EXISTS test_data_sample;
    
    -- Create sample table with same structure
    CREATE TABLE test_data_sample AS
    SELECT *
    FROM patient_vitals 
    LIMIT sample_size;
    
    -- Enable RLS on sample table
    ALTER TABLE test_data_sample ENABLE ROW LEVEL SECURITY;
    
    -- Apply same policies
    CREATE POLICY sample_policy ON test_data_sample
        FOR ALL TO PUBLIC
        USING (patient_department = current_setting('app.test_dept', true));
    
    -- Create indexes
    CREATE INDEX idx_sample_measurement_time ON test_data_sample(measurement_time);
    CREATE INDEX idx_sample_patient_time ON test_data_sample(patient_id, measurement_time);
    CREATE INDEX idx_sample_vital_type_time ON test_data_sample(vital_type, measurement_time);
    
    -- Analyze for better query planning
    ANALYZE test_data_sample;
    
END;
$$ LANGUAGE plpgsql;

-- Function to generate security overhead report
CREATE OR REPLACE FUNCTION generate_security_overhead_report()
RETURNS VOID AS $$
DECLARE
    report_data JSONB;
    report_summary TEXT;
BEGIN
    -- Generate comprehensive report
    SELECT jsonb_build_object(
        'analysis_summary', (
            SELECT jsonb_build_object(
                'total_tests', COUNT(*),
                'analysis_period', MIN(analysis_timestamp) || ' to ' || MAX(analysis_timestamp),
                'avg_execution_time', ROUND(AVG(avg_time_ms), 3),
                'max_overhead', ROUND(MAX(overhead_percentage), 2),
                'avg_overhead', ROUND(AVG(overhead_percentage), 2)
            )
            FROM security_overhead_results
        ),
        'security_level_comparison', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'security_level', security_level,
                    'avg_time_ms', ROUND(AVG(avg_time_ms), 3),
                    'test_count', COUNT(*),
                    'min_time', ROUND(MIN(avg_time_ms), 3),
                    'max_time', ROUND(MAX(avg_time_ms), 3)
                )
                ORDER BY AVG(avg_time_ms)
            )
            FROM security_overhead_results
            WHERE security_level IS NOT NULL
            GROUP BY security_level
        ),
        'policy_complexity_impact', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'policy_complexity', policy_complexity,
                    'avg_time_ms', ROUND(AVG(avg_time_ms), 3),
                    'overhead_percentage', ROUND(AVG(overhead_percentage), 2),
                    'query_count', COUNT(*)
                )
                ORDER BY AVG(avg_time_ms) DESC
            )
            FROM security_overhead_results
            WHERE policy_complexity IS NOT NULL AND rls_enabled = true
            GROUP BY policy_complexity
        ),
        'index_utilization_analysis', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'index_type', index_utilization,
                    'avg_time_ms', ROUND(AVG(avg_time_ms), 3),
                    'cache_hit_ratio', ROUND(AVG(cache_hit_ratio), 1),
                    'query_count', COUNT(*)
                )
                WHERE index_utilization IS NOT NULL
                ORDER BY AVG(avg_time_ms)
            )
            FROM security_overhead_results
            GROUP BY index_utilization
        )
    ) INTO report_data;
    
    -- Create readable report
    report_summary := '
============================================
SECURITY OVERHEAD ANALYSIS REPORT
============================================

KEY FINDINGS:
' || (
    SELECT string_agg(
        '• ' || 
        CASE 
            WHEN overhead_percentage > 50 THEN 'HIGH OVERHEAD: ' || security_level || ' (' || ROUND(overhead_percentage, 1) || '%)'
            WHEN overhead_percentage > 20 THEN 'MODERATE OVERHEAD: ' || security_level || ' (' || ROUND(overhead_percentage, 1) || '%)'
            WHEN overhead_percentage > 0 THEN 'LOW OVERHEAD: ' || security_level || ' (' || ROUND(overhead_percentage, 1) || '%)'
            ELSE 'NEGLIGIBLE OVERHEAD: ' || security_level
        END, E'\n'
    )
    FROM (
        SELECT security_level, AVG(overhead_percentage) as overhead_percentage
        FROM security_overhead_results
        WHERE rls_enabled = true AND security_level != 'no_security'
        GROUP BY security_level
        ORDER BY AVG(overhead_percentage) DESC
    ) t
) || '

PERFORMANCE RECOMMENDATIONS:
' || (
    SELECT string_agg(recommendation, E'\n')
    FROM (
        SELECT DISTINCT ON (category)
            CASE 
                WHEN category = 'index' THEN '• Ensure proper indexing on RLS predicate columns'
                WHEN category = 'cache' THEN '• Increase shared_buffers for better RLS cache performance'
                WHEN category = 'policy' THEN '• Simplify complex RLS policies with WHERE clause optimization'
                WHEN category = 'concurrent' THEN '• Monitor concurrent user impact on RLS performance'
                ELSE '• Review ' || category || ' configuration'
            END as recommendation,
            category
        FROM (
            SELECT 'index' as category, 1 as priority
            UNION SELECT 'cache', 2
            UNION SELECT 'policy', 3
            UNION SELECT 'concurrent', 4
        ) categories
        ORDER BY priority
    ) recs
) || '

DETAILED METRICS:
• Average query time without RLS: ' || (
    SELECT ROUND(AVG(avg_time_ms), 3)::TEXT
    FROM security_overhead_results 
    WHERE security_level = 'no_security'
) || ' ms
• Average query time with RLS: ' || (
    SELECT ROUND(AVG(avg_time_ms), 3)::TEXT
    FROM security_overhead_results 
    WHERE rls_enabled = true AND security_level != 'no_security'
) || ' ms
• Maximum observed overhead: ' || (
    SELECT ROUND(MAX(overhead_percentage), 2)::TEXT
    FROM security_overhead_results
) || '%
• Best performing security level: ' || (
    SELECT security_level
    FROM (
        SELECT security_level, AVG(avg_time_ms) as avg_time
        FROM security_overhead_results
        WHERE security_level != 'no_security'
        GROUP BY security_level
        ORDER BY avg_time
        LIMIT 1
    ) t
) || '
• Worst performing security level: ' || (
    SELECT security_level
    FROM (
        SELECT security_level, AVG(avg_time_ms) as avg_time
        FROM security_overhead_results
        WHERE security_level != 'no_security'
        GROUP BY security_level
        ORDER BY avg_time DESC
        LIMIT 1
    ) t
) || '

NEXT STEPS:
1. Review high-overhead scenarios in detailed results
2. Consider policy simplification for frequently accessed data
3. Evaluate indexing strategy for RLS predicates
4. Monitor performance in production with similar loads
5. Consider connection pooling for concurrent access scenarios

============================================
';
    
    -- Output report
    RAISE NOTICE '%', report_summary;
    
    -- Save report to table
    DROP TABLE IF EXISTS security_analysis_report;
    CREATE TABLE security_analysis_report AS
    SELECT 
        analysis_timestamp,
        test_category,
        security_level,
        policy_complexity,
        avg_time_ms,
        overhead_percentage,
        rows_processed,
        cache_hit_ratio,
        index_utilization,
        recommendations
    FROM security_overhead_results
    ORDER BY analysis_timestamp, test_category;
    
    -- Create summary view
    DROP VIEW IF EXISTS security_overhead_summary;
    CREATE VIEW security_overhead_summary AS
    SELECT 
        security_level,
        policy_complexity,
        COUNT(*) as test_count,
        ROUND(AVG(avg_time_ms), 3) as avg_execution_time_ms,
        ROUND(MIN(avg_time_ms), 3) as min_execution_time_ms,
        ROUND(MAX(avg_time_ms), 3) as max_execution_time_ms,
        ROUND(STDDEV(avg_time_ms), 3) as stddev_execution_time_ms,
        ROUND(AVG(overhead_percentage), 2) as avg_overhead_percentage,
        ROUND(AVG(cache_hit_ratio), 1) as avg_cache_hit_ratio,
        STRING_AGG(DISTINCT index_utilization, ', ') as indexes_used
    FROM security_overhead_results
    GROUP BY security_level, policy_complexity
    ORDER BY AVG(avg_time_ms) DESC;
    
    RAISE NOTICE 'Detailed report saved in: security_analysis_report';
    RAISE NOTICE 'Summary view created: security_overhead_summary';
    
END;
$$ LANGUAGE plpgsql;

-- Function to generate security recommendations
CREATE OR REPLACE FUNCTION generate_security_recommendations()
RETURNS VOID AS $$
DECLARE
    max_overhead NUMERIC;
    avg_overhead NUMERIC;
    complex_policy_count INTEGER;
    index_utilization_stats JSONB;
    cache_stats JSONB;
BEGIN
    -- Get key metrics
    SELECT 
        MAX(overhead_percentage),
        AVG(overhead_percentage),
        COUNT(*) FILTER (WHERE policy_complexity IN ('complex', 'multiple'))
    INTO max_overhead, avg_overhead, complex_policy_count
    FROM security_overhead_results
    WHERE rls_enabled = true;
    
    -- Get index utilization stats
    SELECT jsonb_build_object(
        'index_scans', COUNT(*) FILTER (WHERE index_utilization LIKE 'idx%'),
        'seq_scans', COUNT(*) FILTER (WHERE index_utilization = 'Seq Scan'),
        'avg_time_index', ROUND(AVG(avg_time_ms) FILTER (WHERE index_utilization LIKE 'idx%'), 3),
        'avg_time_seqscan', ROUND(AVG(avg_time_ms) FILTER (WHERE index_utilization = 'Seq Scan'), 3)
    ) INTO index_utilization_stats
    FROM security_overhead_results;
    
    -- Get cache stats
    SELECT jsonb_build_object(
        'avg_hit_ratio', ROUND(AVG(cache_hit_ratio), 1),
        'low_cache_tests', COUNT(*) FILTER (WHERE cache_hit_ratio < 80),
        'high_cache_tests', COUNT(*) FILTER (WHERE cache_hit_ratio >= 95)
    ) INTO cache_stats
    FROM security_overhead_results
    WHERE cache_hit_ratio IS NOT NULL;
    
    -- Generate recommendations based on analysis
    RAISE NOTICE 'GENERAL RECOMMENDATIONS:';
    RAISE NOTICE '----------------------';
    
    IF max_overhead > 50 THEN
        RAISE NOTICE '⚠️  HIGH OVERHEAD DETECTED: Maximum overhead of %%%', ROUND(max_overhead, 1);
        RAISE NOTICE '   • Consider simplifying complex RLS policies';
        RAISE NOTICE '   • Evaluate using column-level security as alternative';
        RAISE NOTICE '   • Review frequently accessed data paths';
    ELSIF max_overhead > 20 THEN
        RAISE NOTICE '⚠️  MODERATE OVERHEAD DETECTED: Maximum overhead of %%%', ROUND(max_overhead, 1);
        RAISE NOTICE '   • Monitor performance in production';
        RAISE NOTICE '   • Consider policy optimization';
    ELSE
        RAISE NOTICE '✅  OVERHEAD WITHIN ACCEPTABLE RANGE: Maximum overhead of %%%', ROUND(max_overhead, 1);
    END IF;
    
    IF complex_policy_count > 5 THEN
        RAISE NOTICE '';
        RAISE NOTICE '⚠️  COMPLEX POLICY USAGE: % complex/multiple policies detected', complex_policy_count;
        RAISE NOTICE '   • Consider consolidating policies';
        RAISE NOTICE '   • Use WITH CHECK for INSERT/UPDATE policies';
        RAISE NOTICE '   • Evaluate using security barrier views';
    END IF;
    
    IF (index_utilization_stats->>'seq_scans')::INTEGER > 
       (index_utilization_stats->>'index_scans')::INTEGER THEN
        RAISE NOTICE '';
        RAISE NOTICE '⚠️  INDEX UTILIZATION: More sequential scans than index scans';
        RAISE NOTICE '   • Review indexes on RLS predicate columns';
        RAISE NOTICE '   • Consider creating composite indexes';
        RAISE NOTICE '   • Analyze query plans for missing indexes';
    END IF;
    
    IF (cache_stats->>'avg_hit_ratio')::NUMERIC < 90 THEN
        RAISE NOTICE '';
        RAISE NOTICE '⚠️  CACHE PERFORMANCE: Average cache hit ratio of %%%', 
            ROUND((cache_stats->>'avg_hit_ratio')::NUMERIC, 1);
        RAISE NOTICE '   • Consider increasing shared_buffers';
        RAISE NOTICE '   • Evaluate using connection pooling';
        RAISE NOTICE '   • Monitor cache performance in production';
    END IF;
    
    -- Specific optimization recommendations
    RAISE NOTICE '';
    RAISE NOTICE 'OPTIMIZATION STRATEGIES:';
    RAISE NOTICE '-----------------------';
    
    RAISE NOTICE '1. POLICY OPTIMIZATION:';
    RAISE NOTICE '   • Use simple equality checks in RLS predicates';
    RAISE NOTICE '   • Avoid functions in RLS predicates when possible';
    RAISE NOTICE '   • Consider using security definer functions';
    
    RAISE NOTICE '';
    RAISE NOTICE '2. INDEXING STRATEGY:';
    RAISE NOTICE '   • Index columns used in RLS USING clauses';
    RAISE NOTICE '   • Create composite indexes for common query patterns';
    RAISE NOTICE '   • Regularly analyze and maintain indexes';
    
    RAISE NOTICE '';
    RAISE NOTICE '3. CACHE OPTIMIZATION:';
    RAISE NOTICE '   • Size shared_buffers appropriately';
    RAISE NOTICE '   • Use prepared statements for frequent queries';
    RAISE NOTICE '   • Consider using pg_prewarm for critical data';
    
    RAISE NOTICE '';
    RAISE NOTICE '4. ARCHITECTURE CONSIDERATIONS:';
    RAISE NOTICE '   • Evaluate partitioning for large datasets';
    RAISE NOTICE '   • Consider read replicas for high concurrency';
    RAISE NOTICE '   • Use connection pooling for web applications';
    
    -- Save recommendations to table
    DROP TABLE IF EXISTS security_recommendations;
    CREATE TABLE security_recommendations (
        recommendation_id SERIAL PRIMARY KEY,
        category VARCHAR(50),
        priority INTEGER,
        recommendation TEXT,
        rationale TEXT,
        estimated_impact VARCHAR(20)
    );
    
    INSERT INTO security_recommendations 
    (category, priority, recommendation, rationale, estimated_impact) VALUES
    ('Policy', 1, 'Simplify complex RLS policies with multiple conditions', 
     'Complex policies increase query planning time and execution overhead', 'High'),
    ('Indexing', 2, 'Create indexes on columns used in RLS predicate conditions', 
     'Indexes on RLS predicate columns significantly reduce filtering overhead', 'High'),
    ('Cache', 3, 'Increase shared_buffers configuration', 
     'Better cache utilization reduces I/O overhead for RLS-enabled queries', 'Medium'),
    ('Architecture', 4, 'Implement connection pooling for web applications', 
     'Reduces connection overhead and improves concurrent performance', 'Medium'),
    ('Monitoring', 5, 'Set up continuous monitoring of RLS performance', 
     'Early detection of performance degradation in production', 'Low');
    
    RAISE NOTICE '';
    RAISE NOTICE 'Detailed recommendations saved in: security_recommendations';
    
END;
$$ LANGUAGE plpgsql;

-- Create function to export analysis results
CREATE OR REPLACE FUNCTION export_security_analysis()
RETURNS TEXT AS $$
DECLARE
    export_path TEXT;
    csv_data TEXT;
BEGIN
    -- Generate CSV export
    SELECT string_agg(
        format('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
            analysis_timestamp,
            test_category,
            security_level,
            policy_complexity,
            avg_time_ms,
            overhead_percentage,
            rows_processed,
            cache_hit_ratio,
            index_utilization,
            execution_count,
            COALESCE(recommendations, '')
        ), E'\n'
    ) INTO csv_data
    FROM security_overhead_results;
    
    -- Add header
    csv_data := 'timestamp,test_category,security_level,policy_complexity,avg_time_ms,overhead_percentage,rows_processed,cache_hit_ratio,index_utilization,execution_count,recommendations' || E'\n' || csv_data;
    
    -- Save to file (requires appropriate permissions)
    export_path := '/tmp/security_analysis_' || to_char(NOW(), 'YYYYMMDD_HH24MISS') || '.csv';
    
    -- Note: This requires superuser or appropriate file system permissions
    -- In practice, you might want to use COPY command or external tool
    RAISE NOTICE 'Export data prepared. To save to file, use:';
    RAISE NOTICE 'COPY (SELECT * FROM security_overhead_results) TO ''%'' WITH CSV HEADER;', export_path;
    
    RETURN csv_data;
    
EXCEPTION WHEN OTHERS THEN
    RETURN 'Error generating export: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Create monitoring view for ongoing performance tracking
CREATE OR REPLACE VIEW security_performance_monitor AS
SELECT 
    date_trunc('hour', analysis_timestamp) as time_bucket,
    test_category,
    security_level,
    COUNT(*) as query_count,
    ROUND(AVG(avg_time_ms), 3) as avg_response_time,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY avg_time_ms), 3) as p95_response_time,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY avg_time_ms), 3) as p99_response_time,
    ROUND(AVG(overhead_percentage), 2) as avg_overhead,
    ROUND(AVG(cache_hit_ratio), 1) as avg_cache_hit
FROM security_overhead_results
GROUP BY date_trunc('hour', analysis_timestamp), test_category, security_level
ORDER BY time_bucket DESC, avg_response_time DESC;

-- Create alerting thresholds based on analysis
CREATE OR REPLACE FUNCTION check_security_performance_alerts()
RETURNS TABLE (
    alert_level VARCHAR(20),
    alert_type VARCHAR(50),
    alert_description TEXT,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        CASE 
            WHEN overhead_percentage > 100 THEN 'CRITICAL'
            WHEN overhead_percentage > 50 THEN 'HIGH'
            WHEN overhead_percentage > 20 THEN 'MEDIUM'
            ELSE 'LOW'
        END as alert_level,
        'RLS_OVERHEAD' as alert_type,
        format('Security overhead of %s%% detected for %s (policy: %s)', 
               ROUND(overhead_percentage, 1), 
               security_level,
               policy_complexity) as alert_description,
        'Consider policy optimization or architectural review' as recommendation
    FROM security_overhead_results
    WHERE overhead_percentage > 20
    AND analysis_timestamp > NOW() - INTERVAL '1 hour'
    UNION ALL
    SELECT 
        'HIGH' as alert_level,
        'CACHE_PERFORMANCE' as alert_type,
        format('Low cache hit ratio: %s%%', ROUND(cache_hit_ratio, 1)) as alert_description,
        'Review shared_buffers configuration and query patterns' as recommendation
    FROM security_overhead_results
    WHERE cache_hit_ratio < 70
    AND analysis_timestamp > NOW() - INTERVAL '1 hour'
    UNION ALL
    SELECT 
        'MEDIUM' as alert_level,
        'INDEX_UTILIZATION' as alert_type,
        'High number of sequential scans detected' as alert_description,
        'Review indexing strategy for RLS-enabled tables' as recommendation
    FROM (
        SELECT COUNT(*) FILTER (WHERE index_utilization = 'Seq Scan') as seq_scans
        FROM security_overhead_results
        WHERE analysis_timestamp > NOW() - INTERVAL '1 hour'
    ) t
    WHERE seq_scans > 10;
END;
$$ LANGUAGE plpgsql;

-- Final cleanup and reset
RESET ALL;

RAISE NOTICE '============================================';
RAISE NOTICE 'SECURITY OVERHEAD ANALYSIS COMPLETE';
RAISE NOTICE '============================================';
RAISE NOTICE '';
RAISE NOTICE 'Available objects created:';
RAISE NOTICE '• security_overhead_results - Detailed test results';
RAISE NOTICE '• security_analysis_report - Formatted analysis report';
RAISE NOTICE '• security_overhead_summary - Summary view';
RAISE NOTICE '• security_recommendations - Optimization suggestions';
RAISE NOTICE '• security_performance_monitor - Ongoing monitoring view';
RAISE NOTICE '• check_security_performance_alerts() - Alerting function';
RAISE NOTICE '';
RAISE NOTICE 'To review results:';
RAISE NOTICE '  SELECT * FROM security_overhead_summary ORDER BY avg_execution_time_ms DESC;';
RAISE NOTICE '  SELECT * FROM security_recommendations ORDER BY priority;';
RAISE NOTICE '';
RAISE NOTICE 'To export results:';
RAISE NOTICE '  SELECT export_security_analysis();';
RAISE NOTICE '';
RAISE NOTICE 'For ongoing monitoring:';
RAISE NOTICE '  SELECT * FROM security_performance_monitor;';
RAISE NOTICE '  SELECT * FROM check_security_performance_alerts();';
RAISE NOTICE '============================================';