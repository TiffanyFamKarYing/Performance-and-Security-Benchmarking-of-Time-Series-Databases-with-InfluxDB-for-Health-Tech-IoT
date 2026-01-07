-- POSTGRESQL STORAGE EFFICIENCY ANALYSIS

\c health_iot_benchmark;

-- Enable timing for all statements
\timing on

-- 1. STORAGE UTILIZATION ANALYSIS
-- ============================================

DO $$
DECLARE
    analysis_start TIMESTAMPTZ;
    analysis_end TIMESTAMPTZ;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'POSTGRESQL STORAGE EFFICIENCY ANALYSIS';
    RAISE NOTICE '============================================';
    
    analysis_start := clock_timestamp();
    
    -- Create storage analysis results table
    DROP TABLE IF EXISTS storage_efficiency_results;
    CREATE TABLE storage_efficiency_results (
        analysis_id SERIAL PRIMARY KEY,
        analysis_timestamp TIMESTAMPTZ DEFAULT NOW(),
        metric_name VARCHAR(100),
        table_name VARCHAR(100),
        index_name VARCHAR(100),
        storage_size_bytes BIGINT,
        storage_size_mb NUMERIC(12,2),
        storage_size_gb NUMERIC(12,2),
        row_count BIGINT,
        avg_row_size_bytes NUMERIC(10,2),
        fillfactor INTEGER,
        compression_ratio NUMERIC(6,2),
        bloat_percentage NUMERIC(6,2),
        vacuum_efficiency VARCHAR(50),
        recommendations TEXT,
        analysis_details JSONB
    );
    
    -- 2. TABLE SIZE ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 1: TABLE SIZE ANALYSIS';
    RAISE NOTICE '--------------------------------';
    
    -- Get table sizes
    WITH table_sizes AS (
        SELECT 
            schemaname,
            tablename,
            pg_total_relation_size(schemaname || '.' || tablename) as total_size,
            pg_relation_size(schemaname || '.' || tablename) as table_size,
            pg_total_relation_size(schemaname || '.' || tablename) - 
            pg_relation_size(schemaname || '.' || tablename) as index_size,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    )
    INSERT INTO storage_efficiency_results (
        metric_name, table_name, storage_size_bytes, 
        storage_size_mb, row_count, avg_row_size_bytes
    )
    SELECT 
        'table_size',
        tablename,
        total_size,
        ROUND(total_size / (1024.0 * 1024.0), 2),
        row_count,
        CASE 
            WHEN row_count > 0 THEN ROUND(table_size::NUMERIC / row_count, 2)
            ELSE 0 
        END
    FROM table_sizes
    ORDER BY total_size DESC;
    
    -- Display table sizes
    RAISE NOTICE 'Table Sizes (MB):';
    FOR rec IN 
        SELECT 
            table_name,
            ROUND(storage_size_mb, 2) as size_mb,
            row_count,
            ROUND(avg_row_size_bytes, 2) as avg_row_bytes
        FROM storage_efficiency_results 
        WHERE metric_name = 'table_size'
        ORDER BY storage_size_bytes DESC
    LOOP
        RAISE NOTICE '  %: % MB (% rows, % bytes/row)',
            rec.table_name, rec.size_mb, rec.row_count, rec.avg_row_bytes;
    END LOOP;
    
    -- 3. INDEX SIZE ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 2: INDEX SIZE ANALYSIS';
    RAISE NOTICE '------------------------------';
    
    -- Get index sizes
    WITH index_sizes AS (
        SELECT 
            schemaname,
            tablename,
            indexname,
            pg_relation_size(schemaname || '.' || indexname) as index_size,
            pg_total_relation_size(schemaname || '.' || tablename) as table_total_size
        FROM pg_stat_user_indexes
        WHERE schemaname = 'public'
    )
    INSERT INTO storage_efficiency_results (
        metric_name, table_name, index_name, 
        storage_size_bytes, storage_size_mb
    )
    SELECT 
        'index_size',
        tablename,
        indexname,
        index_size,
        ROUND(index_size / (1024.0 * 1024.0), 2)
    FROM index_sizes
    ORDER BY index_size DESC;
    
    -- Display index sizes
    RAISE NOTICE 'Largest Indexes (MB):';
    FOR rec IN 
        SELECT 
            table_name,
            index_name,
            ROUND(storage_size_mb, 2) as size_mb
        FROM storage_efficiency_results 
        WHERE metric_name = 'index_size'
        ORDER BY storage_size_bytes DESC
        LIMIT 10
    LOOP
        RAISE NOTICE '  %.%: % MB',
            rec.table_name, rec.index_name, rec.size_mb;
    END LOOP;
    
    -- 4. TABLE BLOAT ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 3: TABLE BLOAT ANALYSIS';
    RAISE NOTICE '-------------------------------';
    
    -- Analyze table bloat (requires pgstattuple extension)
    BEGIN
        -- Try to create extension
        CREATE EXTENSION IF NOT EXISTS pgstattuple;
        
        -- Analyze patient_vitals table bloat
        DECLARE
            bloat_stats RECORD;
        BEGIN
            SELECT * INTO bloat_stats 
            FROM pgstattuple('patient_vitals');
            
            INSERT INTO storage_efficiency_results (
                metric_name, table_name, storage_size_bytes,
                fillfactor, bloat_percentage, vacuum_efficiency,
                analysis_details
            ) VALUES (
                'table_bloat',
                'patient_vitals',
                bloat_stats.table_len,
                100, -- default fillfactor
                ROUND((bloat_stats.dead_tuple_len::NUMERIC / 
                       bloat_stats.table_len::NUMERIC) * 100, 2),
                CASE 
                    WHEN (bloat_stats.dead_tuple_len::NUMERIC / 
                          bloat_stats.table_len::NUMERIC) > 0.2 
                    THEN 'NEEDS_VACUUM'
                    WHEN (bloat_stats.dead_tuple_len::NUMERIC / 
                          bloat_stats.table_len::NUMERIC) > 0.1 
                    THEN 'RECOMMEND_VACUUM'
                    ELSE 'OPTIMAL'
                END,
                jsonb_build_object(
                    'table_len', bloat_stats.table_len,
                    'tuple_count', bloat_stats.tuple_count,
                    'tuple_len', bloat_stats.tuple_len,
                    'tuple_percent', bloat_stats.tuple_percent,
                    'dead_tuple_count', bloat_stats.dead_tuple_count,
                    'dead_tuple_len', bloat_stats.dead_tuple_len,
                    'dead_tuple_percent', bloat_stats.dead_tuple_percent,
                    'free_space', bloat_stats.free_space,
                    'free_percent', bloat_stats.free_percent
                )
            );
            
            RAISE NOTICE 'Table Bloat Analysis for patient_vitals:';
            RAISE NOTICE '  Total size: % MB', 
                ROUND(bloat_stats.table_len / (1024.0 * 1024.0), 2);
            RAISE NOTICE '  Live tuples: %, size: % MB', 
                bloat_stats.tuple_count,
                ROUND(bloat_stats.tuple_len / (1024.0 * 1024.0), 2);
            RAISE NOTICE '  Dead tuples: %, size: % MB (%%)', 
                bloat_stats.dead_tuple_count,
                ROUND(bloat_stats.dead_tuple_len / (1024.0 * 1024.0), 2),
                ROUND(bloat_stats.dead_tuple_percent, 2);
            RAISE NOTICE '  Free space: % MB (%%)', 
                ROUND(bloat_stats.free_space / (1024.0 * 1024.0), 2),
                ROUND(bloat_stats.free_percent, 2);
            RAISE NOTICE '  Bloat percentage: %%', 
                ROUND((bloat_stats.dead_tuple_len::NUMERIC / 
                       bloat_stats.table_len::NUMERIC) * 100, 2);
        END;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'pgstattuple extension not available, using estimate method';
        
        -- Fallback to estimate method
        INSERT INTO storage_efficiency_results (
            metric_name, table_name, bloat_percentage, vacuum_efficiency
        )
        SELECT 
            'table_bloat_estimate',
            relname,
            ROUND(
                (CASE 
                    WHEN n_dead_tup > 0 THEN 
                        (n_dead_tup::NUMERIC / (n_live_tup + n_dead_tup)) * 100
                    ELSE 0
                END), 2
            ),
            CASE 
                WHEN n_dead_tup::NUMERIC / (n_live_tup + n_dead_tup) > 0.2 
                THEN 'NEEDS_VACUUM'
                WHEN n_dead_tup::NUMERIC / (n_live_tup + n_dead_tup) > 0.1 
                THEN 'RECOMMEND_VACUUM'
                ELSE 'OPTIMAL'
            END
        FROM pg_stat_user_tables
        WHERE schemaname = 'public';
    END;
    
    -- 5. COMPRESSION ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 4: COMPRESSION ANALYSIS';
    RAISE NOTICE '--------------------------------';
    
    -- Analyze potential compression savings
    -- Note: PostgreSQL uses TOAST for automatic compression
    
    -- Check TOAST compression
    WITH toast_info AS (
        SELECT 
            t.relname as table_name,
            t.relpages as table_pages,
            c.relname as toast_table,
            c.relpages as toast_pages,
            CASE 
                WHEN c.relpages > 0 THEN 
                    ROUND((c.relpages::NUMERIC / 
                          (t.relpages + c.relpages)) * 100, 2)
                ELSE 0
            END as toast_percentage
        FROM pg_class t
        LEFT JOIN pg_class c ON t.reltoastrelid = c.oid
        WHERE t.relname = 'patient_vitals'
          AND t.relkind = 'r'
    )
    INSERT INTO storage_efficiency_results (
        metric_name, table_name, storage_size_bytes,
        compression_ratio, analysis_details
    )
    SELECT 
        'toast_compression',
        table_name,
        (table_pages + COALESCE(toast_pages, 0)) * 8192,
        CASE 
            WHEN toast_pages > 0 THEN 
                ROUND((table_pages::NUMERIC / 
                      (table_pages + toast_pages)), 2)
            ELSE 1
        END,
        jsonb_build_object(
            'table_pages', table_pages,
            'toast_pages', toast_pages,
            'toast_percentage', toast_percentage
        )
    FROM toast_info;
    
    -- 6. PARTITIONING ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 5: PARTITIONING ANALYSIS';
    RAISE NOTICE '---------------------------------';
    
    -- Analyze if partitioning would be beneficial
    DECLARE
        partition_stats JSONB;
    BEGIN
        -- Analyze data distribution for potential partitioning
        SELECT jsonb_build_object(
            'time_based_distribution', (
                SELECT jsonb_object_agg(
                    date_trunc('month', measurement_time)::text,
                    count
                )
                FROM (
                    SELECT 
                        date_trunc('month', measurement_time) as month,
                        COUNT(*) as count
                    FROM patient_vitals
                    GROUP BY date_trunc('month', measurement_time)
                    ORDER BY month
                ) t
            ),
            'department_distribution', (
                SELECT jsonb_object_agg(patient_department, count)
                FROM (
                    SELECT 
                        patient_department,
                        COUNT(*) as count
                    FROM patient_vitals
                    GROUP BY patient_department
                ) t
            ),
            'vital_type_distribution', (
                SELECT jsonb_object_agg(vital_type, count)
                FROM (
                    SELECT 
                        vital_type,
                        COUNT(*) as count
                    FROM patient_vitals
                    GROUP BY vital_type
                    ORDER BY count DESC
                    LIMIT 10
                ) t
            )
        ) INTO partition_stats;
        
        INSERT INTO storage_efficiency_results (
            metric_name, table_name, analysis_details
        ) VALUES (
            'partitioning_analysis',
            'patient_vitals',
            partition_stats
        );
        
        -- Display partitioning recommendations
        RAISE NOTICE 'Partitioning Analysis:';
        RAISE NOTICE '  Data distribution by month:';
        FOR rec IN (
            SELECT key as month, value::text as count
            FROM jsonb_each_text(
                partition_stats->'time_based_distribution'
            )
            ORDER BY key
        ) LOOP
            RAISE NOTICE '    %: % rows', rec.month, rec.count;
        END LOOP;
    END;
    
    -- 7. STORAGE OPTIMIZATION EXPERIMENTS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 6: STORAGE OPTIMIZATION EXPERIMENTS';
    RAISE NOTICE '-------------------------------------------';
    
    -- Test different storage configurations
    
    -- 7.1 Test with different FILLFACTOR values
    DECLARE
        test_fillfactor INTEGER;
        test_table_name VARCHAR := 'test_fillfactor_' || 
            to_char(NOW(), 'YYYYMMDD_HH24MISS');
    BEGIN
        FOR test_fillfactor IN (70, 85, 100) LOOP
            RAISE NOTICE 'Testing FILLFACTOR = %', test_fillfactor;
            
            -- Create test table with specific fillfactor
            EXECUTE format('
                CREATE TABLE %I (
                    LIKE patient_vitals INCLUDING DEFAULTS
                ) WITH (FILLFACTOR = %s)',
                test_table_name || '_ff' || test_fillfactor,
                test_fillfactor
            );
            
            -- Insert sample data
            EXECUTE format('
                INSERT INTO %I
                SELECT * FROM patient_vitals 
                LIMIT 10000',
                test_table_name || '_ff' || test_fillfactor
            );
            
            -- Analyze table size
            EXECUTE format('
                INSERT INTO storage_efficiency_results (
                    metric_name, table_name, storage_size_bytes,
                    fillfactor, row_count, avg_row_size_bytes
                )
                SELECT 
                    ''fillfactor_test'',
                    %L,
                    pg_total_relation_size(%L),
                    %s,
                    COUNT(*),
                    pg_relation_size(%L)::NUMERIC / COUNT(*)
                FROM %I',
                test_table_name || '_ff' || test_fillfactor,
                test_table_name || '_ff' || test_fillfactor,
                test_fillfactor,
                test_table_name || '_ff' || test_fillfactor,
                test_table_name || '_ff' || test_fillfactor
            );
            
            -- Cleanup
            EXECUTE format('DROP TABLE %I', 
                test_table_name || '_ff' || test_fillfactor);
        END LOOP;
    END;
    
    -- 7.2 Test column compression
    RAISE NOTICE 'Testing column compression strategies...';
    
    -- Analyze column data types and sizes
    WITH column_stats AS (
        SELECT 
            table_name,
            column_name,
            data_type,
            character_maximum_length,
            CASE 
                WHEN data_type LIKE 'varchar%' THEN 'TEXT_COMPRESSIBLE'
                WHEN data_type LIKE 'text%' THEN 'TEXT_COMPRESSIBLE'
                WHEN data_type IN ('json', 'jsonb') THEN 'JSON_COMPRESSIBLE'
                ELSE 'LOW_COMPRESSION'
            END as compression_potential,
            pg_column_size(column_name::text) as sample_size
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'patient_vitals'
    )
    INSERT INTO storage_efficiency_results (
        metric_name, table_name, analysis_details
    )
    SELECT 
        'column_compression',
        'patient_vitals',
        jsonb_build_object(
            'columns', jsonb_agg(
                jsonb_build_object(
                    'column_name', column_name,
                    'data_type', data_type,
                    'compression_potential', compression_potential,
                    'sample_size_bytes', sample_size
                )
            ),
            'compression_recommendations', CASE 
                WHEN COUNT(*) FILTER (
                    WHERE compression_potential = 'TEXT_COMPRESSIBLE'
                ) > 3 
                THEN 'Consider text compression or normalization'
                ELSE 'Current data types appear efficient'
            END
        )
    FROM column_stats;
    
    -- 8. VACUUM AND MAINTENANCE ANALYSIS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 7: VACUUM AND MAINTENANCE ANALYSIS';
    RAISE NOTICE '------------------------------------------';
    
    -- Check vacuum statistics
    WITH vacuum_stats AS (
        SELECT 
            schemaname,
            relname,
            n_live_tup,
            n_dead_tup,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    )
    INSERT INTO storage_efficiency_results (
        metric_name, table_name, analysis_details
    )
    SELECT 
        'vacuum_analysis',
        relname,
        jsonb_build_object(
            'live_tuples', n_live_tup,
            'dead_tuples', n_dead_tup,
            'dead_tuple_percentage', CASE 
                WHEN n_live_tup + n_dead_tup > 0 THEN
                    ROUND((n_dead_tup::NUMERIC / 
                          (n_live_tup + n_dead_tup)) * 100, 2)
                ELSE 0
            END,
            'last_vacuum', last_vacuum,
            'last_autovacuum', last_autovacuum,
            'vacuum_count', vacuum_count,
            'autovacuum_count', autovacuum_count,
            'maintenance_recommendation', CASE 
                WHEN n_dead_tup::NUMERIC / (n_live_tup + n_dead_tup) > 0.2
                THEN 'Manual vacuum recommended'
                WHEN last_autovacuum IS NULL OR 
                     last_autovacuum < NOW() - INTERVAL '7 days'
                THEN 'Monitor autovacuum activity'
                ELSE 'Maintenance adequate'
            END
        )
    FROM vacuum_stats;
    
    -- 9. STORAGE RECOMMENDATIONS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'SECTION 8: STORAGE RECOMMENDATIONS';
    RAISE NOTICE '-----------------------------------';
    
    -- Generate storage recommendations
    INSERT INTO storage_efficiency_results (
        metric_name, table_name, recommendations
    )
    SELECT 
        'storage_recommendations',
        'patient_vitals',
        string_agg(recommendation, E'\n')
    FROM (
        -- Recommendation 1: Based on bloat
        SELECT 1 as priority,
            CASE 
                WHEN bloat_percentage > 20 THEN
                    '⚠️ HIGH BLOAT: Run VACUUM FULL on patient_vitals'
                WHEN bloat_percentage > 10 THEN
                    '⚠️ MODERATE BLOAT: Schedule regular VACUUM'
                ELSE '✅ BLOAT LEVEL OPTIMAL'
            END as recommendation
        FROM storage_efficiency_results
        WHERE metric_name LIKE '%bloat%'
          AND table_name = 'patient_vitals'
        
        UNION ALL
        
        -- Recommendation 2: Based on index size
        SELECT 2 as priority,
            CASE 
                WHEN total_index_size > total_table_size * 0.5 THEN
                    '⚠️ LARGE INDEXES: Review index usage and consider partial indexes'
                ELSE '✅ INDEX SIZE OPTIMAL'
            END as recommendation
        FROM (
            SELECT 
                SUM(CASE WHEN metric_name = 'table_size' 
                         THEN storage_size_bytes ELSE 0 END) as total_table_size,
                SUM(CASE WHEN metric_name = 'index_size' 
                         THEN storage_size_bytes ELSE 0 END) as total_index_size
            FROM storage_efficiency_results
            WHERE table_name = 'patient_vitals'
        ) t
        
        UNION ALL
        
        -- Recommendation 3: Based on data distribution
        SELECT 3 as priority,
            CASE 
                WHEN max_month_count > avg_month_count * 3 THEN
                    '⚠️ UNEVEN DISTRIBUTION: Consider time-based partitioning'
                ELSE '✅ DATA DISTRIBUTION EVEN'
            END as recommendation
        FROM (
            SELECT 
                MAX(value::INTEGER) as max_month_count,
                AVG(value::INTEGER) as avg_month_count
            FROM (
                SELECT value
                FROM storage_efficiency_results s,
                jsonb_each(s.analysis_details->'time_based_distribution')
                WHERE metric_name = 'partitioning_analysis'
            ) t
        ) t
        
        UNION ALL
        
        -- Recommendation 4: General best practices
        SELECT 4 as priority,
            '✅ GENERAL: Consider implementing:' || E'\n' ||
            '   • Regular maintenance jobs' || E'\n' ||
            '   • Monitor growth trends' || E'\n' ||
            '   • Archive old data' as recommendation
    ) recs
    ORDER BY priority;
    
    -- Display recommendations
    RAISE NOTICE 'Storage Recommendations:';
    FOR rec IN 
        SELECT recommendations
        FROM storage_efficiency_results
        WHERE metric_name = 'storage_recommendations'
    LOOP
        RAISE NOTICE '%', rec.recommendations;
    END LOOP;
    
    -- 10. SUMMARY REPORT
    -- ============================================
    analysis_end := clock_timestamp();
    
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'STORAGE EFFICIENCY ANALYSIS SUMMARY';
    RAISE NOTICE '============================================';
    
    -- Display summary statistics
    DECLARE
        total_storage_mb NUMERIC;
        table_storage_mb NUMERIC;
        index_storage_mb NUMERIC;
        total_rows BIGINT;
        avg_row_size NUMERIC;
    BEGIN
        SELECT 
            SUM(CASE WHEN metric_name = 'table_size' 
                    THEN storage_size_mb ELSE 0 END),
            SUM(CASE WHEN metric_name = 'index_size' 
                    THEN storage_size_mb ELSE 0 END),
            SUM(CASE WHEN metric_name = 'table_size' 
                    THEN row_count ELSE 0 END),
            AVG(CASE WHEN metric_name = 'table_size' 
                    THEN avg_row_size_bytes ELSE NULL END)
        INTO total_storage_mb, index_storage_mb, total_rows, avg_row_size
        FROM storage_efficiency_results;
        
        table_storage_mb := total_storage_mb - index_storage_mb;
        
        RAISE NOTICE 'Total Storage: % MB', ROUND(total_storage_mb, 2);
        RAISE NOTICE '  Table Data: % MB (%%)', 
            ROUND(table_storage_mb, 2),
            ROUND((table_storage_mb / total_storage_mb) * 100, 1);
        RAISE NOTICE '  Indexes: % MB (%%)', 
            ROUND(index_storage_mb, 2),
            ROUND((index_storage_mb / total_storage_mb) * 100, 1);
        RAISE NOTICE 'Total Rows: %', total_rows;
        RAISE NOTICE 'Average Row Size: % bytes', ROUND(avg_row_size, 2);
        RAISE NOTICE 'Analysis Duration: % seconds', 
            ROUND(EXTRACT(EPOCH FROM (analysis_end - analysis_start)), 2);
    END;
    
    -- Create summary view
    DROP VIEW IF EXISTS storage_efficiency_summary;
    CREATE VIEW storage_efficiency_summary AS
    SELECT 
        table_name,
        SUM(CASE WHEN metric_name = 'table_size' 
                THEN storage_size_mb ELSE 0 END) as table_size_mb,
        SUM(CASE WHEN metric_name = 'index_size' 
                THEN storage_size_mb ELSE 0 END) as index_size_mb,
        SUM(CASE WHEN metric_name = 'table_size' 
                THEN row_count ELSE 0 END) as row_count,
        AVG(CASE WHEN metric_name = 'table_size' 
                THEN avg_row_size_bytes ELSE NULL END) as avg_row_bytes,
        MAX(CASE WHEN metric_name LIKE '%bloat%' 
                THEN bloat_percentage ELSE NULL END) as bloat_percentage,
        MAX(CASE WHEN metric_name = 'toast_compression' 
                THEN compression_ratio ELSE NULL END) as compression_ratio
    FROM storage_efficiency_results
    GROUP BY table_name
    ORDER BY table_size_mb DESC;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Detailed analysis saved in: storage_efficiency_results';
    RAISE NOTICE 'Summary view created: storage_efficiency_summary';
    RAISE NOTICE '';
    RAISE NOTICE 'To view results:';
    RAISE NOTICE '  SELECT * FROM storage_efficiency_summary;';
    RAISE NOTICE '  SELECT * FROM storage_efficiency_results WHERE metric_name = ''storage_recommendations'';';
    RAISE NOTICE '============================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during storage analysis: %', SQLERRM;
        RAISE;
END $$;

-- ============================================
-- SUPPORTING FUNCTIONS
-- ============================================

-- Function to estimate table growth
CREATE OR REPLACE FUNCTION estimate_table_growth(
    table_name VARCHAR,
    days_history INTEGER DEFAULT 30
)
RETURNS TABLE (
    date DATE,
    row_count BIGINT,
    size_mb NUMERIC,
    daily_growth_mb NUMERIC,
    projected_size_30d_mb NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH daily_stats AS (
        SELECT 
            date_trunc('day', analysis_timestamp) as stat_date,
            MAX(storage_size_mb) as size_mb,
            MAX(row_count) as row_count
        FROM storage_efficiency_results
        WHERE table_name = estimate_table_growth.table_name
          AND metric_name = 'table_size'
          AND analysis_timestamp >= NOW() - (days_history || ' days')::INTERVAL
        GROUP BY date_trunc('day', analysis_timestamp)
    ),
    growth_calc AS (
        SELECT 
            stat_date,
            size_mb,
            row_count,
            LAG(size_mb) OVER (ORDER BY stat_date) as prev_size_mb,
            LAG(stat_date) OVER (ORDER BY stat_date) as prev_date
        FROM daily_stats
    )
    SELECT 
        stat_date::DATE,
        row_count,
        size_mb,
        CASE 
            WHEN prev_size_mb IS NOT NULL THEN
                (size_mb - prev_size_mb) / 
                EXTRACT(DAY FROM stat_date - prev_date)
            ELSE 0
        END as daily_growth_mb,
        size_mb + (
            CASE 
                WHEN prev_size_mb IS NOT NULL THEN
                    (size_mb - prev_size_mb) / 
                    EXTRACT(DAY FROM stat_date - prev_date)
                ELSE 0
            END * 30
        ) as projected_size_30d_mb
    FROM growth_calc
    ORDER BY stat_date DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to generate storage optimization SQL
CREATE OR REPLACE FUNCTION generate_storage_optimization_sql()
RETURNS TEXT AS $$
DECLARE
    optimization_sql TEXT := '';
BEGIN
    -- Generate optimization SQL based on analysis
    
    -- 1. Vacuum recommendations
    SELECT optimization_sql || 
        '-- Run vacuum on tables with high bloat' || E'\n' ||
        string_agg(
            'VACUUM ANALYZE ' || table_name || ';',
            E'\n'
        ) || E'\n\n'
    INTO optimization_sql
    FROM storage_efficiency_results
    WHERE metric_name LIKE '%bloat%'
      AND bloat_percentage > 10;
    
    -- 2. Index optimization recommendations
    SELECT optimization_sql ||
        '-- Consider reviewing these large indexes' || E'\n' ||
        string_agg(
            '-- Index: ' || index_name || ' on ' || table_name || 
            ' (' || ROUND(storage_size_mb, 2) || ' MB)',
            E'\n'
        ) || E'\n\n'
    INTO optimization_sql
    FROM storage_efficiency_results
    WHERE metric_name = 'index_size'
      AND storage_size_mb > 100
    ORDER BY storage_size_mb DESC
    LIMIT 5;
    
    -- 3. Partitioning recommendations
    SELECT optimization_sql ||
        '-- Consider partitioning for time-based data' || E'\n' ||
        '/*' || E'\n' ||
        'CREATE TABLE patient_vitals_partitioned (' || E'\n' ||
        '    LIKE patient_vitals INCLUDING ALL' || E'\n' ||
        ') PARTITION BY RANGE (measurement_time);' || E'\n' ||
        E'\n' ||
        '-- Create monthly partitions' || E'\n' ||
        'CREATE TABLE patient_vitals_2024_01 PARTITION OF patient_vitals_partitioned' || E'\n' ||
        '    FOR VALUES FROM (''2024-01-01'') TO (''2024-02-01'');' || E'\n' ||
        '-- Add more partitions as needed' || E'\n' ||
        '*/' || E'\n\n'
    INTO optimization_sql
    FROM storage_efficiency_results
    WHERE metric_name = 'partitioning_analysis'
    LIMIT 1;
    
    -- 4. Maintenance schedule
    optimization_sql := optimization_sql ||
        '-- Recommended maintenance schedule' || E'\n' ||
        '/*' || E'\n' ||
        '-- Daily: Monitor table growth and bloat' || E'\n' ||
        '-- Weekly: Run ANALYZE on all tables' || E'\n' ||
        '-- Monthly: Review and optimize indexes' || E'\n' ||
        '-- Quarterly: Consider VACUUM FULL on large tables' || E'\n' ||
        '*/';
    
    RETURN optimization_sql;
END;
$$ LANGUAGE plpgsql;

-- Function to compare storage efficiency
CREATE OR REPLACE FUNCTION compare_storage_efficiency()
RETURNS TABLE (
    metric VARCHAR,
    current_value NUMERIC,
    recommended_value NUMERIC,
    improvement_potential NUMERIC,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'Bloat Percentage'::VARCHAR as metric,
        MAX(CASE WHEN metric_name LIKE '%bloat%' 
                THEN bloat_percentage ELSE NULL END) as current_value,
        5 as recommended_value,
        MAX(CASE WHEN metric_name LIKE '%bloat%' 
                THEN bloat_percentage ELSE 0 END) - 5 as improvement_potential,
        'Reduce bloat through regular vacuuming' as recommendation
    
    UNION ALL
    
    SELECT 
        'Index to Table Ratio'::VARCHAR,
        ROUND(
            SUM(CASE WHEN metric_name = 'index_size' 
                    THEN storage_size_mb ELSE 0 END) /
            NULLIF(SUM(CASE WHEN metric_name = 'table_size' 
                           THEN storage_size_mb ELSE 0 END), 0) * 100,
            2
        ),
        50,
        ROUND(
            SUM(CASE WHEN metric_name = 'index_size' 
                    THEN storage_size_mb ELSE 0 END) /
            NULLIF(SUM(CASE WHEN metric_name = 'table_size' 
                           THEN storage_size_mb ELSE 0 END), 0) * 100 - 50,
            2
        ),
        'Optimize indexes to reduce storage overhead'
    
    UNION ALL
    
    SELECT 
        'Average Row Size'::VARCHAR,
        AVG(CASE WHEN metric_name = 'table_size' 
                THEN avg_row_size_bytes ELSE NULL END),
        200,
        AVG(CASE WHEN metric_name = 'table_size' 
                THEN avg_row_size_bytes ELSE 0 END) - 200,
        'Consider normalizing data or using appropriate data types'
    
    ORDER BY improvement_potential DESC;
END;
$$ LANGUAGE plpgsql;

-- Create monitoring view
CREATE OR REPLACE VIEW storage_monitoring_dashboard AS
SELECT 
    table_name,
    ROUND(table_size_mb, 2) as table_size_mb,
    ROUND(index_size_mb, 2) as index_size_mb,
    ROUND(table_size_mb + index_size_mb, 2) as total_size_mb,
    row_count,
    ROUND(avg_row_bytes, 2) as avg_row_bytes,
    ROUND(bloat_percentage, 2) as bloat_percentage,
    ROUND(compression_ratio, 2) as compression_ratio,
    CASE 
        WHEN bloat_percentage > 20 THEN '🔴 High Bloat'
        WHEN bloat_percentage > 10 THEN '🟡 Moderate Bloat'
        ELSE '🟢 Optimal'
    END as bloat_status,
    CASE 
        WHEN index_size_mb > table_size_mb * 0.5 THEN '🔴 Large Indexes'
        WHEN index_size_mb > table_size_mb * 0.3 THEN '🟡 Moderate Indexes'
        ELSE '🟢 Optimal Indexes'
    END as index_status
FROM storage_efficiency_summary;

-- Final cleanup
\timing off

RAISE NOTICE '';
RAISE NOTICE '============================================';
RAISE NOTICE 'ADDITIONAL UTILITIES AVAILABLE:';
RAISE NOTICE '============================================';
RAISE NOTICE '1. Estimate table growth:';
RAISE NOTICE '   SELECT * FROM estimate_table_growth(''patient_vitals'');';
RAISE NOTICE '';
RAISE NOTICE '2. Generate optimization SQL:';
RAISE NOTICE '   SELECT generate_storage_optimization_sql();';
RAISE NOTICE '';
RAISE NOTICE '3. Compare storage efficiency:';
RAISE NOTICE '   SELECT * FROM compare_storage_efficiency();';
RAISE NOTICE '';
RAISE NOTICE '4. View monitoring dashboard:';
RAISE NOTICE '   SELECT * FROM storage_monitoring_dashboard;';
RAISE NOTICE '============================================';