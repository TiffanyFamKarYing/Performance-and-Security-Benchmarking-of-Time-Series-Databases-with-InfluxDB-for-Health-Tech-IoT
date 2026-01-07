-- POSTGRESQL/TIMESCALEDB SETUP WITH RLS
-- Health IoT Benchmarking Database

-- 1. CREATE DATABASE AND CONNECT
-- ============================================
DROP DATABASE IF EXISTS health_iot_benchmark;
CREATE DATABASE health_iot_benchmark;

\c health_iot_benchmark;

-- 2. CREATE EXTENSIONS
-- ============================================
-- Enable TimescaleDB for time-series optimization
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Enable additional useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 3. CREATE MAIN PATIENT VITALS TABLE
-- ============================================
CREATE TABLE patient_vitals (
    -- Primary identifier
    record_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Time-series data
    measurement_time TIMESTAMPTZ NOT NULL,
    ingestion_time TIMESTAMPTZ DEFAULT NOW(),
    
    -- Patient information
    patient_id VARCHAR(20) NOT NULL,
    patient_department VARCHAR(30) NOT NULL,
    patient_room VARCHAR(10),
    patient_age INTEGER,
    patient_gender CHAR(1) CHECK (patient_gender IN ('M', 'F', 'O')),
    
    -- Device information
    device_id VARCHAR(20) NOT NULL,
    device_type VARCHAR(30),
    device_location VARCHAR(50),
    
    -- Vital sign data
    vital_type VARCHAR(40) NOT NULL,
    vital_value DECIMAL(10,3) NOT NULL,
    vital_unit VARCHAR(10),
    
    -- Clinical context
    is_alert BOOLEAN DEFAULT FALSE,
    alert_severity INTEGER CHECK (alert_severity BETWEEN 1 AND 5),
    alert_description TEXT,
    
    -- Data quality flags
    data_quality_score INTEGER DEFAULT 100 CHECK (data_quality_score BETWEEN 0 AND 100),
    is_manual_entry BOOLEAN DEFAULT FALSE,
    
    -- Security metadata
    data_classification VARCHAR(20) DEFAULT 'PHI' CHECK (data_classification IN ('PUBLIC', 'CONFIDENTIAL', 'PHI', 'RESTRICTED')),
    access_level_required VARCHAR(20) DEFAULT 'HEALTHCARE_PROVIDER',
    created_by VARCHAR(50),
    last_modified_by VARCHAR(50),
    
    -- Audit fields
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Indexes will be created separately
    CONSTRAINT chk_vital_value_range CHECK (
        CASE 
            WHEN vital_type = 'heart_rate_bpm' THEN vital_value BETWEEN 30 AND 250
            WHEN vital_type = 'blood_pressure_sys_mmhg' THEN vital_value BETWEEN 60 AND 250
            WHEN vital_type = 'blood_pressure_dia_mmhg' THEN vital_value BETWEEN 40 AND 150
            WHEN vital_type = 'spo2_percent' THEN vital_value BETWEEN 70 AND 100
            WHEN vital_type = 'temperature_c' THEN vital_value BETWEEN 35 AND 42
            ELSE vital_value > 0
        END
    )
);

-- 4. CONVERT TO HYPERTABLE (TimescaleDB)
-- ============================================
-- Create hypertable for time-series optimization
SELECT create_hypertable(
    'patient_vitals', 
    'measurement_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 5. CREATE INDEXES FOR PERFORMANCE
-- ============================================
-- Time-based indexes
CREATE INDEX idx_vitals_measurement_time 
ON patient_vitals (measurement_time DESC);

CREATE INDEX idx_vitals_ingestion_time 
ON patient_vitals (ingestion_time DESC);

-- Patient-based indexes
CREATE INDEX idx_vitals_patient_time 
ON patient_vitals (patient_id, measurement_time DESC);

CREATE INDEX idx_vitals_patient_department 
ON patient_vitals (patient_department, measurement_time DESC);

-- Vital type indexes
CREATE INDEX idx_vitals_vital_type_time 
ON patient_vitals (vital_type, measurement_time DESC);

CREATE INDEX idx_vitals_vital_type_patient 
ON patient_vitals (vital_type, patient_id, measurement_time DESC);

-- Alert indexes
CREATE INDEX idx_vitals_alerts 
ON patient_vitals (measurement_time DESC) 
WHERE is_alert = TRUE;

-- Device indexes
CREATE INDEX idx_vitals_device_time 
ON patient_vitals (device_id, measurement_time DESC);

-- Composite index for common queries
CREATE INDEX idx_vitals_patient_vital_time 
ON patient_vitals (patient_id, vital_type, measurement_time DESC);

-- 6. IMPLEMENT ROW-LEVEL SECURITY (RLS) - HIPAA COMPLIANCE
-- ============================================
-- Enable RLS on the table
ALTER TABLE patient_vitals ENABLE ROW LEVEL SECURITY;

-- Create roles for different access levels
CREATE ROLE health_admin WITH NOLOGIN;
CREATE ROLE doctor_role WITH NOLOGIN;
CREATE ROLE nurse_role WITH NOLOGIN;
CREATE ROLE patient_role WITH NOLOGIN;
CREATE ROLE researcher_role WITH NOLOGIN;
CREATE ROLE auditor_role WITH NOLOGIN;

-- Grant basic permissions
GRANT CONNECT ON DATABASE health_iot_benchmark TO health_admin, doctor_role, nurse_role, patient_role, researcher_role, auditor_role;
GRANT USAGE ON SCHEMA public TO health_admin, doctor_role, nurse_role, patient_role, researcher_role, auditor_role;

-- POLICY 1: Health Administrators - Full access
CREATE POLICY policy_health_admin ON patient_vitals
    FOR ALL TO health_admin
    USING (true)
    WITH CHECK (true);

-- POLICY 2: Doctors - Access to patients in their department
CREATE POLICY policy_doctor_access ON patient_vitals
    FOR ALL TO doctor_role
    USING (
        -- Doctors can see all patients in their assigned department
        patient_department = current_setting('app.current_department') 
        AND data_classification IN ('PHI', 'CONFIDENTIAL', 'PUBLIC')
    )
    WITH CHECK (
        -- Doctors can only insert/update data for their department
        patient_department = current_setting('app.current_department')
        AND created_by = current_user
    );

-- POLICY 3: Nurses - Limited access within their department
CREATE POLICY policy_nurse_access ON patient_vitals
    FOR SELECT TO nurse_role
    USING (
        patient_department = current_setting('app.current_department')
        AND data_classification IN ('PHI', 'PUBLIC')
        AND vital_type NOT IN ('psychiatric_notes', 'sensitive_diagnosis')
    );

CREATE POLICY policy_nurse_insert ON patient_vitals
    FOR INSERT TO nurse_role
    WITH CHECK (
        patient_department = current_setting('app.current_department')
        AND data_classification != 'RESTRICTED'
        AND vital_type NOT IN ('psychiatric_notes', 'sensitive_diagnosis')
        AND created_by = current_user
    );

-- POLICY 4: Patients - Can only see their own data
CREATE POLICY policy_patient_access ON patient_vitals
    FOR SELECT TO patient_role
    USING (
        patient_id = current_setting('app.current_patient_id')
        AND data_classification IN ('PHI', 'PUBLIC')
    );

-- POLICY 5: Researchers - De-identified data only
CREATE POLICY policy_researcher_access ON patient_vitals
    FOR SELECT TO researcher_role
    USING (
        data_classification = 'PUBLIC'
        AND patient_id LIKE 'ANON%'  -- Only anonymized data
    );

-- POLICY 6: Auditors - Read-only access to all for compliance
CREATE POLICY policy_auditor_access ON patient_vitals
    FOR SELECT TO auditor_role
    USING (true);

-- POLICY 7: Default deny all (safety net)
CREATE POLICY policy_default_deny ON patient_vitals
    USING (false);

-- 7. CREATE SECURITY FUNCTIONS AND TRIGGERS
-- ============================================
-- Function to automatically set security metadata
CREATE OR REPLACE FUNCTION set_security_metadata()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_by = current_user;
    NEW.updated_at = NOW();
    
    -- Auto-classify data based on content
    IF NEW.vital_type IN ('psychiatric_notes', 'hiv_status', 'genetic_data') THEN
        NEW.data_classification = 'RESTRICTED';
    ELSIF NEW.patient_id IS NOT NULL THEN
        NEW.data_classification = 'PHI';
    ELSE
        NEW.data_classification = 'PUBLIC';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Trigger to auto-set security metadata
CREATE TRIGGER trg_security_metadata
    BEFORE INSERT OR UPDATE ON patient_vitals
    FOR EACH ROW
    EXECUTE FUNCTION set_security_metadata();

-- Function to check access permissions
CREATE OR REPLACE FUNCTION check_patient_access(
    p_patient_id VARCHAR,
    p_requester_role VARCHAR
)
RETURNS BOOLEAN AS $$
DECLARE
    v_department VARCHAR;
    v_has_access BOOLEAN;
BEGIN
    -- Get requester's department
    IF p_requester_role = 'doctor_role' OR p_requester_role = 'nurse_role' THEN
        v_department := current_setting('app.current_department', true);
        
        -- Check if patient is in same department
        SELECT EXISTS (
            SELECT 1 FROM patient_vitals 
            WHERE patient_id = p_patient_id 
            AND patient_department = v_department
            LIMIT 1
        ) INTO v_has_access;
        
        RETURN v_has_access;
    ELSE
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 8. CREATE AUDIT LOGGING TABLE
-- ============================================
CREATE TABLE audit_logs (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_time TIMESTAMPTZ DEFAULT NOW(),
    event_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(100),
    record_id UUID,
    patient_id VARCHAR(20),
    user_name VARCHAR(100),
    user_role VARCHAR(50),
    ip_address INET,
    query_text TEXT,
    old_values JSONB,
    new_values JSONB,
    is_successful BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

SELECT create_hypertable('audit_logs', 'event_time');

CREATE INDEX idx_audit_event_time ON audit_logs (event_time DESC);
CREATE INDEX idx_audit_patient ON audit_logs (patient_id, event_time DESC);
CREATE INDEX idx_audit_user ON audit_logs (user_name, event_time DESC);

-- Audit trigger function
CREATE OR REPLACE FUNCTION audit_patient_vitals()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_logs (
        event_type,
        table_name,
        record_id,
        patient_id,
        user_name,
        user_role,
        old_values,
        new_values,
        query_text
    ) VALUES (
        TG_OP,
        TG_TABLE_NAME,
        COALESCE(NEW.record_id, OLD.record_id),
        COALESCE(NEW.patient_id, OLD.patient_id),
        current_user,
        current_setting('app.current_role', true),
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN to_jsonb(OLD) ELSE NULL END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN to_jsonb(NEW) ELSE NULL END,
        current_query()
    );
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create audit trigger
CREATE TRIGGER trg_audit_patient_vitals
    AFTER INSERT OR UPDATE OR DELETE ON patient_vitals
    FOR EACH ROW
    EXECUTE FUNCTION audit_patient_vitals();

-- 9. CREATE MATERIALIZED VIEWS FOR PERFORMANCE
-- ============================================
-- Daily patient summary
CREATE MATERIALIZED VIEW mv_daily_patient_summary AS
SELECT 
    DATE(measurement_time) as measurement_date,
    patient_id,
    patient_department,
    vital_type,
    COUNT(*) as reading_count,
    AVG(vital_value) as avg_value,
    MIN(vital_value) as min_value,
    MAX(vital_value) as max_value,
    STDDEV(vital_value) as stddev_value,
    SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as alert_count
FROM patient_vitals
WHERE measurement_time >= NOW() - INTERVAL '30 days'
GROUP BY DATE(measurement_time), patient_id, patient_department, vital_type
WITH DATA;

CREATE UNIQUE INDEX idx_mv_daily_summary 
ON mv_daily_patient_summary (measurement_date, patient_id, vital_type);

-- Department statistics
CREATE MATERIALIZED VIEW mv_department_stats AS
SELECT 
    patient_department,
    vital_type,
    COUNT(*) as total_readings,
    AVG(vital_value) as department_avg,
    MIN(vital_value) as department_min,
    MAX(vital_value) as department_max,
    SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) as total_alerts,
    (SUM(CASE WHEN is_alert THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 as alert_percentage
FROM patient_vitals
WHERE measurement_time >= NOW() - INTERVAL '7 days'
GROUP BY patient_department, vital_type
WITH DATA;

CREATE UNIQUE INDEX idx_mv_department_stats 
ON mv_department_stats (patient_department, vital_type);

-- 10. CREATE REFRESH FUNCTION FOR MATERIALIZED VIEWS
-- ============================================
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_patient_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_department_stats;
END;
$$ LANGUAGE plpgsql;

-- 11. CREATE SCHEDULED JOB FOR VIEW REFRESH (using pg_cron if available)
-- ============================================
-- Note: Requires pg_cron extension
-- CREATE EXTENSION IF NOT EXISTS pg_cron;
-- 
-- SELECT cron.schedule(
--     'refresh-patient-views',
--     '*/5 * * * *',  -- Every 5 minutes
--     'SELECT refresh_materialized_views();'
-- );

-- 12. SETUP COMPLETION MESSAGE
-- ============================================
DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'POSTGRESQL/TIMESCALEDB SETUP COMPLETE';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Database: health_iot_benchmark';
    RAISE NOTICE 'Table: patient_vitals (with RLS enabled)';
    RAISE NOTICE 'Security roles created:';
    RAISE NOTICE '  • health_admin (full access)';
    RAISE NOTICE '  • doctor_role (department-level access)';
    RAISE NOTICE '  • nurse_role (limited department access)';
    RAISE NOTICE '  • patient_role (own data only)';
    RAISE NOTICE '  • researcher_role (de-identified data)';
    RAISE NOTICE '  • auditor_role (read-only all)';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Create users and assign roles';
    RAISE NOTICE '2. Import health IoT data';
    RAISE NOTICE '3. Test security policies';
    RAISE NOTICE '============================================';
END $$;