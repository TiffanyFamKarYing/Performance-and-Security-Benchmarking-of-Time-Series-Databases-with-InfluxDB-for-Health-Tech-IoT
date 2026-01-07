// MongoDB Security Roles and Access Control
// Comprehensive security configuration for Health IoT database

// Configuration
const DATABASE_NAME = "health_iot_benchmark";
const ADMIN_USER = "health_iot_admin";
const ADMIN_PASSWORD = "HealthIoT123!";

// Function to create custom roles for Health IoT application
function createCustomRoles() {
    print("Creating custom security roles...");
    
    const roles = [
        {
            role: "health_iot_reader",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "patient_vitals" },
                    actions: ["find", "aggregate", "count", "distinct"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "audit_logs" },
                    actions: ["find"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "recent_vitals_view" },
                    actions: ["find"]
                }
            ],
            roles: []
        },
        {
            role: "health_iot_writer",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "patient_vitals" },
                    actions: ["insert", "update", "find", "aggregate"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "audit_logs" },
                    actions: ["insert", "find"]
                }
            ],
            roles: ["health_iot_reader"]
        },
        {
            role: "health_iot_alert_manager",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "patient_vitals" },
                    actions: ["find", "update", "aggregate"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "alerts" },
                    actions: ["insert", "update", "find", "remove"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "alert_rules" },
                    actions: ["find", "update"]
                }
            ],
            roles: ["health_iot_reader"]
        },
        {
            role: "health_iot_data_analyst",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "patient_vitals" },
                    actions: ["find", "aggregate", "count", "distinct", "mapReduce"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "aggregated_stats" },
                    actions: ["find", "aggregate", "insert"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "system_metrics" },
                    actions: ["find", "aggregate"]
                }
            ],
            roles: ["health_iot_reader"]
        },
        {
            role: "health_iot_admin",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "" },
                    actions: ["dbAdmin", "dbOwner", "userAdmin"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "patient_vitals" },
                    actions: ["collStats", "indexStats", "validate", "storageDetails"]
                }
            ],
            roles: ["health_iot_writer", "health_iot_alert_manager", "health_iot_data_analyst"]
        },
        {
            role: "health_iot_auditor",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "audit_logs" },
                    actions: ["find", "aggregate", "count"]
                },
                {
                    resource: { db: DATABASE_NAME, collection: "patient_vitals" },
                    actions: ["find"]  // Read-only access for audit purposes
                }
            ],
            roles: []
        },
        {
            role: "health_iot_backup",
            privileges: [
                {
                    resource: { db: DATABASE_NAME, collection: "" },
                    actions: ["find", "listCollections", "listIndexes"]
                }
            ],
            roles: []
        }
    ];
    
    const createdRoles = [];
    const existingRoles = db.getRoles({ showBuiltinRoles: false });
    
    roles.forEach(roleDef => {
        // Check if role already exists
        const roleExists = existingRoles.some(r => r.role === roleDef.role);
        
        if (!roleExists) {
            try {
                db.createRole({
                    role: roleDef.role,
                    privileges: roleDef.privileges,
                    roles: roleDef.roles
                });
                createdRoles.push(roleDef.role);
                print(`  Created role: ${roleDef.role}`);
            } catch (error) {
                print(`  Error creating role ${roleDef.role}: ${error.message}`);
            }
        } else {
            print(`  Role already exists: ${roleDef.role}`);
        }
    });
    
    return createdRoles;
}

// Function to create application users with specific roles
function createApplicationUsers() {
    print("\nCreating application users...");
    
    const users = [
        {
            user: "app_reader",
            pwd: "ReaderAppPass123!",
            roles: ["health_iot_reader"],
            customData: {
                description: "Application read-only user",
                department: "Analytics",
                created: new Date()
            }
        },
        {
            user: "app_writer",
            pwd: "WriterAppPass123!",
            roles: ["health_iot_writer"],
            customData: {
                description: "Application write user for data ingestion",
                department: "Data Ingestion",
                created: new Date()
            }
        },
        {
            user: "alert_processor",
            pwd: "AlertProcPass123!",
            roles: ["health_iot_alert_manager"],
            customData: {
                description: "Alert processing service account",
                department: "Monitoring",
                created: new Date()
            }
        },
        {
            user: "data_analyst",
            pwd: "AnalystPass123!",
            roles: ["health_iot_data_analyst"],
            customData: {
                description: "Data analyst user for reporting",
                department: "Business Intelligence",
                created: new Date()
            }
        },
        {
            user: "auditor",
            pwd: "AuditorPass123!",
            roles: ["health_iot_auditor"],
            customData: {
                description: "Security auditor user",
                department: "Security",
                created: new Date()
            }
        },
        {
            user: "backup_service",
            pwd: "BackupPass123!",
            roles: ["health_iot_backup"],
            customData: {
                description: "Backup service account",
                department: "Operations",
                created: new Date()
            }
        }
    ];
    
    const createdUsers = [];
    
    users.forEach(userDef => {
        // Check if user already exists
        const existingUser = db.getUser(userDef.user);
        
        if (!existingUser) {
            try {
                db.createUser({
                    user: userDef.user,
                    pwd: userDef.pwd,
                    roles: userDef.roles,
                    customData: userDef.customData
                });
                createdUsers.push(userDef.user);
                print(`  Created user: ${userDef.user} (${userDef.customData.description})`);
            } catch (error) {
                print(`  Error creating user ${userDef.user}: ${error.message}`);
            }
        } else {
            print(`  User already exists: ${userDef.user}`);
        }
    });
    
    return createdUsers;
}

// Function to implement Row Level Security (RLS) using views
function implementRowLevelSecurity() {
    print("\nImplementing Row Level Security using views...");
    
    // Create department-specific views
    const departments = ["ICU", "WARD", "OUTPATIENT", "EMERGENCY", "RECOVERY"];
    
    departments.forEach(dept => {
        const viewName = `v_patient_vitals_${dept.toLowerCase()}`;
        
        try {
            db.createView(
                viewName,
                "patient_vitals",
                [
                    {
                        $match: {
                            patient_department: dept,
                            data_classification: { $ne: "RESTRICTED" }
                        }
                    },
                    {
                        $project: {
                            patient_id: 1,
                            measurement_time: 1,
                            vital_type: 1,
                            vital_value: 1,
                            is_alert: 1,
                            patient_department: 1,
                            device_id: 1,
                            data_classification: 1,
                            confidence: 1
                        }
                    }
                ]
            );
            print(`  Created view: ${viewName}`);
        } catch (error) {
            print(`  Error creating view ${viewName}: ${error.message}`);
        }
    });
    
    // Create role-specific views
    const roleViews = [
        {
            viewName: "v_patient_vitals_public",
            pipeline: [
                {
                    $match: {
                        data_classification: "PUBLIC"
                    }
                },
                {
                    $project: {
                        _id: 0,
                        patient_id: 1,
                        measurement_time: 1,
                        vital_type: 1,
                        vital_value: 1,
                        patient_department: 1
                    }
                }
            ]
        },
        {
            viewName: "v_patient_vitals_internal",
            pipeline: [
                {
                    $match: {
                        data_classification: { $in: ["PUBLIC", "INTERNAL"] }
                    }
                }
            ]
        },
        {
            viewName: "v_recent_alerts",
            pipeline: [
                {
                    $match: {
                        is_alert: true,
                        measurement_time: {
                            $gte: new Date(Date.now() - 24 * 60 * 60 * 1000)
                        }
                    }
                },
                {
                    $sort: { measurement_time: -1 }
                },
                {
                    $project: {
                        patient_id: 1,
                        measurement_time: 1,
                        vital_type: 1,
                        vital_value: 1,
                        patient_department: 1,
                        device_id: 1
                    }
                }
            ]
        }
    ];
    
    roleViews.forEach(viewDef => {
        try {
            db.createView(
                viewDef.viewName,
                "patient_vitals",
                viewDef.pipeline
            );
            print(`  Created view: ${viewDef.viewName}`);
        } catch (error) {
            print(`  Error creating view ${viewDef.viewName}: ${error.message}`);
        }
    });
    
    return departments.length + roleViews.length;
}

// Function to create field-level encryption setup
function setupFieldLevelEncryption() {
    print("\nSetting up field-level encryption...");
    
    // Note: Field-level encryption requires MongoDB Enterprise 4.2+
    // This is a conceptual implementation
    
    const encryptionConfig = {
        keyVaultNamespace: "encryption.__keyVault",
        kmsProviders: {
            local: {
                key: BinData(0, "A" + "0".repeat(63)) // Example key
            }
        },
        schemaMap: {
            [`${DATABASE_NAME}.patient_vitals`]: {
                bsonType: "object",
                properties: {
                    patient_id: {
                        encrypt: {
                            bsonType: "string",
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [UUID("12345678-1234-1234-1234-123456789012")]
                        }
                    },
                    vital_value: {
                        encrypt: {
                            bsonType: "double",
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [UUID("12345678-1234-1234-1234-123456789012")]
                        }
                    },
                    metadata: {
                        bsonType: "object",
                        properties: {
                            sensitive_info: {
                                encrypt: {
                                    bsonType: "string",
                                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    
    print("  Field-level encryption configuration prepared.");
    print("  Note: This requires MongoDB Enterprise with Field Level Encryption enabled.");
    
    return encryptionConfig;
}

// Function to implement audit logging
function setupAuditLogging() {
    print("\nSetting up audit logging...");
    
    // Enable audit logging (requires appropriate permissions)
    try {
        // Configure audit log
        db.adminCommand({
            configureAuditing: {
                auditAuthorizationSuccess: true,
                auditAuthorizationFailure: true,
                filter: '{"atype": {"$in": ["authenticate", "createUser", "dropUser"]}}'
            }
        });
        
        // Set audit destination (file or syslog)
        db.adminCommand({
            setParameter: 1,
            auditDestination: "file",
            auditPath: "/var/log/mongodb/audit.log",
            auditFormat: "JSON"
        });
        
        print("  Audit logging configured.");
        
    } catch (error) {
        print(`  Note: Audit logging configuration requires appropriate privileges: ${error.message}`);
    }
    
    // Create audit logging function for application-level audit
    const auditFunction = `
        function logAuditEvent(eventType, userId, patientId, details) {
            db.audit_logs.insertOne({
                event_time: new Date(),
                event_type: eventType,
                user_id: userId,
                patient_id: patientId,
                details: details,
                ip_address: db.serverStatus().connections.current,
                application: "health_iot_system"
            });
            
            // Also log to system profile if enabled
            if (db.getProfilingStatus().was > 0) {
                print(\`[AUDIT] \${eventType}: \${userId} accessed patient \${patientId}\`);
            }
        }
    `;
    
    // Store audit function in system.js for reuse
    db.system.js.save({
        _id: "logAuditEvent",
        value: eval(`(${auditFunction})`)
    });
    
    print("  Application audit function created.");
    
    return true;
}

// Function to implement data masking
function implementDataMasking() {
    print("\nImplementing data masking...");
    
    // Create masked views for sensitive data
    const maskedViews = [
        {
            viewName: "v_patient_vitals_masked",
            pipeline: [
                {
                    $project: {
                        // Mask patient ID (show only last 4 characters)
                        patient_id: {
                            $concat: [
                                "*****",
                                { $substrCP: ["$patient_id", { $subtract: [{ $strLenCP: "$patient_id" }, 4] }, 4] }
                            ]
                        },
                        measurement_time: 1,
                        vital_type: 1,
                        // Round vital values for anonymity
                        vital_value: { $round: ["$vital_value", 0] },
                        is_alert: 1,
                        // Show only department category
                        patient_department: {
                            $switch: {
                                branches: [
                                    { case: { $eq: ["$patient_department", "ICU"] }, then: "CRITICAL_CARE" },
                                    { case: { $eq: ["$patient_department", "EMERGENCY"] }, then: "EMERGENCY_CARE" },
                                    { case: { $eq: ["$patient_department", "WARD"] }, then: "GENERAL_CARE" }
                                ],
                                default: "OTHER"
                            }
                        },
                        // Remove device ID
                        data_classification: 1,
                        confidence: { $round: ["$confidence", 1] }
                    }
                }
            ]
        },
        {
            viewName: "v_anonymous_stats",
            pipeline: [
                {
                    $group: {
                        _id: {
                            department: "$patient_department",
                            hour: { $hour: "$measurement_time" },
                            vital_type: "$vital_type"
                        },
                        avg_value: { $avg: "$vital_value" },
                        count: { $sum: 1 },
                        alert_count: {
                            $sum: { $cond: [{ $eq: ["$is_alert", true] }, 1, 0] }
                        }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        department: "$_id.department",
                        hour: "$_id.hour",
                        vital_type: "$_id.vital_type",
                        avg_value: { $round: ["$avg_value", 2] },
                        count: 1,
                        alert_percentage: {
                            $round: [
                                { $multiply: [{ $divide: ["$alert_count", "$count"] }, 100] },
                                2
                            ]
                        }
                    }
                },
                { $sort: { department: 1, hour: 1 } }
            ]
        }
    ];
    
    maskedViews.forEach(viewDef => {
        try {
            db.createView(
                viewDef.viewName,
                "patient_vitals",
                viewDef.pipeline
            );
            print(`  Created masked view: ${viewDef.viewName}`);
        } catch (error) {
            print(`  Error creating masked view ${viewDef.viewName}: ${error.message}`);
        }
    });
    
    return maskedViews.length;
}

// Function to implement time-based access control
function implementTimeBasedAccess() {
    print("\nImplementing time-based access control...");
    
    // Create a function to check access based on time
    const timeAccessFunction = `
        function checkTimeBasedAccess(userId, requiredRole) {
            const currentHour = new Date().getHours();
            const user = db.getUser(userId);
            
            // Define access schedules
            const accessSchedules = {
                "health_iot_reader": { start: 0, end: 24 }, // 24/7 access
                "health_iot_writer": { start: 6, end: 22 }, // 6 AM - 10 PM
                "health_iot_admin": { start: 8, end: 18 }, // 8 AM - 6 PM
                "health_iot_auditor": { start: 9, end: 17 } // 9 AM - 5 PM
            };
            
            // Check if user has the required role
            const hasRole = user.roles.some(role => role.role === requiredRole);
            
            if (!hasRole) {
                return { allowed: false, reason: "User does not have required role" };
            }
            
            // Check time-based access
            const schedule = accessSchedules[requiredRole];
            if (!schedule) {
                return { allowed: true, reason: "No time restrictions for this role" };
            }
            
            if (currentHour >= schedule.start && currentHour < schedule.end) {
                return { allowed: true, reason: "Within allowed time window" };
            } else {
                return { 
                    allowed: false, 
                    reason: \`Access only allowed between \${schedule.start}:00 and \${schedule.end}:00\`
                };
            }
        }
    `;
    
    // Store function in system.js
    db.system.js.save({
        _id: "checkTimeBasedAccess",
        value: eval(`(${timeAccessFunction})`)
    });
    
    print("  Time-based access control function created.");
    
    // Create view that uses time-based access
    try {
        db.createView(
            "v_time_restricted_vitals",
            "patient_vitals",
            [
                {
                    $addFields: {
                        access_check: {
                            $function: {
                                body: `function() {
                                    return checkTimeBasedAccess("$${ctx.userId}", "health_iot_reader");
                                }`,
                                args: [],
                                lang: "js"
                            }
                        }
                    }
                },
                {
                    $match: {
                        "access_check.allowed": true
                    }
                },
                {
                    $project: {
                        access_check: 0
                    }
                }
            ]
        );
        print("  Created time-restricted view.");
    } catch (error) {
        print(`  Note: Time-restricted view requires MongoDB 4.4+: ${error.message}`);
    }
    
    return true;
}

// Function to perform security audit
function performSecurityAudit() {
    print("\nPerforming security audit...");
    
    const auditResults = {
        timestamp: new Date(),
        database: DATABASE_NAME,
        findings: [],
        recommendations: [],
        score: 0,
        maxScore: 100
    };
    
    let currentScore = 0;
    
    // Check 1: Authentication enabled
    try {
        const authStatus = db.adminCommand({ getParameter: 1, authorization: 1 });
        if (authStatus.authorization === "enabled") {
            currentScore += 20;
            auditResults.findings.push({
                check: "Authentication",
                status: "PASS",
                details: "Authentication is enabled"
            });
        } else {
            auditResults.findings.push({
                check: "Authentication",
                status: "FAIL",
                details: "Authentication is disabled"
            });
            auditResults.recommendations.push("Enable authentication in MongoDB configuration");
        }
    } catch (error) {
        auditResults.findings.push({
            check: "Authentication",
            status: "ERROR",
            details: `Could not check authentication status: ${error.message}`
        });
    }
    
    // Check 2: Custom roles exist
    const customRoles = db.getRoles({ showBuiltinRoles: false });
    if (customRoles.length >= 5) {
        currentScore += 15;
        auditResults.findings.push({
            check: "Custom Roles",
            status: "PASS",
            details: `${customRoles.length} custom roles defined`
        });
    } else {
        auditResults.findings.push({
            check: "Custom Roles",
            status: "WARN",
            details: `Only ${customRoles.length} custom roles defined`
        });
        auditResults.recommendations.push("Define more granular custom roles for least privilege");
    }
    
    // Check 3: Application users exist
    const users = db.getUsers();
    const appUsers = users.filter(u => !u.user.includes("admin"));
    if (appUsers.length >= 3) {
        currentScore += 15;
        auditResults.findings.push({
            check: "Application Users",
            status: "PASS",
            details: `${appUsers.length} application users defined`
        });
    } else {
        auditResults.findings.push({
            check: "Application Users",
            status: "WARN",
            details: `Only ${appUsers.length} application users defined`
        });
        auditResults.recommendations.push("Create separate users for different application components");
    }
    
    // Check 4: Views for data masking
    const views = db.getCollectionInfos({ type: "view" });
    if (views.length >= 5) {
        currentScore += 15;
        auditResults.findings.push({
            check: "Data Masking Views",
            status: "PASS",
            details: `${views.length} views defined for data masking/RLS`
        });
    } else {
        auditResults.findings.push({
            check: "Data Masking Views",
            status: "WARN",
            details: `Only ${views.length} views defined`
        });
        auditResults.recommendations.push("Implement more views for row-level security and data masking");
    }
    
    // Check 5: Audit logging
    try {
        const auditConfig = db.adminCommand({ getParameter: 1, auditAuthorizationSuccess: 1 });
        if (auditConfig.auditAuthorizationSuccess) {
            currentScore += 20;
            auditResults.findings.push({
                check: "Audit Logging",
                status: "PASS",
                details: "Audit logging is enabled"
            });
        } else {
            auditResults.findings.push({
                check: "Audit Logging",
                status: "FAIL",
                details: "Audit logging is disabled"
            });
            auditResults.recommendations.push("Enable audit logging for security monitoring");
        }
    } catch (error) {
        auditResults.findings.push({
            check: "Audit Logging",
            status: "ERROR",
            details: `Could not check audit logging: ${error.message}`
        });
    }
    
    // Check 6: Encryption at rest
    try {
        const encryptionStatus = db.adminCommand({ getParameter: 1, enableEncryption: 1 });
        if (encryptionStatus.enableEncryption) {
            currentScore += 15;
            auditResults.findings.push({
                check: "Encryption at Rest",
                status: "PASS",
                details: "Encryption at rest is enabled"
            });
        } else {
            auditResults.findings.push({
                check: "Encryption at Rest",
                status: "WARN",
                details: "Encryption at rest is disabled"
            });
            auditResults.recommendations.push("Enable encryption at rest for sensitive health data");
        }
    } catch (error) {
        auditResults.findings.push({
            check: "Encryption at Rest",
            status: "INFO",
            details: "Encryption at rest check requires Enterprise MongoDB"
        });
    }
    
    // Calculate final score
    auditResults.score = currentScore;
    auditResults.grade = currentScore >= 80 ? "A" :
                        currentScore >= 70 ? "B" :
                        currentScore >= 60 ? "C" :
                        currentScore >= 50 ? "D" : "F";
    
    // Print audit results
    print("Security Audit Results:");
    print(`  Overall Score: ${currentScore}/${auditResults.maxScore} (${auditResults.grade})`);
    print("");
    
    auditResults.findings.forEach(finding => {
        const statusIcon = finding.status === "PASS" ? "✓" :
                          finding.status === "WARN" ? "⚠" :
                          finding.status === "FAIL" ? "✗" : "?";
        print(`  ${statusIcon} ${finding.check}: ${finding.details}`);
    });
    
    if (auditResults.recommendations.length > 0) {
        print("\nRecommendations:");
        auditResults.recommendations.forEach((rec, index) => {
            print(`  ${index + 1}. ${rec}`);
        });
    }
    
    // Save audit results
    db.security_audit_results.insertOne(auditResults);
    
    return auditResults;
}

// Function to generate security report
function generateSecurityReport() {
    print("\nGenerating security configuration report...");
    
    const report = {
        timestamp: new Date(),
        database: DATABASE_NAME,
        security_configuration: {}
    };
    
    // Gather role information
    report.security_configuration.roles = db.getRoles({ showBuiltinRoles: false }).map(role => ({
        name: role.role,
        privileges: role.privileges,
        inherited_roles: role.roles
    }));
    
    // Gather user information (without passwords)
    report.security_configuration.users = db.getUsers().map(user => ({
        username: user.user,
        roles: user.roles,
        custom_data: user.customData
    }));
    
    // Gather view information for RLS
    report.security_configuration.views = db.getCollectionInfos({ type: "view" })
        .filter(view => view.name.startsWith('v_'))
        .map(view => ({
            name: view.name,
            source: view.options.viewOn,
            pipeline: view.options.pipeline
        }));
    
    // Gather encryption information
    try {
        const encryptionParams = db.adminCommand({
            getParameter: "*"
        });
        report.security_configuration.encryption = {
            at_rest: encryptionParams.enableEncryption || false,
            network: encryptionParams.tlsMode || "disabled"
        };
    } catch (error) {
        report.security_configuration.encryption = {
            error: "Could not retrieve encryption configuration"
        };
    }
    
    // Gather audit configuration
    try {
        const auditParams = db.adminCommand({
            getParameter: "*"
        });
        report.security_configuration.audit = {
            enabled: auditParams.auditAuthorizationSuccess || false,
            destination: auditParams.auditDestination || "none"
        };
    } catch (error) {
        report.security_configuration.audit = {
            error: "Could not retrieve audit configuration"
        };
    }
    
    // Print summary
    print("Security Configuration Summary:");
    print(`  Custom Roles: ${report.security_configuration.roles.length}`);
    print(`  Application Users: ${report.security_configuration.users.length}`);
    print(`  Security Views: ${report.security_configuration.views.length}`);
    
    if (report.security_configuration.encryption.at_rest) {
        print(`  Encryption at Rest: Enabled`);
    }
    
    if (report.security_configuration.audit.enabled) {
        print(`  Audit Logging: Enabled`);
    }
    
    // Save report
    db.security_reports.insertOne(report);
    
    print("\nDetailed report saved to security_reports collection.");
    
    return report;
}

// Function to test security controls
function testSecurityControls() {
    print("\nTesting security controls...");
    
    const tests = [];
    
    // Test 1: Reader role can only read
    tests.push({
        name: "Reader Role - Read Only",
        user: "app_reader",
        password: "ReaderAppPass123!",
        test: function(dbConn) {
            // Should succeed
            const findResult = dbConn.patient_vitals.find().limit(1).toArray();
            if (findResult.length > 0) {
                return { passed: true, message: "Can read data" };
            }
            return { passed: false, message: "Cannot read data" };
        }
    });
    
    // Test 2: Reader role cannot write
    tests.push({
        name: "Reader Role - No Write",
        user: "app_reader",
        password: "ReaderAppPass123!",
        test: function(dbConn) {
            try {
                dbConn.patient_vitals.insertOne({
                    patient_id: "TEST_SECURITY",
                    measurement_time: new Date(),
                    vital_type: "test",
                    vital_value: 100
                });
                return { passed: false, message: "Should not be able to insert" };
            } catch (error) {
                if (error.code === 13 || error.message.includes("not authorized")) {
                    return { passed: true, message: "Correctly prevented from writing" };
                }
                return { passed: false, message: `Unexpected error: ${error.message}` };
            }
        }
    });
    
    // Test 3: Writer role can write
    tests.push({
        name: "Writer Role - Can Write",
        user: "app_writer",
        password: "WriterAppPass123!",
        test: function(dbConn) {
            try {
                const result = dbConn.patient_vitals.insertOne({
                    patient_id: "TEST_SECURITY_WRITE",
                    measurement_time: new Date(),
                    vital_type: "test",
                    vital_value: 100,
                    is_alert: false,
                    metadata: { test: true }
                });
                
                // Clean up
                dbConn.patient_vitals.deleteOne({ _id: result.insertedId });
                
                return { passed: true, message: "Can write data" };
            } catch (error) {
                return { passed: false, message: `Cannot write: ${error.message}` };
            }
        }
    });
    
    // Test 4: Masked view obscures sensitive data
    tests.push({
        name: "Data Masking View",
        user: "app_reader",
        password: "ReaderAppPass123!",
        test: function(dbConn) {
            try {
                const result = dbConn.v_patient_vitals_masked.find().limit(1).toArray();
                if (result.length > 0) {
                    const doc = result[0];
                    // Check if patient_id is masked
                    if (doc.patient_id && doc.patient_id.startsWith("*****")) {
                        return { passed: true, message: "Data is properly masked" };
                    }
                }
                return { passed: false, message: "Data masking not working" };
            } catch (error) {
                return { passed: false, message: `Error: ${error.message}` };
            }
        }
    });
    
    // Run tests
    const testResults = [];
    
    tests.forEach(test => {
        print(`  Testing: ${test.name}...`);
        
        try {
            // Create connection with test user
            const testConn = new Mongo(`localhost:27017/${DATABASE_NAME}`);
            const testDb = testConn.getDB(DATABASE_NAME);
            testDb.auth(test.user, test.password);
            
            const result = test.test(testDb);
            testResults.push({
                test: test.name,
                user: test.user,
                ...result
            });
            
            print(`    ${result.passed ? '✓' : '✗'} ${result.message}`);
            
            // Close connection
            testConn.close();
            
        } catch (error) {
            testResults.push({
                test: test.name,
                user: test.user,
                passed: false,
                message: `Connection error: ${error.message}`
            });
            print(`    ✗ Connection error: ${error.message}`);
        }
    });
    
    // Calculate pass rate
    const passedTests = testResults.filter(t => t.passed).length;
    const passRate = (passedTests / testResults.length * 100).toFixed(1);
    
    print(`\n  Security Tests Pass Rate: ${passRate}% (${passedTests}/${testResults.length})`);
    
    // Save test results
    db.security_test_results.insertOne({
        timestamp: new Date(),
        tests: testResults,
        summary: {
            total_tests: testResults.length,
            passed_tests: passedTests,
            pass_rate: passRate
        }
    });
    
    return testResults;
}

// Main function to setup complete security configuration
function setupCompleteSecurity() {
    print("=".repeat(60));
    print("MONGODB SECURITY CONFIGURATION");
    print("=".repeat(60));
    print(`Database: ${DATABASE_NAME}`);
    print(`Timestamp: ${new Date()}`);
    print("");
    
    const results = {};
    
    // Step 1: Create custom roles
    print("STEP 1: CREATING CUSTOM ROLES");
    print("-".repeat(40));
    results.roles = createCustomRoles();
    print("");
    
    // Step 2: Create application users
    print("STEP 2: CREATING APPLICATION USERS");
    print("-".repeat(40));
    results.users = createApplicationUsers();
    print("");
    
    // Step 3: Implement Row Level Security
    print("STEP 3: IMPLEMENTING ROW LEVEL SECURITY");
    print("-".repeat(40));
    results.rlsViews = implementRowLevelSecurity();
    print("");
    
    // Step 4: Implement data masking
    print("STEP 4: IMPLEMENTING DATA MASKING");
    print("-".repeat(40));
    results.maskedViews = implementDataMasking();
    print("");
    
    // Step 5: Setup audit logging
    print("STEP 5: SETTING UP AUDIT LOGGING");
    print("-".repeat(40));
    results.auditLogging = setupAuditLogging();
    print("");
    
    // Step 6: Implement time-based access
    print("STEP 6: IMPLEMENTING TIME-BASED ACCESS CONTROL");
    print("-".repeat(40));
    results.timeBasedAccess = implementTimeBasedAccess();
    print("");
    
    // Step 7: Perform security audit
    print("STEP 7: PERFORMING SECURITY AUDIT");
    print("-".repeat(40));
    results.audit = performSecurityAudit();
    print("");
    
    // Step 8: Test security controls
    print("STEP 8: TESTING SECURITY CONTROLS");
    print("-".repeat(40));
    results.securityTests = testSecurityControls();
    print("");
    
    // Step 9: Generate security report
    print("STEP 9: GENERATING SECURITY REPORT");
    print("-".repeat(40));
    results.report = generateSecurityReport();
    print("");
    
    // Print summary
    print("=".repeat(60));
    print("SECURITY CONFIGURATION SUMMARY");
    print("=".repeat(60));
    
    print(`Custom Roles Created: ${results.roles.length}`);
    print(`Application Users Created: ${results.users.length}`);
    print(`Security Views Created: ${results.rlsViews + results.maskedViews}`);
    print(`Security Audit Score: ${results.audit.score}/${results.audit.maxScore} (${results.audit.grade})`);
    
    const testSummary = results.securityTests.reduce((acc, test) => {
        acc.total++;
        if (test.passed) acc.passed++;
        return acc;
    }, { total: 0, passed: 0 });
    
    print(`Security Tests Passed: ${testSummary.passed}/${testSummary.total}`);
    
    print("\nSecurity configuration completed successfully!");
    print("=".repeat(60));
    
    return results;
}

// Execute main function
setupCompleteSecurity();