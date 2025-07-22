IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dataset')
BEGIN
    CREATE TABLE dataset (
        id INTEGER NOT NULL IDENTITY(1,1),
        uri VARCHAR(3000) NOT NULL,
        extra NVARCHAR(max) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        is_orphaned BIT NOT NULL DEFAULT '0',
        CONSTRAINT dataset_pkey PRIMARY KEY (id)
    );
END
GO

-- dataset_alias table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dataset_alias')
BEGIN
    CREATE TABLE dataset_alias (
        id INTEGER NOT NULL IDENTITY(1,1),
        name VARCHAR(3000) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT dataset_alias_pkey PRIMARY KEY (id),
        CONSTRAINT dataset_alias_name_key UNIQUE (name)
    );
END
GO

-- dataset_alias_dataset table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dataset_alias_dataset')
BEGIN
    CREATE TABLE dataset_alias_dataset (
        alias_id INTEGER NOT NULL,
        dataset_id INTEGER NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT dataset_alias_dataset_pkey PRIMARY KEY (alias_id, dataset_id),
        CONSTRAINT ds_dsa_alias_id FOREIGN KEY(alias_id) REFERENCES dataset_alias(id) ON DELETE NO ACTION,
        CONSTRAINT ds_dsa_dataset_id FOREIGN KEY(dataset_id) REFERENCES dataset(id) ON DELETE NO ACTION
    );
END
GO

-- dataset_event table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dataset_event')
BEGIN
    CREATE TABLE dataset_event (
        id INTEGER NOT NULL IDENTITY(1,1),
        dataset_id INTEGER NOT NULL,
        extra NVARCHAR(max) NULL,
        source_dag_id VARCHAR(250) NULL,
        source_task_id VARCHAR(250) NULL,
        source_run_id VARCHAR(250) NULL,
        source_map_index INTEGER NOT NULL DEFAULT -1,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT dataset_event_pkey PRIMARY KEY (id),
        CONSTRAINT dataset_event_dataset_id_fkey FOREIGN KEY(dataset_id) REFERENCES dataset (id)
    );
END
GO

-- dataset_alias_dataset_event table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dataset_alias_dataset_event')
BEGIN
    CREATE TABLE dataset_alias_dataset_event (
        alias_id INTEGER NOT NULL,
        event_id INTEGER NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT dataset_alias_dataset_event_pkey PRIMARY KEY (alias_id, event_id),
        CONSTRAINT dss_de_alias_id FOREIGN KEY(alias_id) REFERENCES dataset_alias(id) ON DELETE NO ACTION,
        CONSTRAINT dss_de_event_id FOREIGN KEY(event_id) REFERENCES dataset_event(id) ON DELETE NO ACTION
    );
END
GO

-- log table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'log')
BEGIN
    CREATE TABLE log (
        id INTEGER NOT NULL IDENTITY(1,1),
        dttm DATETIME2 NULL,
        dag_id VARCHAR(250) NULL,
        task_id VARCHAR(250) NULL,
        map_index INTEGER NULL,
        event VARCHAR(60) NULL,
        execution_date DATETIME2 NULL,
        run_id VARCHAR(250) NULL,
        owner VARCHAR(500) NULL,
        owner_display_name VARCHAR(500) NULL,
        extra NVARCHAR(max) NULL,
        try_number INTEGER NULL,
        CONSTRAINT log_pkey PRIMARY KEY (id)
    );
END
GO

-- job table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'job')
BEGIN
    CREATE TABLE job (
        id INTEGER NOT NULL IDENTITY(1,1),
        dag_id VARCHAR(250) NULL,
        state VARCHAR(20) NULL,
        job_type VARCHAR(30) NULL,
        start_date DATETIME2 NULL,
        end_date DATETIME2 NULL,
        latest_heartbeat DATETIME2 NULL,
        executor_class VARCHAR(500) NULL,
        hostname VARCHAR(500) NULL,
        unixname VARCHAR(1000) NULL,
        CONSTRAINT job_pkey PRIMARY KEY (id)
    );
END
GO

-- task_instance table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'task_instance')
BEGIN
    CREATE TABLE task_instance (
        task_id VARCHAR(250) NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        run_id VARCHAR(250) NOT NULL,
        map_index INTEGER NOT NULL DEFAULT -1,
        start_date DATETIME2 NULL,
        end_date DATETIME2 NULL,
        execution_date DATETIME2 NULL,
        duration FLOAT NULL,
        state VARCHAR(20) NULL,
        try_number INTEGER NULL,
        max_tries INTEGER NULL,
        hostname VARCHAR(1000) NULL,
        unixname VARCHAR(1000) NULL,
        pool VARCHAR(256) NOT NULL,
        pool_slots INTEGER NOT NULL,
        queue VARCHAR(256) NULL,
        priority_weight INTEGER NULL,
        operator VARCHAR(1000) NULL,
        queued_dttm DATETIME2 NULL,
        queued_by_job_id INTEGER NULL,
        pid INTEGER NULL,
        executor VARCHAR(1000) NULL,
        executor_config NVARCHAR(max) NULL,
        updated_at DATETIME2 NULL,
        external_executor_id VARCHAR(250) NULL,
        trigger_id BIGINT NULL,
        trigger_timeout DATETIME2 NULL,
        next_method VARCHAR(1000) NULL,
        next_kwargs NVARCHAR(max) NULL,
        task_display_name VARCHAR(2000) NULL,
        CONSTRAINT task_instance_pkey PRIMARY KEY (dag_id, task_id, run_id, map_index)
    );
END
GO

-- dag_run table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dag_run')
BEGIN
    CREATE TABLE dag_run (
        id INTEGER NOT NULL IDENTITY(1,1),
        dag_id VARCHAR(250) NOT NULL,
        queued_at DATETIME2 NULL,
        execution_date DATETIME2 NOT NULL,
        start_date DATETIME2 NULL,
        end_date DATETIME2 NULL,
        state VARCHAR(50) NULL,
        run_id VARCHAR(250) NOT NULL,
        creating_job_id INTEGER NULL,
        external_trigger BIT NOT NULL DEFAULT '1',
        run_type VARCHAR(50) NOT NULL,
        conf NVARCHAR(max) NULL,
        data_interval_start DATETIME2 NULL,
        data_interval_end DATETIME2 NULL,
        last_scheduling_decision DATETIME2 NULL,
        run_after DATETIME2 NULL,
        CONSTRAINT dag_run_dag_id_execution_date_key UNIQUE (dag_id, execution_date),
        CONSTRAINT dag_run_dag_id_run_id_key UNIQUE (dag_id, run_id),
        CONSTRAINT dag_run_pkey PRIMARY KEY (id)
    );
END
GO

-- sla_miss table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sla_miss')
BEGIN
    CREATE TABLE sla_miss (
        task_id VARCHAR(250) NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        execution_date DATETIME2 NOT NULL,
        email_sent BIT NULL,
        timestamp DATETIME2 NULL,
        description NVARCHAR(max) NULL,
        notification_sent BIT NULL,
        CONSTRAINT sla_miss_pkey PRIMARY KEY (task_id, dag_id, execution_date)
    );
END
GO

-- task_fail table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'task_fail')
BEGIN
    CREATE TABLE task_fail (
        id INTEGER NOT NULL IDENTITY(1,1),
        task_id VARCHAR(250) NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        run_id VARCHAR(250) NOT NULL,
        map_index INTEGER NOT NULL DEFAULT -1,
        start_date DATETIME2 NULL,
        end_date DATETIME2 NULL,
        execution_date DATETIME2 NOT NULL,
        duration FLOAT NULL,
        CONSTRAINT task_fail_pkey PRIMARY KEY (id)
    );
END
GO

-- task_reschedule table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'task_reschedule')
BEGIN
    CREATE TABLE task_reschedule (
        id INTEGER NOT NULL IDENTITY(1,1),
        task_id VARCHAR(250) NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        run_id VARCHAR(250) NOT NULL,
        map_index INTEGER NOT NULL DEFAULT -1,
        start_date DATETIME2 NULL,
        end_date DATETIME2 NULL,
        reschedule_date DATETIME2 NULL,
        duration FLOAT NULL,
        CONSTRAINT task_reschedule_pkey PRIMARY KEY (id)
    );
END
GO

-- xcom table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'xcom')
BEGIN
    CREATE TABLE xcom (
        id INTEGER NOT NULL IDENTITY(1,1),
        dag_id VARCHAR(250) NOT NULL,
        task_id VARCHAR(250) NOT NULL,
        run_id VARCHAR(250) NOT NULL,
        map_index INTEGER NOT NULL DEFAULT -1,
        key VARCHAR(512) NOT NULL,
        value VARBINARY(max) NULL,
        timestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT xcom_dag_task_run_map_key UNIQUE (dag_id, task_id, run_id, map_index, key),
        CONSTRAINT xcom_pkey PRIMARY KEY (id)
    );
END
GO

-- dag table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dag')
BEGIN
    CREATE TABLE dag (
        dag_id VARCHAR(250) NOT NULL,
        root_dag_id VARCHAR(250) NULL,
        is_paused BIT NOT NULL DEFAULT '0',
        is_subdag BIT NULL,
        is_active BIT NULL,
        last_parsed_time DATETIME2 NULL,
        last_pickled DATETIME2 NULL,
        last_expired DATETIME2 NULL,
        scheduler_lock BIT NULL,
        pickle_id INTEGER NULL,
        fileloc VARCHAR(2000) NULL,
        processor_subdir VARCHAR(2000) NULL,
        owners VARCHAR(2000) NULL,
        description NVARCHAR(max) NULL,
        default_view VARCHAR(25) NULL,
        schedule_interval NVARCHAR(max) NULL,
        timetable_description VARCHAR(1000) NULL,
        max_active_tasks INTEGER NOT NULL DEFAULT 16,
        max_active_runs INTEGER NULL,
        max_consecutive_failed_dag_runs INTEGER NULL,
        has_task_concurrency_limits BIT NOT NULL DEFAULT '1',
        has_import_errors BIT NULL,
        next_dagrun DATETIME2 NULL,
        next_dagrun_data_interval_start DATETIME2 NULL,
        next_dagrun_data_interval_end DATETIME2 NULL,
        next_dagrun_create_after DATETIME2 NULL,
        CONSTRAINT dag_pkey PRIMARY KEY (dag_id)
    );
END
GO

-- variable table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'variable')
BEGIN
    CREATE TABLE variable (
        id INTEGER NOT NULL IDENTITY(1,1),
        key VARCHAR(250) NOT NULL,
        val NVARCHAR(max) NULL,
        updated_at DATETIME2 NULL,
        description NVARCHAR(max) NULL,
        CONSTRAINT variable_key_key UNIQUE (key),
        CONSTRAINT variable_pkey PRIMARY KEY (id)
    );
END
GO

-- connection table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'connection')
BEGIN
    CREATE TABLE connection (
        id INTEGER NOT NULL IDENTITY(1,1),
        conn_id VARCHAR(250) NOT NULL,
        conn_type VARCHAR(500) NOT NULL,
        description NVARCHAR(max) NULL,
        host VARCHAR(500) NULL,
        schema VARCHAR(500) NULL,
        login VARCHAR(500) NULL,
        password VARCHAR(5000) NULL,
        port INTEGER NULL,
        extra NVARCHAR(max) NULL,
        updated_at DATETIME2 NULL,
        CONSTRAINT connection_conn_id_key UNIQUE (conn_id),
        CONSTRAINT connection_pkey PRIMARY KEY (id)
    );
END
GO

-- slot_pool table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'slot_pool')
BEGIN
    CREATE TABLE slot_pool (
        id INTEGER NOT NULL IDENTITY(1,1),
        pool VARCHAR(256) NOT NULL,
        slots INTEGER NULL,
        description NVARCHAR(max) NULL,
        updated_at DATETIME2 NULL,
        CONSTRAINT slot_pool_pool_key UNIQUE (pool),
        CONSTRAINT slot_pool_pkey PRIMARY KEY (id)
    );
END
GO

-- dag_schedule_dataset_reference table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dag_schedule_dataset_reference')
BEGIN
    CREATE TABLE dag_schedule_dataset_reference (
        dataset_id INTEGER NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT dsdr_pkey PRIMARY KEY (dataset_id, dag_id),
        CONSTRAINT dsdr_dataset_fkey FOREIGN KEY(dataset_id) REFERENCES dataset (id) ON DELETE CASCADE,
        CONSTRAINT dsdr_dag_id_fkey FOREIGN KEY(dag_id) REFERENCES dag (dag_id) ON DELETE CASCADE
    );
END
GO

-- task_instance_note table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'task_instance_note')
BEGIN
    CREATE TABLE task_instance_note (
        task_id VARCHAR(250) NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        run_id VARCHAR(250) NOT NULL,
        map_index INTEGER NOT NULL,
        content NVARCHAR(max) NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        created_by VARCHAR(250) NULL,
        updated_by VARCHAR(250) NULL,
        CONSTRAINT task_instance_note_pkey PRIMARY KEY (dag_id, task_id, run_id, map_index),
        CONSTRAINT task_instance_note_fkey FOREIGN KEY(dag_id, task_id, run_id, map_index) REFERENCES task_instance (dag_id, task_id, run_id, map_index) ON DELETE CASCADE
    );
END
GO

-- task_outlet_dataset_reference table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'task_outlet_dataset_reference')
BEGIN
    CREATE TABLE task_outlet_dataset_reference (
        dataset_id INTEGER NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        task_id VARCHAR(250) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        CONSTRAINT todr_pkey PRIMARY KEY (dataset_id, dag_id, task_id),
        CONSTRAINT todr_dataset_fkey FOREIGN KEY(dataset_id) REFERENCES dataset (id) ON DELETE CASCADE,
        CONSTRAINT todr_dag_id_fkey FOREIGN KEY(dag_id) REFERENCES dag (dag_id) ON DELETE CASCADE
    );
END
GO

-- task_instance_history table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'task_instance_history')
BEGIN
    CREATE TABLE task_instance_history (
        id INTEGER NOT NULL IDENTITY(1,1),
        task_id VARCHAR(250) NOT NULL,
        dag_id VARCHAR(250) NOT NULL,
        run_id VARCHAR(250) NOT NULL,
        map_index INTEGER NOT NULL DEFAULT -1,
        try_number INTEGER NOT NULL,
        start_date DATETIME2 NULL DEFAULT GETDATE(),
        end_date DATETIME2 NULL DEFAULT GETDATE(),
        duration FLOAT NULL,
        state VARCHAR(20) NULL,
        max_tries INTEGER NULL DEFAULT -1,
        hostname VARCHAR(1000) NULL,
        unixname VARCHAR(1000) NULL,
        job_id INTEGER NULL,
        pool VARCHAR(256) NOT NULL,
        pool_slots INTEGER NOT NULL,
        queue VARCHAR(256) NULL,
        priority_weight INTEGER NULL,
        operator VARCHAR(1000) NULL,
        custom_operator_name VARCHAR(1000) NULL,
        queued_dttm DATETIME2 NULL DEFAULT GETDATE(),
        queued_by_job_id INTEGER NULL,
        pid INTEGER NULL,
        executor VARCHAR(1000) NULL,
        executor_config VARBINARY(max) NULL,
        updated_at DATETIME2 NULL DEFAULT GETDATE(),
        rendered_map_index VARCHAR(250) NULL,
        external_executor_id VARCHAR(250) NULL,
        trigger_id INTEGER NULL,
        trigger_timeout DATETIME2 NULL,
        next_method VARCHAR(1000) NULL,
        next_kwargs NVARCHAR(max) NULL,
        task_display_name VARCHAR(2000) NULL,
        CONSTRAINT task_instance_history_pkey PRIMARY KEY (id),
        CONSTRAINT task_instance_history_ti_fkey FOREIGN KEY(dag_id, task_id, run_id, map_index) REFERENCES task_instance (dag_id, task_id, run_id, map_index) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT task_instance_history_dtrt_uq UNIQUE (dag_id, task_id, run_id, map_index, try_number)
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dag_schedule_dataset_alias_reference')
BEGIN
    CREATE TABLE dag_schedule_dataset_alias_reference (
        alias_id INTEGER NOT NULL,
        dag_id VARCHAR(250) NOT NULL, 
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(), 
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(), 
        CONSTRAINT dsdar_pkey PRIMARY KEY (alias_id, dag_id), 
        CONSTRAINT dsdar_dataset_alias_fkey FOREIGN KEY(alias_id) REFERENCES dataset_alias (id) ON DELETE CASCADE, 
        CONSTRAINT dsdar_dag_fkey FOREIGN KEY(dag_id) REFERENCES dag (dag_id) ON DELETE CASCADE
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dag_run_note')
BEGIN
    CREATE TABLE dag_run_note (
        user_id INTEGER NULL, 
        dag_run_id INTEGER NOT NULL, 
        content VARCHAR(1000) NULL, 
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(), 
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(), 
        CONSTRAINT dag_run_note_pkey PRIMARY KEY (dag_run_id), 
        CONSTRAINT dag_run_note_dr_fkey FOREIGN KEY(dag_run_id) REFERENCES dag_run (id) ON DELETE CASCADE, 
        CONSTRAINT dag_run_note_user_fkey FOREIGN KEY(user_id) REFERENCES ab_user (id)
    );
END
GO