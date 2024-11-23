-- Criação da tabela tables
CREATE TABLE tables (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    requires_approval BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(255) NOT NULL,
    last_modified_by VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE table_execution (
    id BINARY(16) NOT NULL PRIMARY KEY, 
    table_id INT NOT NULL,
    date_time DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    source VARCHAR(255) NOT NULL,
    CONSTRAINT fk_table_execution_table FOREIGN KEY (table_id) REFERENCES tables (id) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da tabela partitions
CREATE TABLE partitions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    is_required BOOLEAN NOT NULL,
    FOREIGN KEY (table_id) REFERENCES tables(id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da tabela dependencies
CREATE TABLE dependencies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    dependency_id INT NOT NULL,
    is_required BOOLEAN NOT NULL,
    FOREIGN KEY (table_id) REFERENCES tables(id),
    FOREIGN KEY (dependency_id) REFERENCES tables(id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da tabela approval_status
CREATE TABLE approval_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    status ENUM('PENDING', 'APPROVED', 'REJECTED') DEFAULT 'PENDING',
    requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    reviewed_at DATETIME DEFAULT NULL,
    approver_name VARCHAR(255),
    FOREIGN KEY (table_id) REFERENCES tables(id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da tabela task_executor
CREATE TABLE task_executor (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alias VARCHAR(255) NOT NULL,
    description TEXT
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da tabela task_table
CREATE TABLE task_table (
    table_id INT NOT NULL,
    task_executor_id INT NOT NULL,
    alias VARCHAR(255) NOT NULL,
    params JSON,
    PRIMARY KEY (table_id, task_executor_id, alias),
    FOREIGN KEY (table_id) REFERENCES tables(id),
    FOREIGN KEY (task_executor_id) REFERENCES task_executor(id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da tabela process_status
CREATE TABLE process_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    partition_set JSON NOT NULL,
    status ENUM('IDLE', 'RUNNING', 'EXECUTED', 'FAILED') NOT NULL,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    execution_start_time DATETIME DEFAULT NULL,
    execution_end_time DATETIME DEFAULT NULL,
    execution_logs TEXT,
    FOREIGN KEY (table_id) REFERENCES tables(id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE table_partition_exec (
    table_id INT NOT NULL,
    partition_id INT NOT NULL,
    value VARCHAR(255) NOT NULL,
    execution_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    execution_id BINARY(16), -- FK para a tabela table_execution
    deletion_date TIMESTAMP NULL,
    deleted_by_user VARCHAR(255) NULL,
    PRIMARY KEY (table_id, partition_id, value),
    CONSTRAINT fk_table FOREIGN KEY (table_id) REFERENCES tables (id) ON DELETE CASCADE,
    CONSTRAINT fk_partition FOREIGN KEY (partition_id) REFERENCES partitions (id) ON DELETE CASCADE,
    CONSTRAINT fk_execution FOREIGN KEY (execution_id) REFERENCES table_execution (id) ON DELETE SET NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Criação da visão table_process_view
CREATE OR REPLACE VIEW table_process_view AS
SELECT
    t.name AS table_name,
    t.description AS table_description,
    p.partition_set AS partition_set,
    ps.status AS process_status,
    a.status AS approval_status,
    te.alias AS approver_name,
    ps.execution_start_time AS execution_time,
    ps.execution_end_time AS completion_time,
    ps.execution_logs AS execution_logs,
    a.reviewed_at AS approval_reviewed_at,
    e.id AS execution_id, -- Adiciona o ID da execução
    e.date_time AS execution_date_time,
    e.source AS execution_source
FROM
    tables t
LEFT JOIN
    process_status ps ON t.id = ps.table_id
LEFT JOIN
    (SELECT table_id, JSON_ARRAYAGG(JSON_OBJECT('name', name, 'type', type, 'is_required', is_required)) AS partition_set
     FROM partitions
     GROUP BY table_id) p ON t.id = p.table_id
LEFT JOIN
    approval_status a ON t.id = a.table_id
LEFT JOIN
    task_table tt ON t.id = tt.table_id
LEFT JOIN
    task_executor te ON tt.task_executor_id = te.id
LEFT JOIN
    table_execution e ON t.id = e.table_id -- Associa a execução
ORDER BY
    t.name, e.date_time DESC; -- Ordena por nome da tabela e data da execução
