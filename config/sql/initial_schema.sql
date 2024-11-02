-- Criação da tabela TaskExecutor
CREATE TABLE TaskExecutor (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alias VARCHAR(255) NOT NULL,
    description TEXT
);

-- Criação da tabela Tables
CREATE TABLE Tables (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    requires_approval BOOLEAN DEFAULT FALSE,
    approver_name VARCHAR(255),
    execution_start_time DATETIME DEFAULT NULL,
    execution_end_time DATETIME DEFAULT NULL,
    execution_logs TEXT,
    created_by VARCHAR(255) NOT NULL,             
    last_modified_by VARCHAR(255),                
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, 
    last_modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Criação da tabela TaskTable para relacionar Tables e TaskExecutor
CREATE TABLE TaskTable (
    table_id INT NOT NULL,
    task_executor_id INT NOT NULL,
    alias VARCHAR(255) NOT NULL,
    params JSON,
    PRIMARY KEY (table_id, task_executor_id, alias),
    FOREIGN KEY (table_id) REFERENCES Tables(id),
    FOREIGN KEY (task_executor_id) REFERENCES TaskExecutor(id)
);

-- Criação da tabela Partitions
CREATE TABLE Partitions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    is_required BOOLEAN NOT NULL,
    FOREIGN KEY (table_id) REFERENCES Tables(id)
);

-- Criação da tabela Dependencies
CREATE TABLE Dependencies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    dependency_id INT NOT NULL,
    FOREIGN KEY (table_id) REFERENCES Tables(id),
    FOREIGN KEY (dependency_id) REFERENCES Tables(id)
);

-- Criação da tabela ProcessStatus
CREATE TABLE ProcessStatus (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    partition_set JSON NOT NULL,
    status ENUM('IDLE', 'RUNNING', 'EXECUTED', 'FAILED') NOT NULL,
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    execution_start_time DATETIME DEFAULT NULL,
    execution_end_time DATETIME DEFAULT NULL,
    execution_logs TEXT,
    FOREIGN KEY (table_id) REFERENCES Tables(id)
);

-- Criação da tabela ApprovalStatus
CREATE TABLE ApprovalStatus (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_id INT NOT NULL,
    status ENUM('PENDING', 'APPROVED', 'REJECTED') DEFAULT 'PENDING',
    requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    reviewed_at DATETIME DEFAULT NULL,
    approver_name VARCHAR(255),
    FOREIGN KEY (table_id) REFERENCES Tables(id)
);

-- Criação da visão TableProcessView
CREATE VIEW TableProcessView AS
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
    a.reviewed_at AS approval_reviewed_at
FROM
    Tables t
JOIN
    ProcessStatus ps ON t.id = ps.table_id
LEFT JOIN
    (SELECT table_id, JSON_ARRAYAGG(JSON_OBJECT('name', name, 'type', type, 'is_required', is_required)) AS partition_set
     FROM Partitions
     GROUP BY table_id) p ON t.id = p.table_id
LEFT JOIN
    ApprovalStatus a ON t.id = a.table_id
LEFT JOIN
    TaskTable tt ON t.id = tt.table_id
LEFT JOIN
    TaskExecutor te ON tt.task_executor_id = te.id
ORDER BY
    t.name, ps.last_updated;
