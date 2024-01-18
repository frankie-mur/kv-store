package main

import (
	"database/sql"
	"fmt"
)

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

func NewPostgresTransactionLogger(params PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		params.host, params.user, params.password, params.dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	logger := PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, err
	}

	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, err
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"

	var result string

	stmt := fmt.Sprintf(`SELECT to_regclass('public.%s');`, table)

	rows, err := l.db.Query(stmt)
	defer rows.Close()
	if err != nil {
		return false, err
	}

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return result == table, rows.Err()
}

func (l *PostgresTransactionLogger) createTable() error {
	stmt := `CREATE TABLE transactions (
		sequence SERIAL PRIMARY KEY,
		event_type SMALLINT,
		key TEXT,
		value TEXT
	);`

	_, err := l.db.Exec(stmt)
	if err != nil {
		return err
	}

	return nil
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) LastSequence() uint64 {
	return 0
}
