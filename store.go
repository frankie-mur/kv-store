package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
)

type LockableStore struct {
	m map[string]string
	sync.RWMutex
}

var store = LockableStore{
	m: make(map[string]string),
}

var transact TransactionLogger

var ErrNoSuchKey = errors.New("key doesn not exist")

func putHandler(w http.ResponseWriter, r *http.Request) {
	//Extract the body from the request
	key := mux.Vars(r)["key"]
	val, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	//Store the value
	err = Put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusCreated)

}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	val, err := Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
	}
	w.Write([]byte(val))
}

func Put(key, value string) error {
	store.Lock()
	defer store.Unlock()

	store.m[key] = value

	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()
	val, ok := store.m[key]
	if !ok {
		return "", ErrNoSuchKey
	}

	return val, nil
}

func Delete(key string) error {
	store.Lock()
	defer store.Unlock()

	delete(store.m, key)

	return nil
}

func initializeTransactionLog() error {
	var err error
	pgHost := os.Getenv("PG_HOST")
	dbName := os.Getenv("DB_NAME")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")

	transact, err = NewPostgresTransactionLogger(
		PostgresDBParams{
			host:     pgHost,
			dbName:   dbName,
			user:     pgUser,
			password: pgPassword,
		},
	)

	// logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create transaction logger: %w", err)
	}

	events, errors := transact.ReadEvents()
	count, ok, e := 0, true, Event{}

	for ok && err == nil {
		select {
		case err, ok = <-errors:

		case e, ok = <-events:
			fmt.Printf("Event: %v\n", e)
			switch e.EventType {
			case EventDelete: // Got a DELETE event!
				err = Delete(e.Key)
				count++
			case EventPut: // Got a PUT event!
				fmt.Printf("Putting %v\n", e.Value)
				err = Put(e.Key, e.Value)
				count++
			}
		}
	}

	log.Printf("%d events replayed\n", count)

	transact.Run()

	go func() {
		for err := range transact.Err() {
			log.Print(err)
		}
	}()

	return err
}
