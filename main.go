package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	err = initializeTransactionLog()
	if err != nil {
		fmt.Printf("Error initializing transaction log: %v\n", err)
		panic(err)
	}
	r := mux.NewRouter()
	r.HandleFunc("/v1/key/{key}", putHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", getHandler).Methods("GET")

	log.Fatal(http.ListenAndServe(":8080", r))

}
