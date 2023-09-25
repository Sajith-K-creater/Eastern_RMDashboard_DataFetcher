package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func connect_db(host, port, username, password, dbname string) (*sql.DB, error) {

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, username, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	return db, nil

}
