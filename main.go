package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	now := time.Now()

	godotenv.Load()
	sftp_host := os.Getenv("SFTP_HOST")
	sftp_port := os.Getenv("SFTP_PORT")
	sftp_username := os.Getenv("SFTP_USERNAME")
	sftp_password := os.Getenv("SFTP_PASSWORD")
	remote_directory_root_path := os.Getenv("REMOTE_DIRECTORY_ROOT_PATH")
	local_directory_root_path := os.Getenv("LOCAL_DIRECTORY_ROOT_PATH")
	remote_directory_processed_path := os.Getenv("REMOTE_DIRECTORY_PROCESSED_FILE_PATH")
	total_file_count, err := strconv.Atoi(os.Getenv("FILE_COUNT"))
	if err != nil {
		log.Print(err)
		return
	}

	db_host := os.Getenv("DB_HOST")
	db_port := os.Getenv("DB_PORT")
	db_username := os.Getenv("DB_USERNAME")
	db_password := os.Getenv("DB_PASSWORD")
	db_name := os.Getenv("DB_NAME")

	processed_file_count := 0

	//connect sftp
	client, conn, err := connect_sftp(sftp_host, sftp_port, sftp_username, sftp_password)
	if err != nil {
		return
	}

	//connect db
	db, err := connect_db(db_host, db_port, db_username, db_password, db_name)
	if err != nil {
		return
	}

	//read remote directory
	remote_files, err := read_remote_directory(remote_directory_root_path, client)
	if err != nil {
		return
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Print(err)
		return
	}

	for _, file := range remote_files {
		if !file.IsDir() {
			file_name := file.Name()

			// copy file to local
			err := copy_file(remote_directory_root_path, local_directory_root_path, file_name, client)
			if err != nil {
				tx.Rollback()
				return
			}

			// process file
			err = write_excel_to_db(local_directory_root_path, file_name, tx, &processed_file_count)
			if err != nil {
				tx.Rollback()
				return
			}

			// remove processed file from local
			err = os.Remove(local_directory_root_path + "/" + file.Name())
			if err != nil {
				log.Printf("couldn't remove local file")
			}
		}
	}

	//check if all files are processed
	if processed_file_count != total_file_count {
		tx.Rollback()
		return
	}

	tx.Commit()
	//move all files to processed location
	for _, file := range remote_files {
		if !file.IsDir() {
			err = move_file(remote_directory_root_path, remote_directory_processed_path, file.Name(), client)
			if err != nil {
				log.Printf("cannot move file: %s", err)
				return
			}
		}
	}

	defer client.Close()
	defer conn.Close()
	defer db.Close()

	fmt.Printf("Elapsed Time: %v\n", time.Since(now))
}
