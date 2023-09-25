package main

import (
	"io"
	"io/fs"
	"log"
	"os"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func connect_sftp(host, port, username, password string) (*sftp.Client, *ssh.Client, error) {

	host = host + ":" + port
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	log.Print("dialing host " + host)
	conn, err := ssh.Dial("tcp", host, config)
	if err != nil {
		log.Print("Failed to dial: ", err)
		return nil, nil, err
	}

	log.Print("creating client")
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Print("Failed to create SFTP client: ", err)
		return nil, nil, err
	}

	return client, conn, nil
}

func read_remote_directory(directory_path string, client *sftp.Client) ([]fs.FileInfo, error) {

	remote_files, err := client.ReadDir(directory_path)
	if err != nil {
		log.Print("Failed to open remote dir: ", err)
		return nil, err
	}

	return remote_files, nil
}

func copy_file(remote_directory_path, local_directory_path, file_name string, client *sftp.Client) error {
	remote_file, err := client.Open(remote_directory_path + "/" + file_name)
	if err != nil {
		log.Print("Failed to open remote file: ", err)
		return err
	}

	local_file, err := os.Create(local_directory_path + "/" + file_name)
	if err != nil {
		log.Print("Failed to create local file: ", err)
		return err
	}

	_, err = io.Copy(local_file, remote_file)
	if err != nil {
		log.Print("Failed to upload file: ", err)
		return err
	}

	defer remote_file.Close()
	defer local_file.Close()

	return nil
}

func move_file(remote_directory_path, remote_directory_processed_path, file_name string, client *sftp.Client) error {
	remote_file, err := client.Open(remote_directory_path + "/" + file_name)
	if err != nil {
		log.Print("Failed to open remote file: ", err)
		return err
	}

	remote_processed_file, err := client.Create(remote_directory_processed_path + "/" + file_name)
	if err != nil {
		log.Print("Failed to create local file: ", err)
		return err
	}

	//copy files to processed files directory
	_, err = io.Copy(remote_processed_file, remote_file)
	if err != nil {
		log.Print("Failed to upload file: ", err)
		return err
	}

	//remove copied files
	client.Remove(remote_directory_path + "/" + file_name)

	defer remote_file.Close()
	defer remote_processed_file.Close()

	return nil
}
