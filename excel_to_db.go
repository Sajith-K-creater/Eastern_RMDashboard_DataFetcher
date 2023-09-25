package main

import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

type file interface {
	write_to_db(rows [][]string, db *sql.Tx) error
	get_headers() []string
}

func write_excel_to_db(local_directory_root_path, file_name string, tx *sql.Tx, file_count *int) error {

	log.Print("Opening file: " + file_name)

	f, err := excelize.OpenFile(local_directory_root_path + "/" + file_name)
	if err != nil {
		log.Print(err)
		return err
	}

	rows, err := f.GetRows("Sheet1")
	if err != nil {
		log.Print(err)
		return err
	}

	var file file
	switch {
	case strings.Contains(file_name, "ON_HAND_STOCK"):
		file = get_on_hand_stock()
		*file_count++
	case strings.Contains(file_name, "ITEM_COST"):
		file = get_item_cost()
		*file_count++
	case strings.Contains(file_name, "STOCK_PO"):
		file = get_stock_po()
		*file_count++
	}

	if file != nil {
		err = check_file_header(file, rows)
		if err != nil {
			return err
		}
		err = write_to_db(file, rows, tx)
		if err != nil {
			return err
		}
	}

	defer f.Close()

	return nil
}

func check_file_header(file file, rows [][]string) error {

	file_headers := file.get_headers()

	if len(rows) == 0 {
		return errors.New("no headers")
	}

	header := rows[0]

	if len(header) != len(file_headers) {
		return errors.New("header count doesn't match")
	}

	for i := range header {
		if file_headers[i] != header[i] {
			log.Print("headers not match")
			return errors.New("headers not match")
		}
	}

	return nil
}

func write_to_db(file file, rows [][]string, tx *sql.Tx) error {
	err := file.write_to_db(rows, tx)
	if err != nil {
		return err
	}

	return nil
}

func filter_row(row []string, length int) []sql.NullString {

	row_len := len(row)
	null_len := 0
	filtered_row := make([]sql.NullString, length)

	for i := 0; i < length; i++ {
		if i >= row_len {
			filtered_row[i] = sql.NullString{}
			null_len++
			continue
		}
		if len(strings.TrimSpace(row[i])) == 0 {
			filtered_row[i] = sql.NullString{}
			null_len++
		} else {
			filtered_row[i] = sql.NullString{
				String: row[i],
				Valid:  true,
			}
		}
	}

	if null_len == length {
		return nil
	}

	return filtered_row

}

func set_default_float(data sql.NullString, default_value float64) string {
	if !data.Valid {
		return strconv.FormatFloat(default_value, 'f', -1, 64)
	} else {
		return data.String
	}
}

func set_default_int(data sql.NullString, default_value int64) string {
	if !data.Valid {
		return strconv.FormatInt(default_value, 10)
	} else {
		return data.String
	}
}

func set_date_format(data sql.NullString, input_format string, output_format string) (sql.NullString, error) {
	if !data.Valid {
		return sql.NullString{}, nil
	}

	in_time, err := time.Parse(input_format, data.String)
	if err != nil {
		return sql.NullString{}, err
	}

	out_time := in_time.Format(output_format)
	return sql.NullString{
		String: out_time,
		Valid:  true,
	}, nil

}
