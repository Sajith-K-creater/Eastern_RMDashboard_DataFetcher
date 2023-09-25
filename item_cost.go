package main

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

type item_cost struct {
	file_headers []string
}

func get_item_cost() item_cost {
	return item_cost{file_headers: []string{"ORGCODE", "ITEMCODE", "DATE", "COST",
		"OPENINGQUANTITY", "TRANSACTIONQUANTITY", "AVGPURCHASECOST"}}
}

func (file item_cost) get_headers() []string {
	return file.file_headers
}

func (file item_cost) write_to_db(rows [][]string, tx *sql.Tx) error {

	//delete db table
	for i, row := range rows {
		//skip header
		if i == 0 {
			continue
		}
		//remove empty rows and convert empty string to sqlnull
		data := filter_row(row, 7)
		if data != nil {
			if data[2].Valid {
				date, err := set_date_format(data[2], "01-02-06", "2006-01-02")
				if err != nil {
					log.Printf("date parse error %v", err)
					return err
				}
				delete_sql := `DELETE FROM item_cost WHERE date=$1`
				_, err = tx.Exec(delete_sql, date.String)
				if err != nil {
					log.Printf("Query error: %s", err)
					return err
				}
				break
			}
		}
	}

	//create statement for bulkinsert
	stmt, err := tx.Prepare(pq.CopyIn("item_cost", "org_code", "item_code", "date", "cost", "opening_quantity",
		"transaction_quantity", "avg_purchase_cost"))
	if err != nil {
		log.Print(err)
		return err
	}

	//read excel rows to statment
	for i, row := range rows {

		//skip header
		if i == 0 {
			continue
		}

		//remove empty rows and convert empty string to sqlnull
		data := filter_row(row, 7)

		//if data write to statement
		if data != nil {

			date, err := set_date_format(data[2], "01-02-06", "2006-01-02")
			if err != nil {
				log.Print(err)
				return err
			}
			cost := set_default_float(data[3], 0.0)
			opening_quantity := set_default_float(data[4], 0.0)
			transaction_quantity := set_default_float(data[5], 0.0)
			avg_purchase_cost := set_default_float(data[6], 0.0)

			_, err = stmt.Exec(data[0], data[1], date, cost, opening_quantity, transaction_quantity, avg_purchase_cost)
			if err != nil {
				log.Print(err)
				return err
			}
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Print(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Print(err)
	}

	return nil
}
