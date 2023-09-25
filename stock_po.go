package main

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

type stock_po struct {
	file_headers []string
}

func get_stock_po() stock_po {
	return stock_po{file_headers: []string{"ITEMCODE", "PONUMBER", "PODATE", "SUPPLIER", "SUPPLIERSITE",
		"QUANTITYORDERED", "UNITPRICE", "CURRENCY", "CONVRATE", "RECEIVINGORG", "QUANTITYDELIVERED",
		"QUANTITYCANCELLED", "QUANTITYPENDING", "QUANTITYEXCESS", "CLOSEDCODE"}}
}

func (file stock_po) get_headers() []string {
	return file.file_headers
}

func (file stock_po) write_to_db(rows [][]string, tx *sql.Tx) error {
	//truncate db table
	truncate_sql := `TRUNCATE TABLE stock_po`
	_, err := tx.Exec(truncate_sql)
	if err != nil {
		log.Printf("Query error: %s", err)
		return err
	}

	//create statement for bulkinsert
	stmt, err := tx.Prepare(pq.CopyIn("stock_po", "item_code", "po_number", "po_date", "supplier", "supplier_site", "quantity_ordered",
		"unit_price", "currency", "conv_rate", "receiving_org", "quantity_delivered", "quantity_cancelled",
		"quantity_pending", "quantity_excess", "closed_code"))
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
		data := filter_row(row, 15)

		//if data write to statement
		if data != nil {
			po_date, err := set_date_format(data[2], "02-01-2006", "2006-01-02")
			if err != nil {
				log.Print(err)
				return err
			}
			quantity_ordered := set_default_float(data[5], 0.0)
			unit_price := set_default_float(data[6], 0.0)
			conv_rate := set_default_float(data[8], 0.0)
			quantity_delivered := set_default_float(data[10], 0.0)
			quantity_cancelled := set_default_float(data[11], 0.0)
			quantity_pending := set_default_float(data[12], 0.0)
			quantity_excess := set_default_float(data[13], 0.0)

			_, err = stmt.Exec(data[0], data[1], po_date, data[3], data[4], quantity_ordered, unit_price, data[7], conv_rate, data[9],
				quantity_delivered, quantity_cancelled, quantity_pending, quantity_excess, data[14])
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
