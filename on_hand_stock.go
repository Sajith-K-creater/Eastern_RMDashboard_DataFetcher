package main

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

type on_hand_stock struct {
	file_headers []string
}

func get_on_hand_stock() on_hand_stock {
	return on_hand_stock{file_headers: []string{"ITEMCODE", "DESCRIPTION", "CATEGORY", "GROUPNAME", "SUBGROUP",
		"SUBINVENTORYCODE", "ORGANIZATIONID", "ORGANIZATIONCODE", "ORGANIZATIONNAME", "SUBINVDESC", "ONHANDQUANTITY",
		"LOTNUMBER", "UOMCODE", "LOCATION", "RECIEPTNUMBER", "PURCHASEDATE", "RECEIVINGDATE", "PURCHASEAGE", "RECEIVINGAGE",
		"PONUMBER", "UNITPRICE"}}
}

func (file on_hand_stock) get_headers() []string {
	return file.file_headers
}

func (file on_hand_stock) write_to_db(rows [][]string, tx *sql.Tx) error {
	//truncate db table
	truncate_sql := `Truncate table onhandstock`
	_, err := tx.Exec(truncate_sql)
	if err != nil {
		log.Printf("Query error: %s", err)
		return err
	}

	//create statement for bulkinsert
	stmt, err := tx.Prepare(pq.CopyIn("onhandstock", "item_code", "description", "category", "group_name", "sub_group",
		"subinventory_code", "organization_id", "organization_code", "organization_name", "subinvdesc", "onhand_quantity",
		"lot_number", "uom_code", "location", "reciept_number", "purchase_date", "receiving_date", "purchase_age", "receiving_age",
		"po_number", "unit_price"))
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
		data := filter_row(row, 21)

		//if data write to statement
		if data != nil {
			onhand_quantity := set_default_float(data[10], 0.0)

			purchase_date, err := set_date_format(data[15], "02-01-2006", "2006-01-02")
			if err != nil {
				log.Print(err)
				return err
			}
			receiving_date, err := set_date_format(data[16], "02-01-2006", "2006-01-02")
			if err != nil {
				log.Print(err)
				return err
			}
			purchase_age := set_default_int(data[17], 0)
			receiving_age := set_default_int(data[18], 0)
			unit_price := set_default_float(data[20], 0.0)

			_, err = stmt.Exec(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
				data[9], onhand_quantity, data[11], data[12], data[13], data[14], purchase_date, receiving_date, purchase_age, receiving_age,
				data[19], unit_price)
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
