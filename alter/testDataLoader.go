package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	//Driver for Oracle database
	_ "gopkg.in/goracle.v2"
)

type Compound struct {
	UCI              string           `json:"uci,omitempty"`
	Inchi            string           `json:"inchi"`
	StandardInchiKey string           `json:"standard_inchi_key"`
	Smiles           string           `json:"smiles"`
}

var db *sql.DB

func main(){
	var err error

	db, err = sql.Open("goracle", "hr/hr@oradb:1521/xe")
	if err != nil {
		fmt.Println("ERROR DB ", err)
	}
	defer db.Close()

	fmt.Println("Test start")

	f, err := os.Open("csv_file2M.csv")
	if err != nil {
		fmt.Println("error reading file bru")
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.Comma = '|'

	for {
		rec, err := r.Read()
		if err == io.EOF {
			fmt.Println("END OF FILE")
			break
		}
		if err != nil {
			fmt.Println("ERROR MATE ", err)
			break
		}

		insertData(rec)
	}

}

func insertData(rec []string){
	_, err := db.Exec(
		"INSERT INTO INCHIS (UCI, STANDARDINCHI, STANDARDINCHIKEY, SMILES) VALUES (:1, :2, :3, :4)",
		rec[0],
		rec[1],
		rec[2],
		rec[3])
	if err != nil {
		println("Error inserting line ", err)
	}
}