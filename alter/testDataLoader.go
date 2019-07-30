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
	UCI              string `json:"uci,omitempty"`
	Inchi            string `json:"inchi"`
	StandardInchiKey string `json:"standard_inchi_key"`
	Smiles           string `json:"smiles"`
}

var db *sql.DB

func main() {
	var err error
	limit := 1200000

	db, err = sql.Open("goracle", "hr/hr@oradb:1521/xe")
	if err != nil {
		fmt.Println("ERROR DB ", err)
	}
	defer db.Close()

	fmt.Println("Test start")

	f, err := os.Open("test_files/csv_file2M.csv")
	if err != nil {
		fmt.Println("error reading file bru")
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.Comma = '|'
	line := 0
	for {
		if line >= limit {
			break
		}
		rec, err := r.Read()
		if err == io.EOF {
			fmt.Println("END OF FILE")
			break
		}
		if err != nil {
			fmt.Println("ERROR MATE ", err)
			break
		}

		insertData(rec, line)
		line++
	}

}

func insertData(rec []string, line int) {
	fmt.Printf("\r%d UCI:%s SRC_ID: %s                                       ", line, rec[0], rec[7])
	_, err := db.Exec(
		"INSERT INTO INCHIS (UCI, STANDARDINCHI, STANDARDINCHIKEY, SMILES, SRC_COMPOUND_ID, src_id, NAME_LONG, NAME_LABEL, DESCRIPTION, BASE_ID_URL) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)",
		rec[0],
		rec[1],
		rec[2],
		rec[3],
		rec[4],
		rec[5],
		rec[6],
		rec[7],
		rec[8],
		rec[9],
	)
	if err != nil {
		println("Error inserting line ")
		panic(err)
	}
}
