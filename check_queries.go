package main

import (
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"log"
	"os"
)

const (
	port = 5432
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

func conn_db(host string, query string) {

	user, _ := os.LookupEnv("USER_DB")
	password, _ := os.LookupEnv("PASS_DB")
	dbname, _ := os.LookupEnv("NAME_DB")

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	defer db.Close()

	err = db.Ping()
	CheckError(err)

	rows, err := db.Query(query)
	CheckError(err)

	defer rows.Close()
	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		CheckError(err)

		fmt.Println(name)
	}

	CheckError(err)

}

func main() {
	transCashError, _ := os.LookupEnv("SELECT_TR_C_ERR")
	endConDB, _ := os.LookupEnv("SELECT_END_CON_DB")
	freezeQueryDB, _ := os.LookupEnv("SELECT_FREEZY_QUERY")
	sendErrorIDOC, _ := os.LookupEnv("SELECT_ERROR_IDOC")
	defferProd, _ := os.LookupEnv("SELECT_DEFFER_PROD")
	endOfDay, _ := os.LookupEnv("SELECT_END_OF_DAY")
	remains, _ := os.LookupEnv("SELECT_REMAINS")
	duplicateTickets, _ := os.LookupEnv("SELECT_DUPL_TICKETS")
	zeroTickets, _ := os.LookupEnv("SELECT_ZERO_TICKETS")
	alcoProhib, _ := os.LookupEnv("SELECT_ALCO_PROHIB")
	alcoMRC, _ := os.LookupEnv("SELECT_ALCO_MRC")
	absentEGAIS, _ := os.LookupEnv("SELECT_ABSENT_EGAIS")

	m := make(map[string]string)
	m["TransactionCacheError"] = transCashError
	m["EndConnectionToDB"] = endConDB
	m["FreezeQueryToDB"] = freezeQueryDB
	m["SendErrorOfIdoc"] = sendErrorIDOC
	m["DeferredProduct"] = defferProd
	m["EndOfDay"] = endOfDay
	m["Remains"] = remains
	m["DuplicateOfTickets"] = duplicateTickets
	m["ZeroTickets"] = zeroTickets
	m["AlcoProhibited"] = alcoProhib
	m["AlcoSellingsLowMRC"] = alcoMRC
	m["AbsentOnEGAIS"] = absentEGAIS

	switch {
	case len(os.Args) == 2:
		{
			ip := os.Args[1]
			if ip == "help" {
				fmt.Println(`
	For the script to work, you need to pass two parameters:

        1) DNS/IP BO, to which you need to connect;
        2) Name of the query to be executed.

	List of available requests:

    TransactionCacheError - Number of unsent transaction records
    EndConnectionToDB - Number of active connections to the store database
    FreezeQueryToDB - Number of queries in the database that take more than 5 minutes to complete
    SendErrorOfIdoc - Number of records sent IDOC's with errors
    DeferredProduct - Number of pending transactions older than 18 days
    EndOfDay - Date of last closed day
    Remains - Number of discrepancies found between current balances and movement history
    DuplicateOfTickets - Number of checks with the same values
    ZeroTickets - Number of checks equal to zero
    AlcoProhibited - Sale of alcohol during prohibited hours
    AlcoSellingsLowMRC - Alcohol below the minimum retail price
    AbsentOnEGAIS - Products with this brand are not available in EGAIS
    `)
			}
		}
	case len(os.Args) == 3:
		{
			ip := os.Args[1]
			name := os.Args[2]

			conn_db(ip, m[name])
		}
	default:
		{
			fmt.Println(`Invalid parameters entered. For information, use the key "help".`)
		}
	}

}
