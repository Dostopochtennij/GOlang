package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func main() {
	conn, err := connect()
	if err != nil {
		panic((err))
	}
	
	ctx := context.Background()
	rows, err := conn.Query(ctx, "SELECT name,toString(uuid) as uuid_str FROM system.tables LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}
	
	for rows.Next() {
	var (
	    name, uuid string
	)
	if err := rows.Scan(
	    &name,
	    &uuid,
	); err != nil {
	    log.Fatal(err)
	}
	log.Printf("name: %s, uuid: %s",
	    name, uuid)
	}

}

func connect() (driver.Conn, error) {
  
	user, _ := os.LookupEnv("USER_DB")
  	password, _ := os.LookupEnv("PASS_DB")
	dbname, _ := os.LookupEnv("NAME_DB")
    
  var (
        ctx       = context.Background()
        conn, err = clickhouse.Open(&clickhouse.Options{
            Addr: []string{"<CLICKHOUSE_SECURE_NATIVE_HOSTNAME>:9440"},
            Auth: clickhouse.Auth{
                Database: dbname,
                Username: user,
                Password: password,
            },
            ClientInfo: clickhouse.ClientInfo{
                Products: []struct {
                    Name    string
                    Version string
                }{
                    {Name: "an-example-go-client", Version: "0.1"},
                },
            },

            Debugf: func(format string, v ...interface{}) {
                fmt.Printf(format, v)
            },
            TLS: &tls.Config{
                InsecureSkipVerify: true,
            },
        })
    )

    if err != nil {
        return nil, err
    }

    if err := conn.Ping(ctx); err != nil {
        if exception, ok := err.(*clickhouse.Exception); ok {
            fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
        }
        return nil, err
    }
    return conn, nil
}
