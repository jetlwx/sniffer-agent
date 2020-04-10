package exporter

import (
	"flag"
	"os"

	"github.com/gogf/gf/util/gconv"

	"github.com/gogf/gf/encoding/gjson"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jetlwx/comm"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/jetlwx/sniffer-agent/model"
)

/*
CREATE TABLE mysqlAudit(
	serIP String,
	serPort Int64,
	srcIP String,
	srcPort Int64,
	user String,
	db String,
	sqlStr String,
	cms Int32,
	rate Int32,
	ts Int64,
	create_date Date DEFAULT CAST(now(),'Date')
	 ) ENGINE = MergeTree(create_date,(srcIP,serIP,ts), 8192);

*/
var (
	clickHouseServer string
	clickHousePort   string
	clickHouseUser   string
	clickHousePass   string
	clickWriteCount  int
	conn             *sqlx.DB
	msgCh            = make(chan *string, 1024)
)

type clickHouseExporter struct{}

func NewClicKHouseExporter() *clickHouseExporter {
	newclickConn()

	return &clickHouseExporter{}
}
func init() {
	flag.StringVar(&clickHouseServer, "cServer", "127.0.0.1", "clickhouse DB Server IP")
	flag.StringVar(&clickHousePort, "cPort", "9000", "clickhouse DB Server Port")
	flag.StringVar(&clickHouseUser, "cUser", "default", "clickhouse DB login Username")
	flag.StringVar(&clickHousePass, "cPass", "", "clickhouse DB login password")
	flag.IntVar(&clickWriteCount, "cCount", 50, "一次写入clickhouse条数")

}
func (ch *clickHouseExporter) Export(qp model.QueryPiece) (err error) {
	//fmt.Println(*qp.String())
	//comm.Jlog.Debug(*qp.String())
	msgCh <- qp.String()
	return
}
func newclickConn() {
	cCon := "tcp://" + clickHouseServer + ":" + clickHousePort + "/?database=default&debug=true"
	connect, err := sqlx.Open("clickhouse", cCon)
	if err != nil {
		comm.Jlog.Error(err)
		os.Exit(1)
	}
	conn = connect

	go cDo()
}

func cDo() {
	str := []*string{}
	count := 0

	for {
		select {
		case m := <-msgCh:
			str = append(str, m)
			count++
			if count >= clickWriteCount {
				sub(str)
				str = []*string{}
				count = 0
			}

		default:
			continue
		}
	}
}

func sub(str []*string) {
	tx, err := conn.Begin()
	if err != nil {
		comm.Jlog.Error(err)
		return
	}
	sqlIn := "insert into mysqlAudit (serIP,serPort,srcIP,srcPort,user,db,sqlStr,cms,rate,ts)  " + " "
	sqlIn += "VALUES (?,?,?,?,?,?,?,?,?,?)"
	stmt, err := tx.Prepare(sqlIn)
	if err != nil {
		comm.Jlog.Error(err)
		return
	}

	for _, v := range str {
		js, err := gjson.LoadContent(*v, true)
		if err != nil {
			comm.Jlog.Error(err)
			continue
		}

		//{"sip":"172.16.6.210","sport":3306,"cpr":1,"bt":1586342896131,"cip":"172.16.6.55","cport":3306,
		//"user":null,"db":null,"sql":"SET PROFILING=1;","cms":0}
		sip := js.Get("sip")
		sport := gconv.Int(js.Get("sport"))
		cpr := gconv.Int(js.Get("cpr"))
		bt := gconv.Int64(js.Get("bt"))
		cip := gconv.String(js.Get("cip"))
		cport := gconv.Int(js.Get("cport"))
		user := gconv.String(js.Get("user"))
		db := gconv.String(js.Get("db"))
		sqlStr := gconv.String(js.Get("sql"))
		cms := gconv.Int(js.Get("cms"))

		if _, err := stmt.Exec(sip, sport, cip, cport, user, db, sqlStr, cms, cpr, bt); err != nil {
			comm.Jlog.Error(err)
			continue
		}
		// sqlIn := "insert into mysqlAudit (serIP,serPort,srcIP,srcPort,user,db,sqlStr,cms,rate,ts)  " + " "
		// sqlIn += "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
		// tx.MustExec(sqlIn, sip, sport, cip, cport, user, db, sqlStr, cms, cpr, bt)
	}

	tx.Commit()
	comm.Jlog.Info("写入数据：", len(str), "条")
}
