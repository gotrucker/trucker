//go:build gnarly

package main

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tonyfg/trucker/test/helpers"
)

const (
	StoreSalesCount = 3_000_000
	DateDimCount    = 73_049
	StoreCount      = 12
	CustomerCount   = 100_000
	ItemCount       = 18_000

	// First valid d_date_sk for store_sales references; covers dates 1900-01-02 through 2099-12-31.
	dateDimMinSk = 1
	dateDimMaxSk = DateDimCount
)

// ensureTpcdsLoaded sets up TPC-DS schema and data, skipping data generation if
// store_sales already has the expected row count from a prior run.
func ensureTpcdsLoadedPgOut(t *testing.T) (*pgx.Conn, *pgx.Conn) {
	t.Helper()

	pgConn := helpers.Connect(helpers.PostgresCfg)

	var count int64
	if err := pgConn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='store_sales'",
	).Scan(&count); err != nil || count == 0 {
		t.Log("TPC-DS schema not present, creating from scratch...")
		prepareTpcdsSchema(pgConn)
		generateAndLoad(t, pgConn)
	} else {
		var rowCount int64
		pgConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM public.store_sales").Scan(&rowCount)
		if rowCount != StoreSalesCount {
			t.Logf("store_sales has %d rows (expected %d), reloading...", rowCount, StoreSalesCount)
			prepareTpcdsSchema(pgConn)
			generateAndLoad(t, pgConn)
		} else {
			t.Log("TPC-DS data already loaded, resetting replication artifacts only.")
			resetReplicationArtifacts(pgConn)
		}
	}

	pgOutConn := prepareTpcdsPostgresOutput()
	return pgConn, pgOutConn
}

func prepareTpcdsPostgresOutput() *pgx.Conn {
	conn := helpers.Connect(helpers.PostgresOutputCfg)
	sql := readFixtureSQL("tpcds_postgres_output.sql")
	for _, stmt := range splitSQL(sql) {
		if _, err := conn.Exec(context.Background(), stmt); err != nil {
			panic(fmt.Sprintf("error executing TPC-DS postgres output DDL:\n%s\n%v", stmt, err))
		}
	}
	return conn
}

func ensureTpcdsLoaded(t *testing.T) (*pgx.Conn, *ch.Client) {
	t.Helper()

	pgConn := helpers.Connect(helpers.PostgresCfg)

	var count int64
	if err := pgConn.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='store_sales'",
	).Scan(&count); err != nil || count == 0 {
		t.Log("TPC-DS schema not present, creating from scratch...")
		prepareTpcdsSchema(pgConn)
		generateAndLoad(t, pgConn)
	} else {
		var rowCount int64
		pgConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM public.store_sales").Scan(&rowCount)
		if rowCount != StoreSalesCount {
			t.Logf("store_sales has %d rows (expected %d), reloading...", rowCount, StoreSalesCount)
			prepareTpcdsSchema(pgConn)
			generateAndLoad(t, pgConn)
		} else {
			t.Log("TPC-DS data already loaded, resetting replication artifacts only.")
			resetReplicationArtifacts(pgConn)
		}
	}

	chConn := prepareTpcdsClickhouse()
	return pgConn, chConn
}

func prepareTpcdsSchema(conn *pgx.Conn) {
	sql := readFixtureSQL("tpcds_postgres.sql")
	// Execute each statement separately; the file uses ';' + newline as delimiters.
	for _, stmt := range splitSQL(sql) {
		if _, err := conn.Exec(context.Background(), stmt); err != nil {
			panic(fmt.Sprintf("error executing TPC-DS postgres DDL:\n%s\n%v", stmt, err))
		}
	}
}

func resetReplicationArtifacts(conn *pgx.Conn) {
	stmts := []string{
		"DELETE FROM pg_publication",
		"DROP PUBLICATION IF EXISTS trucker_trucker3",
		"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots",
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots",
	}
	for _, stmt := range stmts {
		conn.Exec(context.Background(), stmt)
	}
}

func prepareTpcdsClickhouse() *ch.Client {
	conn, err := ch.Dial(context.Background(), ch.Options{
		Address:    fmt.Sprintf("%s:%d", helpers.ClickhouseCfg.Host, helpers.ClickhouseCfg.Port),
		Database:   helpers.ClickhouseCfg.Database,
		User:       helpers.ClickhouseCfg.User,
		Password:   helpers.ClickhouseCfg.Pass,
		ClientName: "trucker",
	})
	if err != nil {
		panic(err)
	}

	sql := readFixtureSQL("tpcds_clickhouse.sql")
	for _, stmt := range splitSQL(sql) {
		if err := conn.Do(context.Background(), ch.Query{Body: stmt}); err != nil {
			panic(fmt.Sprintf("error executing TPC-DS clickhouse DDL:\n%s\n%v", stmt, err))
		}
	}
	return conn
}

func readFixtureSQL(filename string) string {
	path := filepath.Join(Basepath, "../fixtures", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("cannot read fixture %s: %v", filename, err))
	}
	return string(data)
}

func splitSQL(sql string) []string {
	var stmts []string
	for _, s := range strings.Split(sql, ";\n") {
		s = strings.TrimSpace(s)
		if s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}

// generateAndLoad generates TPC-DS shaped data and loads it into Postgres via COPY.
func generateAndLoad(t *testing.T, conn *pgx.Conn) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	t.Log("Loading store (12 rows)...")
	loadTable(ctx, conn, pgx.Identifier{"public", "store"}, storeColumns(), generateStoreRows())

	t.Log("Loading date_dim (73049 rows)...")
	loadTableFromSource(ctx, conn, pgx.Identifier{"public", "date_dim"}, dateDimColumns(), generateDateDimSource())

	t.Logf("Loading item (%d rows)...", ItemCount)
	loadTableFromSource(ctx, conn, pgx.Identifier{"public", "item"}, itemColumns(), generateItemSource(rng))

	t.Logf("Loading customer (%d rows)...", CustomerCount)
	loadTableFromSource(ctx, conn, pgx.Identifier{"public", "customer"}, customerColumns(), generateCustomerSource(rng))

	t.Logf("Loading store_sales (%d rows)...", StoreSalesCount)
	rng2 := rand.New(rand.NewSource(99))
	loadTableFromSource(ctx, conn, pgx.Identifier{"public", "store_sales"}, storeSalesColumns(), generateStoreSalesSource(rng2))
	t.Log("Data load complete.")
}

func loadTable(ctx context.Context, conn *pgx.Conn, id pgx.Identifier, cols []string, rows [][]any) {
	src := pgx.CopyFromRows(rows)
	n, err := conn.CopyFrom(ctx, id, cols, src)
	if err != nil {
		panic(fmt.Sprintf("CopyFrom %v failed: %v", id, err))
	}
	_ = n
}

func loadTableFromSource(ctx context.Context, conn *pgx.Conn, id pgx.Identifier, cols []string, src pgx.CopyFromSource) {
	n, err := conn.CopyFrom(ctx, id, cols, src)
	if err != nil {
		panic(fmt.Sprintf("CopyFrom %v failed: %v", id, err))
	}
	_ = n
}

// ---------- store ----------

func storeColumns() []string {
	return []string{
		"s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date",
		"s_closed_date_sk", "s_store_name", "s_number_employees", "s_floor_space",
		"s_hours", "s_manager", "s_market_id", "s_geography_class",
		"s_market_desc", "s_market_manager", "s_division_id", "s_division_name",
		"s_company_id", "s_company_name", "s_street_number", "s_street_name",
		"s_street_type", "s_suite_number", "s_city", "s_county",
		"s_state", "s_zip", "s_country", "s_gmt_offset", "s_tax_precentage",
	}
}

var storeStates = []string{"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID"}
var storeNames = []string{
	"Allied Home", "Blue Mart", "Commerce Plus", "Delta Store", "Eagle Plaza",
	"Family Mart", "Grand Depot", "Harbor Center", "Island Market", "Junction Mall",
	"King's Corner", "Lakeside Shop",
}
var storeCities = []string{
	"Fairview", "Riverside", "Springfield", "Georgetown", "Midvale",
	"Hillcrest", "Oakwood", "Pinehurst", "Sandpoint", "Clearwater",
	"Greenfield", "Lakewood",
}

func generateStoreRows() [][]any {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([][]any, StoreCount)
	for i := range StoreCount {
		sk := i + 1
		rows[i] = []any{
			int32(sk),
			fmt.Sprintf("STORE%011d", sk),
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			epoch.AddDate(9999-1970, 11, 30), // 9999-12-31
			int32(0),
			storeNames[i],
			int32(50 + i*10),
			int32(100000 + i*5000),
			"8AM-11PM           ",
			fmt.Sprintf("Manager %d", i+1),
			int32(i + 1),
			"Unknown",
			fmt.Sprintf("Market %d serves region %d", i+1, i+1),
			fmt.Sprintf("Market Mgr %d", i+1),
			int32(1),
			"Division One",
			int32(1),
			"TPC-DS Corp",
			fmt.Sprintf("%d", 100+i),
			"Main St",
			"Boulevard      ",
			fmt.Sprintf("Suite %d  ", i+1),
			storeCities[i],
			fmt.Sprintf("%s County", storeStates[i]),
			storeStates[i],
			fmt.Sprintf("%05d     ", 10000+i*1000),
			"United States",
			decimalVal(-5.00 + float64(i)*0.5),
			decimalVal(0.06),
		}
	}
	return rows
}

// ---------- date_dim ----------

func dateDimColumns() []string {
	return []string{
		"d_date_sk", "d_date_id", "d_date",
		"d_month_seq", "d_week_seq", "d_quarter_seq",
		"d_year", "d_dow", "d_moy", "d_dom", "d_qoy",
		"d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq",
		"d_day_name", "d_quarter_name",
		"d_holiday", "d_weekend", "d_following_holiday",
		"d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq",
		"d_current_day", "d_current_week", "d_current_month",
		"d_current_quarter", "d_current_year",
	}
}

var dayNames = []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

func generateDateDimSource() pgx.CopyFromSource {
	base := time.Date(1900, 1, 2, 0, 0, 0, 0, time.UTC)
	monthSeqBase := (1900-1900)*12 + 1
	quarterSeqBase := (1900-1900)*4 + 1
	weekSeqBase := 1

	return pgx.CopyFromSlice(DateDimCount, func(i int) ([]any, error) {
		d := base.AddDate(0, 0, i)
		sk := i + 1
		year, month, day := d.Date()
		dow := int(d.Weekday())
		moy := int(month)
		dom := day
		qoy := (moy-1)/3 + 1
		monthSeq := monthSeqBase + (year-1900)*12 + (moy - 1)
		quarterSeq := quarterSeqBase + (year-1900)*4 + (qoy - 1)
		weekSeq := weekSeqBase + i/7
		fyYear := year
		fyQuarterSeq := quarterSeq
		fyWeekSeq := weekSeq
		quarterName := fmt.Sprintf("%dQ%d", year, qoy)
		holiday := "N"
		weekend := "N"
		if dow == 0 || dow == 6 {
			weekend = "Y"
		}
		followingHoliday := "N"
		firstDom := sk - dom + 1
		lastDom := firstDom + daysInMonth(year, int(month)) - 1
		sameDayLY := sk - 365
		if isLeapYear(year - 1) {
			sameDayLY = sk - 366
		}
		sameDayLQ := sk - 91
		return []any{
			int32(sk),
			fmt.Sprintf("%-16s", d.Format("2006-01-02")),
			d,
			int32(monthSeq),
			int32(weekSeq),
			int32(quarterSeq),
			int32(year),
			int32(dow),
			int32(moy),
			int32(dom),
			int32(qoy),
			int32(fyYear),
			int32(fyQuarterSeq),
			int32(fyWeekSeq),
			dayNames[dow],
			quarterName,
			holiday,
			weekend,
			followingHoliday,
			int32(firstDom),
			int32(lastDom),
			int32(sameDayLY),
			int32(sameDayLQ),
			"N",
			"N",
			"N",
			"N",
			"N",
		}, nil
	})
}

func daysInMonth(year, month int) int {
	return time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.UTC).Day()
}

func isLeapYear(y int) bool {
	return (y%4 == 0 && y%100 != 0) || y%400 == 0
}

// ---------- item ----------

func itemColumns() []string {
	return []string{
		"i_item_sk", "i_item_id",
		"i_rec_start_date", "i_rec_end_date",
		"i_item_desc", "i_current_price", "i_wholesale_cost",
		"i_brand_id", "i_brand", "i_class_id", "i_class",
		"i_category_id", "i_category", "i_manufact_id", "i_manufact",
		"i_size", "i_formulation", "i_color", "i_units",
		"i_container", "i_manager_id", "i_product_name",
	}
}

var itemCategories = []string{"Books", "Children", "Electronics", "Home", "Jewelry", "Men", "Music", "Shoes", "Sports", "Women"}
var itemColors = []string{"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush"}
var itemSizes = []string{"extra large", "large", "medium", "N/A", "petite", "small"}

func generateItemSource(rng *rand.Rand) pgx.CopyFromSource {
	return pgx.CopyFromSlice(ItemCount, func(i int) ([]any, error) {
		sk := i + 1
		catIdx := i % len(itemCategories)
		cat := itemCategories[catIdx]
		price := 1.00 + rng.Float64()*499.00
		wholesale := price * (0.4 + rng.Float64()*0.3)
		return []any{
			int32(sk),
			fmt.Sprintf("%-16s", fmt.Sprintf("ITEM%012d", sk)),
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
			fmt.Sprintf("%-200s", fmt.Sprintf("Product %d in category %s", sk, cat)),
			decimalVal(price),
			decimalVal(wholesale),
			int32(catIdx + 1),
			fmt.Sprintf("%-50s", fmt.Sprintf("Brand%d", (sk%100)+1)),
			int32((sk % 16) + 1),
			fmt.Sprintf("%-50s", fmt.Sprintf("Class%d", (sk%16)+1)),
			int32(catIdx + 1),
			fmt.Sprintf("%-50s", cat),
			int32((sk % 50) + 1),
			fmt.Sprintf("%-50s", fmt.Sprintf("Manufact%d", (sk%50)+1)),
			fmt.Sprintf("%-20s", itemSizes[sk%len(itemSizes)]),
			fmt.Sprintf("%-20s", fmt.Sprintf("Formula%d", sk%10)),
			fmt.Sprintf("%-20s", itemColors[sk%len(itemColors)]),
			fmt.Sprintf("%-10s", "Each"),
			fmt.Sprintf("%-10s", "Unknown"),
			int32((sk % 100) + 1),
			fmt.Sprintf("%-50s", fmt.Sprintf("Product Name %d", sk)),
		}, nil
	})
}

// ---------- customer ----------

func customerColumns() []string {
	return []string{
		"c_customer_sk", "c_customer_id",
		"c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk",
		"c_first_shipto_date_sk", "c_first_sales_date_sk",
		"c_salutation", "c_first_name", "c_last_name",
		"c_preferred_cust_flag",
		"c_birth_day", "c_birth_month", "c_birth_year",
		"c_birth_country", "c_login", "c_email_address",
		"c_last_review_date_sk",
	}
}

var firstNames = []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda"}
var lastNames = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"}
var salutations = []string{"Mr.       ", "Mrs.      ", "Ms.       ", "Dr.       ", "Sir       "}
var countries = []string{"United States", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil"}

func generateCustomerSource(rng *rand.Rand) pgx.CopyFromSource {
	return pgx.CopyFromSlice(CustomerCount, func(i int) ([]any, error) {
		sk := i + 1
		fn := firstNames[sk%len(firstNames)]
		ln := lastNames[sk%len(lastNames)]
		return []any{
			int32(sk),
			fmt.Sprintf("%-16s", fmt.Sprintf("CUST%012d", sk)),
			int32(rng.Int31n(1920800) + 1),
			int32(rng.Int31n(7200) + 1),
			int32(rng.Int31n(1000000) + 1),
			int32(dateDimMinSk + int(rng.Int31n(int32(dateDimMaxSk/2)))),
			int32(dateDimMinSk + int(rng.Int31n(int32(dateDimMaxSk/2)))),
			salutations[sk%len(salutations)],
			fmt.Sprintf("%-20s", fn),
			fmt.Sprintf("%-30s", ln),
			"Y",
			int32(rng.Int31n(28) + 1),
			int32(rng.Int31n(12) + 1),
			int32(1940 + rng.Int31n(80)),
			fmt.Sprintf("%-20s", countries[sk%len(countries)]),
			fmt.Sprintf("%-13s", fmt.Sprintf("%s%d", fn[:2], sk)),
			fmt.Sprintf("%-50s", fmt.Sprintf("%s.%s@example.com", strings.ToLower(fn), strings.ToLower(ln))),
			int32(dateDimMinSk + int(rng.Int31n(int32(dateDimMaxSk)))),
		}, nil
	})
}

// ---------- store_sales ----------

func storeSalesColumns() []string {
	return []string{
		"ss_sold_date_sk", "ss_sold_time_sk",
		"ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk",
		"ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number",
		"ss_quantity", "ss_wholesale_cost", "ss_list_price", "ss_sales_price",
		"ss_ext_discount_amt", "ss_ext_sales_price", "ss_ext_wholesale_cost",
		"ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt",
		"ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit",
	}
}

func generateStoreSalesSource(rng *rand.Rand) pgx.CopyFromSource {
	return pgx.CopyFromSlice(StoreSalesCount, func(i int) ([]any, error) {
		itemSk := int32(rng.Int31n(int32(ItemCount)) + 1)
		ticket := int64(i + 1)
		custSk := int32(rng.Int31n(int32(CustomerCount)) + 1)
		storeSk := int32(rng.Int31n(int32(StoreCount)) + 1)
		dateSk := int32(dateDimMinSk + int(rng.Int31n(int32(dateDimMaxSk))))
		qty := int32(rng.Int31n(100) + 1)
		wholesale := math.Round((1.00+rng.Float64()*99.00)*100) / 100
		listPrice := math.Round(wholesale*(1.1+rng.Float64()*0.5)*100) / 100
		salesPrice := math.Round(listPrice*(0.7+rng.Float64()*0.3)*100) / 100
		discount := math.Round(rng.Float64()*10.00*100) / 100
		coupon := math.Round(rng.Float64()*5.00*100) / 100
		extSales := math.Round(float64(qty)*salesPrice*100) / 100
		extWholesale := math.Round(float64(qty)*wholesale*100) / 100
		extList := math.Round(float64(qty)*listPrice*100) / 100
		extTax := math.Round(extSales*0.08*100) / 100
		netPaid := math.Round((extSales-discount)*100) / 100
		netPaidIncTax := math.Round((netPaid+extTax)*100) / 100
		netProfit := math.Round((netPaid-extWholesale)*100) / 100

		return []any{
			dateSk,
			int32(rng.Int31n(86400)),
			itemSk,
			custSk,
			int32(rng.Int31n(1920800) + 1),
			int32(rng.Int31n(7200) + 1),
			int32(rng.Int31n(1000000) + 1),
			storeSk,
			int32(rng.Int31n(1000) + 1),
			ticket,
			qty,
			decimalVal(wholesale),
			decimalVal(listPrice),
			decimalVal(salesPrice),
			decimalVal(discount),
			decimalVal(extSales),
			decimalVal(extWholesale),
			decimalVal(extList),
			decimalVal(extTax),
			decimalVal(coupon),
			decimalVal(netPaid),
			decimalVal(netPaidIncTax),
			decimalVal(netProfit),
		}, nil
	})
}

// decimalVal converts a float64 to pgtype.Numeric with 2 decimal places.
func decimalVal(v float64) pgtype.Numeric {
	cents := int64(math.Round(v * 100))
	return pgtype.Numeric{
		Int:   big.NewInt(cents),
		Exp:   -2,
		Valid: true,
	}
}
