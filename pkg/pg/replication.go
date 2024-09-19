package pg

// For each truck:
// 1. Check if replication slot exists, and create it if it doesn't
// 1.2. Backfill data if it's a new slot
// 2. Start replication
//
// We need to keep some kind of global state to keep a connection pool for each database we're connecting to

// func startReplication(conn *pgconn.PgConn, slotName string, outputPlugin string) {
// 	// Check if replication slot exists
// 	// Create replication slot if it doesn't
// 	// Backfill data if it's a new slot
// 	createReplicationSlot(conn, slotName, outputPlugin)

// 	// Start replication
// 	startReplicationStream(conn, slotName)
// }

// func createReplicationSlot(conn *pgconn.PgConn, slotName string, outputPlugin string) {
// 	// Check if replication slot exists
// 	// Create replication slot if it doesn't
// 	// Backfill data if it's a new slot
// 	_, err := conn.Exec("SELECT * FROM pg_create_logical_replication_slot($1, $2)", slotName, outputPlugin)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }

// func startReplicationStream(conn *pgconn.PgConn, slotName string) {
// 	// Start replication
// 	// Create replication slot if it doesn't
// 	_, err := conn.Exec("START_REPLICATION SLOT $1 LOGICAL 0/0", slotName)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }

// func readReplicationStream(conn *pgconn.PgConn) {
// 	var replicationMessage pgproto3.FrontendMessage
// 	var err error

// 	for {
// 		replicationMessage, err = conn.ReceiveMessage()
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		switch replicationMessage.(type) {
// 		case *pgproto3.CopyData:
// 			copyData := replicationMessage.(*pgproto3.CopyData)
// 			// Process replication message
// 			processReplicationMessage(copyData.Data)
// 		default:
// 			log.Fatalf("Unhandled replication message: %T", replicationMessage)
// 		}
// 	}
// }

// func processReplicationMessage(data []byte) {
// 	// Parse replication message
// 	// Process replication message
// 	// Write to Kafka
// }
