package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

func main() {
	const outputPlugin = "pgoutput"
	const publicationName = "demo_pub"

	conn, err := pgconn.Connect(context.Background(), os.Getenv("CONN_STRING"))
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", publicationName)}
	result := conn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", publicationName))

	if _, err = result.ReadAll(); err != nil {
		log.Fatalln("drop publication if exists error", err)
	}

	result = conn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", publicationName))

	if _, err = result.ReadAll(); err != nil {
		log.Fatalln("create publication error", err)
	}
	log.Printf("create publication %s", publicationName)

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := "demo_slot"

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	set := NewRelationSet(nil)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

				logmsg, err := Parse(xld.WALData)
				if err != nil {
					log.Fatal("can't decode", err)
				}

				log.Println("Got event:", logmsg.msg())

				switch v := logmsg.(type) {
				case Relation:
					set.Add(v)
				case Insert:
					if err := set.Dump(v.RelationID, v.Row); err != nil {
						log.Fatal(err)
					}
				case Update:
					if err := set.Dump(v.RelationID, v.Row); err != nil {
						log.Fatal(err)
					}
				case Delete:
					if err := set.Dump(v.RelationID, v.Row); err != nil {
						log.Fatal(err)
					}
				}
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}

	}
}

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

func (d *decoder) bool() bool {
	x := d.buf.Next(1)[0]
	return x != 0

}

func (d *decoder) uint8() uint8 {
	x := d.buf.Next(1)[0]
	return x
}

func (d *decoder) uint16() uint16 {
	x := d.order.Uint16(d.buf.Next(2))
	return x
}

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		// TODO: Return an error
		panic(err)
	}
	return string(s[:len(s)-1])
}

func (d *decoder) uint32() uint32 {
	x := d.order.Uint32(d.buf.Next(4))
	return x

}

func (d *decoder) uint64() uint64 {
	x := d.order.Uint64(d.buf.Next(8))
	return x
}

func (d *decoder) int32() int32 { return int32(d.uint32()) }

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowinfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	} else {
		d.buf.UnreadByte()
		return false
	}
}

func (d *decoder) tupledata() []Tuple {
	size := int(d.uint16())
	data := make([]Tuple, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 'n':
		case 'u':
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = Tuple{Flag: 't', Value: d.buf.Next(vsize)}
		}
	}
	return data
}

func (d *decoder) columns() []Column {
	size := int(d.uint16())
	data := make([]Column, size)
	for i := 0; i < size; i++ {
		data[i] = Column{
			Key:  d.bool(),
			Name: d.string(),
			Type: d.uint32(),
			Mode: d.uint32(),
		}
	}
	return data
}

type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction. The value is in number of
	// microseconds since PostgreSQL epoch (2000-01-01).
	Timestamp time.Time
	// 	Xid of the transaction.
	XID int32
}

type Commit struct {
	Flags uint8
	// The final LSN of the transaction.
	LSN uint64
	// The final LSN of the transaction.
	TransactionLSN uint64
	Timestamp      time.Time
}

type Relation struct {
	// ID of the relation.
	ID uint32
	// Namespace (empty string for pg_catalog).
	Namespace string
	Name      string
	Replica   uint8
	Columns   []Column
}

func (r Relation) IsEmpty() bool {
	return r.ID == 0 && r.Name == "" && r.Replica == 0 && len(r.Columns) == 0
}

type Type struct {
	// ID of the data type
	ID        uint32
	Namespace string
	Name      string
}

type Insert struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	New bool
	Row []Tuple
}

type Update struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Old    bool
	Key    bool
	New    bool
	OldRow []Tuple
	Row    []Tuple
}

type Delete struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Key bool // TODO
	Old bool // TODO
	Row []Tuple
}

type Truncate struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
	Truncate uint8
	// ID of the relation corresponding to the ID in the relation message. This field is repeated for each relation.
	CorrecpondingRelation uint32
}

type Origin struct {
	LSN  uint64
	Name string
}

type DecoderValue interface {
	pgtype.TextDecoder
	pgtype.Value
}

type Column struct {
	Key  bool
	Name string
	Type uint32
	Mode uint32
}
type Tuple struct {
	Flag  int8
	Value []byte
}

type Message interface {
	msg() string
}

func (Begin) msg() string    { return "BEGIN" }
func (Relation) msg() string { return "RELATION" }
func (Update) msg() string   { return "UPDATE" }
func (Insert) msg() string   { return "INSERT" }
func (Delete) msg() string   { return "DELETE" }
func (Commit) msg() string   { return "COMMIT" }
func (Origin) msg() string   { return "ORIGIN" }
func (Truncate) msg() string { return "TRUNCATE" }
func (Type) msg() string     { return "TYPE" }

// Parse a logical replication message.
// See https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
func Parse(src []byte) (Message, error) {
	msgType := src[0]
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}
	switch msgType {
	case 'B':
		b := Begin{}
		b.LSN = d.uint64()
		b.Timestamp = d.timestamp()
		b.XID = d.int32()
		return b, nil
	case 'C':
		c := Commit{}
		c.Flags = d.uint8()
		c.LSN = d.uint64()
		c.TransactionLSN = d.uint64()
		c.Timestamp = d.timestamp()
		return c, nil
	case 'O':
		o := Origin{}
		o.LSN = d.uint64()
		o.Name = d.string()
		return o, nil
	case 'R':
		r := Relation{}
		r.ID = d.uint32()
		r.Namespace = d.string()
		r.Name = d.string()
		r.Replica = d.uint8()
		r.Columns = d.columns()
		return r, nil
	case 'Y':
		t := Type{}
		t.ID = d.uint32()
		t.Namespace = d.string()
		t.Name = d.string()
		return t, nil
	case 'I':
		i := Insert{}
		i.RelationID = d.uint32()
		i.New = d.uint8() > 0
		i.Row = d.tupledata()
		return i, nil
	case 'U':
		u := Update{}
		u.RelationID = d.uint32()
		u.Key = d.rowinfo('K')
		u.Old = d.rowinfo('O')
		if u.Key || u.Old {
			u.OldRow = d.tupledata()
		}
		u.New = d.uint8() > 0
		u.Row = d.tupledata()
		return u, nil
	case 'T':
		t := Truncate{}
		t.RelationID = d.uint32()
		t.Truncate = d.uint8()
		t.CorrecpondingRelation = d.uint32()
		return t, nil
	case 'D':
		dl := Delete{}
		dl.RelationID = d.uint32()
		dl.Key = d.rowinfo('K')
		dl.Old = d.rowinfo('O')
		dl.Row = d.tupledata()
		return dl, nil
	default:
		return nil, fmt.Errorf("Unknown message type for %s (%d)", []byte{msgType}, msgType)
	}
}

type RelationSet struct {
	// Mutex probably will be redundant as receiving
	// a replication stream is currently strictly single-threaded
	relations map[uint32]Relation
	connInfo  *pgtype.ConnInfo
}

// NewRelationSet creates a new relation set.
// Optionally ConnInfo can be provided, however currently we need some changes to pgx to get it out
// from ReplicationConn.
func NewRelationSet(ci *pgtype.ConnInfo) *RelationSet {
	return &RelationSet{map[uint32]Relation{}, ci}
}

func (rs *RelationSet) Add(r Relation) {
	rs.relations[r.ID] = r
}

func (rs *RelationSet) Dump(relation uint32, row []Tuple) error {
	rel, ok := rs.Get(relation)
	if !ok {
		return fmt.Errorf("can't find relation with ID: %d", relation)
	}

	values, err := rs.Values(relation, row)
	if err != nil {
		return fmt.Errorf("error parsing values: %s", err)
	}

	for name, value := range values {
		val := value.Get()
		log.Printf("[Table: %s] %s (%T): %#v", rel.Name, name, val, val)
	}
	return nil
}

func (rs *RelationSet) Get(ID uint32) (r Relation, ok bool) {
	r, ok = rs.relations[ID]
	return
}

func (rs *RelationSet) Values(id uint32, row []Tuple) (map[string]pgtype.Value, error) {
	values := map[string]pgtype.Value{}
	rel, ok := rs.Get(id)
	if !ok {
		return values, fmt.Errorf("no relation for %d", id)
	}

	// assert same number of row and columns
	for i, tuple := range row {
		col := rel.Columns[i]
		decoder := col.Decoder()

		if err := decoder.DecodeText(rs.connInfo, tuple.Value); err != nil {
			return nil, fmt.Errorf("error decoding tuple %d: %s", i, err)
		}

		values[col.Name] = decoder
	}

	return values, nil
}

func (c Column) Decoder() DecoderValue {
	switch c.Type {
	case pgtype.ACLItemArrayOID:
		return &pgtype.ACLItemArray{}
	case pgtype.ACLItemOID:
		return &pgtype.ACLItem{}
	case pgtype.BoolArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.ByteaArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.ByteaOID:
		return &pgtype.Bytea{}
	case pgtype.CIDOID:
		return &pgtype.CID{}
	case pgtype.CIDRArrayOID:
		return &pgtype.CIDRArray{}
	case pgtype.CIDROID:
		return &pgtype.CIDR{}
	// case pgtype.CharOID:
	// 	// Not all possible values of QChar are representable in the text format
	// 	return &pgtype.Unknown{}
	case pgtype.DateArrayOID:
		return &pgtype.DateArray{}
	case pgtype.DateOID:
		return &pgtype.Date{}
	case pgtype.Float4ArrayOID:
		return &pgtype.Float4Array{}
	case pgtype.Float4OID:
		return &pgtype.Float4{}
	case pgtype.Float8ArrayOID:
		return &pgtype.Float8Array{}
	case pgtype.Float8OID:
		return &pgtype.Float8{}
	case pgtype.InetArrayOID:
		return &pgtype.InetArray{}
	case pgtype.InetOID:
		return &pgtype.Inet{}
	case pgtype.Int2ArrayOID:
		return &pgtype.Int2Array{}
	case pgtype.Int2OID:
		return &pgtype.Int2{}
	case pgtype.Int4ArrayOID:
		return &pgtype.Int4Array{}
	case pgtype.Int4OID:
		return &pgtype.Int4{}
	case pgtype.Int8ArrayOID:
		return &pgtype.Int8Array{}
	case pgtype.Int8OID:
		return &pgtype.Int8{}
	case pgtype.JSONBOID:
		return &pgtype.JSONB{}
	case pgtype.JSONOID:
		return &pgtype.JSON{}
	case pgtype.NameOID:
		return &pgtype.Name{}
	case pgtype.OIDOID:
		// pgtype.OID does not implement the value interface
		return &pgtype.Unknown{}
	case pgtype.RecordOID:
		// The text format output format for Records does not include type
		// information and is therefore impossible to decode
		return &pgtype.Unknown{}
	case pgtype.TIDOID:
		return &pgtype.TID{}
	case pgtype.TextArrayOID:
		return &pgtype.TextArray{}
	case pgtype.TextOID:
		return &pgtype.Text{}
	case pgtype.TimestampArrayOID:
		return &pgtype.TimestampArray{}
	case pgtype.TimestampOID:
		return &pgtype.Timestamp{}
	case pgtype.TimestamptzArrayOID:
		return &pgtype.TimestamptzArray{}
	case pgtype.TimestamptzOID:
		return &pgtype.Timestamptz{}
	case pgtype.UUIDOID:
		return &pgtype.UUID{}
	case pgtype.UnknownOID:
		return &pgtype.Unknown{}
	case pgtype.VarcharArrayOID:
		return &pgtype.VarcharArray{}
	case pgtype.VarcharOID:
		return &pgtype.Varchar{}
	case pgtype.XIDOID:
		return &pgtype.XID{}
	default:
		// panic(fmt.Sprintf("unknown OID type %d", c.Type))
		return &pgtype.Unknown{}
	}
}
