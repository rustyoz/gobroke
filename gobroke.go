package main

import (
	"flag"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/rustyoz/gobroke/mqtt"
	"net"
	"os"
	"runtime/debug"
)

type CmdFunc func(mqtt *mqtt.Mqtt, conn *net.Conn, client **mqtt.ClientRep)

var g_debug = flag.Bool("d", false, "enable debugging log")
var g_port = flag.Int("p", 1883, "port of the broker to listen")
var g_redis_port = flag.Int("r", 6379, "port of the broker to listen")

var g_bolt_file = flag.String("b", "bolt.db", "database file")

var g_bolt_db *mqtt.BoltDB = new(mqtt.BoltDB)

var g_cmd_route = map[uint8]CmdFunc{
	mqtt.CONNECT:     mqtt.HandleConnect,
	mqtt.PUBLISH:     mqtt.HandlePublish,
	mqtt.SUBSCRIBE:   mqtt.HandleSubscribe,
	mqtt.UNSUBSCRIBE: mqtt.HandleUnsubscribe,
	mqtt.PINGREQ:     mqtt.HandlePingreq,
	mqtt.DISCONNECT:  mqtt.HandleDisconnect,
	mqtt.PUBACK:      mqtt.HandlePuback,
}

func handleConnection(conn *net.Conn) {
	remoteAddr := (*conn).RemoteAddr()
	var client *mqtt.ClientRep = nil

	defer func() {
		log.Debug("executing defered func in handleConnection")
		if r := recover(); r != nil {
			log.Debugf("got panic:(%s) will close connection from %s:%s", r, remoteAddr.Network(), remoteAddr.String())
			debug.PrintStack()
		}
		if client != nil {
			mqtt.ForceDisconnect(client, mqtt.G_clients_lock, mqtt.SEND_WILL)
		}
		(*conn).Close()
	}()

	var conn_str string = fmt.Sprintf("%s:%s", string(remoteAddr.Network()), remoteAddr.String())
	log.Debug("Got new conection", conn_str)
	for {
		// Read fixed header
		fixed_header, body := mqtt.ReadCompleteCommand(conn)
		if fixed_header == nil {
			log.Debug(conn_str, "reading header returned nil, will disconnect")
			return
		}

		mqtt_parsed, err := mqtt.DecodeAfterFixedHeader(fixed_header, body)
		if err != nil {
			log.Debug(conn_str, "read command body failed:", err.Error())
		}

		var client_id string
		if client == nil {
			client_id = ""
		} else {
			client_id = client.ClientId
		}
		log.Debugf("Got request: %s from %s", mqtt.MessageTypeStr(fixed_header.MessageType), client_id)
		proc, found := g_cmd_route[fixed_header.MessageType]
		if !found {
			log.Debugf("Handler func not found for message type: %d(%s)",
				fixed_header.MessageType, mqtt.MessageTypeStr(fixed_header.MessageType))
			return
		}
		proc(mqtt_parsed, conn, &client)
	}
}

func setup_logging() {
	level := "info"
	if *g_debug == true {
		level = "debug"
	}
	config := fmt.Sprintf(`
<seelog type="sync" minlevel="%s">
	<outputs formatid="main">
		<console/>
	</outputs>
	<formats>
		<format id="main" format="%%Date %%Time [%%LEVEL] %%File|%%FuncShort|%%Line: %%Msg%%n"/>
	</formats>
</seelog>`, level)

	logger, err := log.LoggerFromConfigAsBytes([]byte(config))

	if err != nil {
		fmt.Println("Failed to config logging:", err)
		os.Exit(1)
	}

	log.ReplaceLogger(logger)

	log.Info("Logging config is successful")
}

func main() {
	flag.Parse()

	setup_logging()

	g_bolt_db.OpenDatabase(g_bolt_file)
	mqtt.Bolt_db = g_bolt_db
	mqtt.RecoverFromBolt()
	log.Debugf("Gossipd kicking off, listening localhost:%d", *g_port)

	link, _ := net.Listen("tcp", fmt.Sprintf(":%d", *g_port))

	for {
		conn, err := link.Accept()
		if err != nil {
			continue
		}
		go handleConnection(&conn)
	}
	defer link.Close()
}
