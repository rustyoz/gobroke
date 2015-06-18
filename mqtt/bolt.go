// FIXME: handle disconnection from redis

package mqtt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
)

type BoltDB struct {
	*bolt.DB
}

var world = []byte("mqtt")

func (db *BoltDB) Store(key string, value interface{}) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		panic(fmt.Sprintf("gob encoding failed, error:(%s)", err))
	}

	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(world)
		if err != nil {
			return err
		}

		err = bucket.Put([]byte(key), buf.Bytes())
		if err != nil {
			return err
		}
		return nil
	})

	log.Debugf("stored to bolt, key=%s, val=%s", key, value)

}

func (db *BoltDB) Fetch(key string, value interface{}) int {
	var val []byte
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(world))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", world)
		}

		val = bucket.Get([]byte(key))
		if val == nil {
			return fmt.Errorf("Key not found: %q", key)
		}
		return nil
	})
	if err != nil {
		//log.Debugf("%s", err)
		return 1
	}

	buf := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(value)

	if err != nil {
		panic(fmt.Sprintf("gob decode failed, key=(%s), value=(%s), error:%s", key, val, err))
	}
	return 0
}

func (db *BoltDB) Delete(key string) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(world))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", world)
		}

		bucket.Delete([]byte(key))
		return nil
	})
	if err != nil {
		panic(err)
	}

}

func (db *BoltDB) GetSubsClients() []string {

	clients := make([]string, 0)

	db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(world)).Cursor()

		prefix := []byte("gobroke.client-subs.")
		for key, _ := c.Seek(prefix); bytes.HasPrefix(key, prefix); key, _ = c.Next() {
			clients = append(clients, string(key))
			fmt.Printf("here\n")
		}

		return nil
	})

	return clients
}

func (db *BoltDB) GetRetainMessage(topic string) *MqttMessage {
	msg := new(MqttMessage)
	key := fmt.Sprintf("gobroke.topic-retained.%s", topic)
	var internal_id uint64

	ret := db.Fetch(key, &internal_id)
	if ret != 0 {
		log.Debugf("retained message internal id not found in redis for topic(%s)", topic)
		return nil
	}

	key = fmt.Sprintf("gobroke.mqtt-msg.%d", internal_id)
	ret = db.Fetch(key, &msg)
	if ret != 0 {
		log.Debugf("retained message, though internal id found, not found in redis for topic(%s)", topic)
		return nil
	}
	return msg
}

func (db *BoltDB) SetRetainMessage(topic string, msg *MqttMessage) {
	key := fmt.Sprintf("gobroke.topic-retained.%s", topic)
	internal_id := msg.InternalId
	db.Store(key, internal_id)
}

func (db *BoltDB) GetFlyingMessagesForClient(client_id string) *map[uint16]FlyingMessage {
	key := fmt.Sprintf("gobroke.client-msg.%s", client_id)
	messages := make(map[uint16]FlyingMessage)
	db.Fetch(key, &messages)
	return &messages
}

func (db *BoltDB) SetFlyingMessagesForClient(client_id string,
	messages *map[uint16]FlyingMessage) {
	key := fmt.Sprintf("gobroke.client-msg.%s", client_id)
	db.Store(key, messages)
}

func (db *BoltDB) RemoveAllFlyingMessagesForClient(client_id string) {
	key := fmt.Sprintf("gobroke.client-msg.%s", client_id)
	db.Delete(key)
}

func (db *BoltDB) AddFlyingMessage(dest_id string,
	fly_msg *FlyingMessage) {

	messages := *db.GetFlyingMessagesForClient(dest_id)

	messages[fly_msg.ClientMessageId] = *fly_msg

	db.SetFlyingMessagesForClient(dest_id, &messages)

	log.Debugf("Added flying message to boltdb:(%s), message_id:(%d)",
		dest_id, fly_msg.ClientMessageId)
}

func (db *BoltDB) IsFlyingMessagePendingAck(client_id string, message_id uint16) bool {
	messages := db.GetFlyingMessagesForClient(client_id)

	flying_msg, found := (*messages)[message_id]

	return found && flying_msg.Status == PENDING_ACK
}
