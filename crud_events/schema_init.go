package crud_events

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	dapr "github.com/dapr/go-sdk/dapr/proto/runtime/v1"
	"github.com/sirupsen/logrus"
)

var (
	errorStoragenameIsEmpty   = errors.New("Storage name is empty")
	errorAddressIsUnreachable = errors.New("Address is unreacheable")
)

func publish(pubsubname, topic string, dat []byte, client dapr.DaprClient) error {
	if client == nil {
		return errors.New("Client is not initialized")
	}

	_, err := client.PublishEvent(context.Background(), &dapr.PublishEventRequest{
		PubsubName: pubsubname,
		Topic:      topic,
		Data:       dat,
	})
	return err
}

// Just an example how incoming protobuf might be processed
func MessageJSONReader(body []byte) error {
	receivedInfo := &Message_info{}
	b := bytes.Buffer{}
	b.Write(body)
	d := gob.NewDecoder(&b)
	err := d.Decode(&receivedInfo)
	if err != nil {
		return err
	}

	logrus.Debugf("Received message %q should be used in application %s, package %s. Encoded file is %#v", receivedInfo.MessageName, receivedInfo.ApplicationId, receivedInfo.PackageName, receivedInfo.EncodedFile)
	return nil
}
