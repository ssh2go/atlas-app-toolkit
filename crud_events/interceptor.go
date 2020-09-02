package crud_events

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/infobloxopen/protoc-gen-gorm/types"
	"reflect"
	"strings"

	dapr "github.com/dapr/go-sdk/dapr/proto/runtime/v1"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Message_info struct {
	EncodedFile   []byte // probably it makes sense to change []byte to JSONValue?
	ApplicationId string
	PackageName   string
	MessageName   string
	Version       int32
}
type Encoded_data struct {
	MessageUUID types.UUID
	Reference   string
	EncodedData interface{}
}

func UnaryServerInterceptor(applicationId, pubsubname, topic string, handleOnlySuccessful bool, referensceList interface{}, client dapr.DaprClient) grpc.UnaryServerInterceptor {
	// 1. get the schema version from file
	if client == nil {
		logrus.Fatal("Dapr client is invalid")
	}
	//pflag.String("atlas.feature.flag.address", "0.0.0.0", "address of the feature flag service")
	//pflag.String("atlas.feature.flag.port", "9090", "port of the feature flag service")
	//pflag.Duration("muzzling.value.ttl", time.Minute*1, "TTL of cached value retrieved from the feature flag service")

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		handlerResponse, handlerError := handler(ctx, req)
		if handlerError != nil && handleOnlySuccessful {
			logrus.Errorf("CRUD event will not handled, because request was not completed successfully: %v\n", handlerError)
			return handlerResponse, handlerError
		}
		// 2. encode the data from this request, because we are not able to send interface directly
		bufferEvent := bytes.Buffer{}
		encodedMessage := gob.NewEncoder(&bufferEvent)
		err := encodedMessage.Encode(req)
		if err != nil {
			logrus.Errorf("Event request encoding error: %v\n", err)
			return handlerResponse, handlerError
		}
		typeRequest := reflect.TypeOf(req).String()
		typeRequest = strings.ReplaceAll(typeRequest, "*", "")
		logrus.Infof("Request type is %s, data size is %v", typeRequest, bufferEvent.Len())
		// 3. fill the outcoming struct with encoded data and encode the struct before sending
		fqn := fmt.Sprintf("%s.%s", applicationId, typeRequest)
		messageDescription, err := referensceList.(IMessages).GetMessageDescription(fqn)
		if err != nil {
			return handlerResponse, handlerError
		}

		toDapr := Encoded_data{
			MessageUUID: types.UUID{Value: messageDescription.MessageId},
			Reference:   messageDescription.MessageReference,
			EncodedData: bufferEvent.Bytes(),
		}
		bufferData := bytes.Buffer{}
		encodedData := gob.NewEncoder(&bufferData)
		err = encodedData.Encode(toDapr)
		if err != nil {
			logrus.Errorf("Event data encoding failed: %v", err)
		} else {
			encodedBuf := bufferData.Bytes()
			// 4. send the encoded struct to dapr
			err = publish(pubsubname, topic, encodedBuf, client)
			if err != nil {
				logrus.Errorf("Event data sending failed: %v", err)
			}
		}
		return handlerResponse, handlerError
	}
}
