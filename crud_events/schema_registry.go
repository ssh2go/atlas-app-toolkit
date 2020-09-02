package crud_events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	dapr "github.com/dapr/go-sdk/dapr/proto/runtime/v1"
)

type RegisteredMessages struct {
	sync.RWMutex
	Data map[string]MessageDescription
}

type MessageDetails struct {
	ApplicationId  string `protobuf:"bytes,1,opt,name=application_id,json=applicationId,proto3" json:"application_id,omitempty"`
	PackageName    string `protobuf:"bytes,2,opt,name=package_name,json=packageName,proto3" json:"package_name,omitempty"`
	MessageName    string `protobuf:"bytes,3,opt,name=message_name,json=messageName,proto3" json:"message_name,omitempty"`
	EncodedMessage []byte `protobuf:"bytes,4,opt,name=encoded_message,json=encodedMessage,proto3" json:"encoded_message,omitempty"`
	Version        int32  `protobuf:"varint,5,opt,name=version,proto3" json:"version,omitempty"`
}

type MessageDescription struct {
	MessageId            string
	MessageReference     string
	Version              int32
}

type IMessages interface {
	Init()
	ParseResponseToDescription(string, interface{}) error
	GetMessageDescription(string) (MessageDescription, error)
}

func (im* RegisteredMessages) Init() interface{} {
	im.Data = map[string]MessageDescription{}
	return im
}

func (im* RegisteredMessages) ParseResponseToDescription(key string, value interface{}) error {
	return fmt.Errorf("Method is not implemented")
}

func (im* RegisteredMessages) GetMessageDescription (key string) (MessageDescription, error) {
	im.RLock()
	defer im.RUnlock()
	messageDescription, ok := im.Data[key]
	if !ok {
		return MessageDescription{}, fmt.Errorf("Message %s was not registered", key)
	}
	return messageDescription, nil
}

type Registry struct {
	messages        interface{}
	registryAddress string
	messagebus      string
	schemaTopic     string
	daprClient      dapr.DaprClient
}

type SchemaRegistry interface {
	RegisterMessage(string, string, string) error
	SendProtobuf(string, string) error
	SendData(string, interface{}) error
}

// should only create valid Registry structure with filled schema registry address and dapr-related fields
func InitRegistry(address, daprMessagebus, messagesTopic string, client dapr.DaprClient, messages interface{}) *Registry {
	r := &Registry{
		messages,
		address,
		daprMessagebus,
		messagesTopic,
		client,
	}
	return r
}

func (r *Registry) RegisterMessage(applicationId, packageName, messageName, messageFilename string) (*RegisteredMessages, error) {
	if len(r.registryAddress) == 0 {
		return nil, fmt.Errorf("Storage address is empty")
	}

	messageJson, err := ioutil.ReadFile(messageFilename)
	if err != nil {
		return nil, err
	}

	req := MessageDetails{
		ApplicationId:  applicationId,
		PackageName:    packageName,
		MessageName:    messageName,
		EncodedMessage: messageJson,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal schema create request: %s", err)
	}

	rq, err := http.NewRequest("POST", fmt.Sprintf("%s/register_message", r.registryAddress), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	s2stoken := os.Getenv("SECRET_JWT")
	rq.Header.Set("Authorization", s2stoken)
	rq.Header.Set("Content-type", "application/json")
	httpClient := http.Client{Timeout: 10 * time.Second}
	response, err := httpClient.Do(rq)
	if err != nil {
		return nil, err
	}

	err = r.messages.(IMessages).ParseResponseToDescription(fmt.Sprintf("%s.%s.%s", applicationId, packageName, messageName), response)
	if err != nil {
		return nil, fmt.Errorf("Message %s (file %q) cannot be registered: error %s", messageName, messageFilename, err)
	}

	return nil, nil
}
