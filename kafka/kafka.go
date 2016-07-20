/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"strings"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"

	"gopkg.in/Shopify/sarama.v1"

)

const (
	PluginName    = "kafka"
	PluginVersion = 7
	PluginType    = plugin.PublisherPluginType
)

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(PluginName, PluginVersion, PluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

type kafkaPublisher struct{}

func NewKafkaPublisher() *kafkaPublisher {
	return &kafkaPublisher{}
}
type Metric struct{
	Namespace string 		`json:"namespace"`
	LastAdvertisedTime time.Time 	`json:"last_advertised_time"`
	Data interface{} 		`json:"data"`
	Unit string `json:"unit"`
	Tags map[string]string `json:"tags"`
	Timestamp time.Time `json:"timestamp"`
}

// Publish sends data to a Kafka server
func (k *kafkaPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	var mts []plugin.MetricType
	var metrics []Metric

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&mts); err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid incoming content: %v, err=%v", content, err)
			return fmt.Errorf("Cannot decode incoming content, err=%v", err)
		}
	default:
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}


	for _, mt := range mts {
		metrics = append(metrics, Metric{
			Namespace: mt.Namespace().String(),
			LastAdvertisedTime: mt.LastAdvertisedTime(),
			Data: mt.Data(),
			Unit: mt.Unit(),
			Tags: mt.Tags(),
			Timestamp: mt.Timestamp(),
		})
	}

	jsonOut, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("Cannot marshal metrics to JSON format, err=%v", err)
	}

	topic := config["topic"].(ctypes.ConfigValueStr).Value
	brokers := parseBrokerString(config["brokers"].(ctypes.ConfigValueStr).Value)

	return k.publish(topic, brokers, []byte(jsonOut))
}

func (k *kafkaPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewStringRule("topic", false, "snap")
	handleErr(err)
	r1.Description = "Kafka topic for publishing"

	r2, _ := cpolicy.NewStringRule("brokers", false, "localhost:9092")
	handleErr(err)
	r2.Description = "List of brokers separated by semicolon in the format: <broker-ip:port;broker-ip:port> (ex: \"192.168.1.1:9092;172.16.9.99:9092\")"

	config.Add(r1, r2)
	cp.Add([]string{""}, config)
	return cp, nil
}

// Internal method after data has been converted to serialized bytes to send
func (k *kafkaPublisher) publish(topic string, brokers []string, content []byte) error {
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return fmt.Errorf("Cannot initialize a new Sarama SyncProducer using the given broker addresses (%v), err=%v", brokers, err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(content),
	})
	return err
}

func parseBrokerString(brokerStr string) []string {
	// remove spaces from 'brokerStr'
	brokers := strings.Replace(brokerStr, " ", "", -1)

	// return split brokers separated by semicolon
	return strings.Split(brokers, ";")
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
