// +build integration

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
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/ctypes"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/Shopify/sarama.v1"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

var mockMetricType = plugin.MetricType{
	Namespace_:   core.NewNamespace("mock", "foo"),
	Data_:        1,
	Timestamp_:   time.Now(),
	Description_: "mock metric",
	Unit_:        "kB",
}

// integration test
func TestPublish(t *testing.T) {
	// This integration test requires KAFKA_BROKERS
	if os.Getenv("SNAP_TEST_KAFKA") == "" {
		fmt.Println("Skipping integration")
		return
	}
	brokers := os.Getenv("SNAP_TEST_KAFKA")

	// Pick a random topic
	topic := fmt.Sprintf("%d", time.Now().Nanosecond())
	fmt.Printf("Topic: %s\n", topic)
	config := make(map[string]ctypes.ConfigValue)
	Convey("Publish to Kafka", t, func() {
		k := NewKafkaPublisher()
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		Convey("publish mock metrics and consume", func() {
			contentType := plugin.SnapGOBContentType
			mts := []plugin.MetricType{
				mockMetricType,
			}

			enc.Encode(mts)

			// set config items
			config["brokers"] = ctypes.ConfigValueStr{Value: brokers}
			config["topic"] = ctypes.ConfigValueStr{Value: topic}

			// Get validated policy
			cp, cp_err := k.GetConfigPolicy()
			Convey("validatation of config policy", func() {
				So(cp_err, ShouldBeNil)
				So(cp, ShouldNotBeNil)
			})

			// return a ConfigPolicyNode
			cfg, _ := cp.Get([]string{""}).Process(config)
			Convey("validatation of returned ConfigPolicyNode", func() {
				So(cfg, ShouldNotBeNil)
			})

			Convey("send the message", func() {
				err := k.Publish(contentType, buf.Bytes(), *cfg)
				So(err, ShouldBeNil)
			})

			Convey("verify data published to Kafka", func() {
				m, err := consumer(topic, brokers)
				So(err, ShouldBeNil)
				So(m.Value, ShouldNotBeNil)

				Convey("check if marshalled metrics and published data to Kafka are equal", func() {
					metricsAsJson, err := json.Marshal(formatMetricTypes(mts))
					So(err, ShouldBeNil)
					So(string(m.Value), ShouldEqual, string(metricsAsJson))
				})
			})
		})
	})
}

func consumer(topic, brokers string) (*sarama.ConsumerMessage, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true // Handle errors manually instead of letting Sarama log them.

	master, err := sarama.NewConsumer([]string{brokers}, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		master.Close()
	}()

	consumer, err := master.ConsumePartition(topic, 0, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		consumer.Close()
	}()

	msgCount := 0
	timeout := make(chan bool, 1)

	go func() {
		time.Sleep(time.Millisecond * 500)
		timeout <- true
	}()

	select {
	case err := <-consumer.Errors():
		return nil, err
	case m := <-consumer.Messages():
		msgCount++
		return m, nil
	case <-timeout:
		return nil, errors.New("timed out waiting for produced message")
	}
}
