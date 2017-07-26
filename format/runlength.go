// Copyright 2015-2017 trivago GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package format

import (
	"github.com/trivago/gollum/core"
	"strconv"
)

// Runlength formatter plugin
//
// Runlength is a formatter that prepends the length of the message, followed by
// a ":". The actual message is formatted by a nested formatter.
//
// Parameters
//
// - Separator: This value is used as separator.
// By default this parameter is set to ":".
//
// - StoreRunlengthOnly: If this value is set to "true" the runlength only will stored.
// This option is useful to store the runlength only in a meta data field by the `ApplyTo` parameter.
// By default this parameter is set to "false".
//
// Examples
//
// In this example is the `format.Runlength` used as "subformatter" from the `format.MetadataCopy`.
// The `format.MetadataCopy` formatter copies the payload to the defined meta data field.
// At the end the `format.Runlength` formatter will transform the meta data value to the length.
//
//  exampleConsumer:
//    Type: consumer.Console
//    Streams: "*"
//    Modulators:
//      - format.MetadataCopy:
//          WriteTo:
//            - original_length:
//              - format.Runlength:
//                  Separator: ""
// 	                StoreRunlengthOnly: true
//
type Runlength struct {
	core.SimpleFormatter `gollumdoc:"embed_type"`
	separator            []byte `config:"Separator" default:":"`
	storeRunlengthOnly   bool   `config:"StoreRunlengthOnly" default:"false"`
}

func init() {
	core.TypeRegistry.Register(Runlength{})
}

// Configure initializes this formatter with values from a plugin config.
func (format *Runlength) Configure(conf core.PluginConfigReader) {
}

// ApplyFormatter update message payload
func (format *Runlength) ApplyFormatter(msg *core.Message) error {
	content := format.GetAppliedContent(msg)
	contentLen := len(content)
	lengthStr := strconv.Itoa(contentLen)

	var payload []byte
	if format.storeRunlengthOnly == false {
		dataSize := len(lengthStr) + len(format.separator) + contentLen
		payload = core.MessageDataPool.Get(dataSize)

		offset := copy(payload, []byte(lengthStr))
		offset += copy(payload[offset:], format.separator)
		copy(payload[offset:], content)
	} else {
		payload = []byte(lengthStr)
	}

	format.SetAppliedContent(msg, payload)
	return nil
}
