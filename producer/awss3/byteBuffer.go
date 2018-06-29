// Copyright 2015-2018 trivago N.V.
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

package awss3

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
)

// s3ByteBuffer is a byte buffer used for s3 target objects
type s3ByteBuffer struct {
	position int64

	// Whether or not to compress this buffer.
	compress bool

	// Used for no compression
	bytes []byte

	// Used for compression.
	gzipWriter *gzip.Writer
	buffer     bytes.Buffer
}

func newS3ByteBuffer() *s3ByteBuffer {
	return &s3ByteBuffer{
		bytes:    make([]byte, 0),
		position: int64(0),
		compress: false,
	}
}

func newCompressedS3ByteBuffer() *s3ByteBuffer {
	var instance = s3ByteBuffer{
		position: int64(0),
		compress: true,
	}
	instance.gzipWriter = gzip.NewWriter(&instance.buffer)

	return &instance
}

func (buf *s3ByteBuffer) Bytes() ([]byte, error) {
	if buf.compress {
		return buf.buffer.Bytes(), nil
	} else {
		return buf.bytes, nil
	}
}

func (buf *s3ByteBuffer) CloseAndDelete() error {
	if buf.compress {
		if err := buf.gzipWriter.Flush(); err != nil {
			return err
		}
		if err := buf.gzipWriter.Close(); err != nil {
			return err
		}
	} else {
		buf.bytes = make([]byte, 0)
		buf.position = 0
	}
	return nil
}

func (buf *s3ByteBuffer) Read(p []byte) (n int, err error) {
	if buf.compress {
		n, err = buf.buffer.Read(p)
		if err != nil {
			return 0, err
		}
		return n, nil
	} else {
		n = copy(p, buf.bytes[buf.position:])
		buf.position += int64(n)
		if buf.position == int64(len(buf.bytes)) {
			return n, io.EOF
		}
		return n, nil
	}
}

func (buf *s3ByteBuffer) Write(p []byte) (n int, err error) {
	if buf.compress {
		var res, err = buf.gzipWriter.Write(p)
		if err != nil {
			return 0, err
		}
		return res, nil
	} else {
		buf.bytes = append(buf.bytes[:buf.position], p...)
		buf.position += int64(len(p))
		return len(p), nil
	}
}

func (buf *s3ByteBuffer) Seek(offset int64, whence int) (int64, error) {
	var position int64

	if buf.compress {
		switch whence {
		case 0:
			position = offset
		case 1:
			position = buf.position + offset
		case 2:
			position = int64(buf.buffer.Len()) + offset
		}
		if position < 0 {
			return 0, fmt.Errorf("S3Buffer bad seek result %d", position)
		}
		if position < buf.position {
			var diff = buf.position - position
			for diff > 0 {
				buf.buffer.UnreadByte()
				diff--
			}
		}
		if position > buf.position {
			var diff = position - buf.position
			for diff > 0 {
				buf.buffer.ReadByte()
				diff++
			}
		}
		buf.position = position
		return position, nil
	} else {
		switch whence {
		case 0: // io.SeekStart
			position = offset
		case 1: // io.SeekCurrent
			position = buf.position + offset
		case 2: // io.SeekEnd
			position = int64(len(buf.bytes)) + offset
		}
		if position < 0 {
			return 0, fmt.Errorf("S3Buffer bad seek result %d", position)
		}
		buf.position = position
		return position, nil
	}
}

func (buf *s3ByteBuffer) Size() (int, error) {
	if buf.compress {
		return buf.buffer.Len(), nil
	} else {
		return len(buf.bytes), nil
	}
}

func (buf *s3ByteBuffer) Sha1() (string, error) {
	if buf.compress {
		hash := sha1.Sum(buf.buffer.Bytes())
		return hex.EncodeToString(hash[:]), nil
	} else {
		hash := sha1.Sum(buf.bytes)
		return hex.EncodeToString(hash[:]), nil
	}
}
