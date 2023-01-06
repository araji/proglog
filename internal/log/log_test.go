package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	apiv1 "github.com/araji/proglog/api/v1"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read record succesfully":      testAppendRead,
		"append and read record many succesfully": testAppendReadMany,
		"offset out of range error":               testOutOfRangeErr,
		"init with existing segments":             testInitExisting,
		"reader":                                  testReader,
		"truncate":                                testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)

		})
	}
}

func testAppendReadMany(t *testing.T, log *Log) {

	for i := 0; i < 5; i++ {
		append := &apiv1.Record{
			Value: []byte(fmt.Sprintf("Hello World %d !", i)),
		}
		off, err := log.Append(append)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)

		read, err := log.Read(off)
		require.NoError(t, err)
		require.Equal(t, append.Value, read.Value)
	}

}

func testAppendRead(t *testing.T, log *Log) {
	append := &apiv1.Record{Value: []byte("hello world")}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(apiv1.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	append := &apiv1.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

}
func testReader(t *testing.T, log *Log) {
	append := &apiv1.Record{Value: []byte("hello world")}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &apiv1.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	append := &apiv1.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)

}
