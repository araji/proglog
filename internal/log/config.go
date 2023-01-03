package log

type Config struct {
	// Segment is the configuration for log segments.
	Segment struct {
		MaxIndexBytes uint64
		MaxStoreBytes uint64
		InitialOffset uint64
	}
}
