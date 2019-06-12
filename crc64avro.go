package goavro

// crc64Empty is a constant used to initialize the crc64Table, and to compute
// the CRC-64-AVRO fingerprint of every object schema.
const crc64Empty = uint64(0xc15d213aa4d7a795)

// crc64Table is never modified after initialization but its values are read to
// compute the CRC-64-AVRO fingerprint of every schema its given.
var crc64Table [256]uint64

func init() {
	// This is the only place where crc64Table is modified, and it only happens
	// during this initialization step.
	for i := uint64(0); i < 256; i++ {
		fp := i
		for j := 0; j < 8; j++ {
			fp = (fp >> 1) ^ (crc64Empty & -(fp & 1)) // unsigned right shift >>>
		}
		crc64Table[i] = fp
	}
}

// crc64Avro returns an unsigned 64-bit integer Rabin fingerprint for buf.
func crc64Avro(buf []byte) uint64 {
	fp := crc64Empty
	for i := 0; i < len(buf); i++ {
		fp = (fp >> 8) ^ crc64Table[(byte(fp)^buf[i])&0xff] // unsigned right shift >>>
	}
	return fp
}
