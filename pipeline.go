package hllpp

import (
	"bytes"
	"encoding/binary"
)

// Padded PipelineDB HyperLogLog struct observed on database-2 (0.8.5, x64)
type pipelineHLL struct {
	Encoding byte
	_        [3]byte
	Card     uint64
	P        uint8
	_        [3]byte
	Mlen     uint32
	// M is variable-length, and must be dealt with separately.
}

const (
	// Public constants.
	PipelineDenseDirty    = 'd'
	PipelineDenseClean    = 'D'
	PipelineExplicitDirty = 'e'
	PipelineExplicitClean = 'E'

	// Private constants.
	pipelineBitsPerRegister = 6

	// Feature flags.

	// Whether to always write 'dense' PipelineDB HLLs. Currently, this is true
	// because I observe cardinality / union issues when taking the union of
	// explicit PipelineDB HLLs that were converted from Retailnext.
	alwaysWriteDense = true

	// Whether to mark the cardinality calculation as clean or dirty. Clean is
	// better for production, but dirty is better for testing.
	writeDirtyEncoding = false
)

// Converts dense or sparse data structure to Pipeline padded struct.
func (h *HLLPP) AsPipeline() ([]byte, error) {
	if !h.sparse || alwaysWriteDense {
		return h.convertToDense()
	} else {
		return h.convertToSparse()
	}
}

func (h *HLLPP) convertToDense() ([]byte, error) {
	p := pipelineHLL{
		Card: h.Count(),
		Mlen: 1 + h.m*pipelineBitsPerRegister/8,
		P:    h.p,
	}
	if writeDirtyEncoding {
		p.Encoding = PipelineDenseDirty
	} else {
		p.Encoding = PipelineDenseClean
	}

	// Write the size-invariant preamble.
	var ret bytes.Buffer
	if err := binary.Write(&ret, binary.LittleEndian, &p); err != nil {
		return nil, err
	}

	// Read registers out of Retailnext implementation and write to whichever
	// format we selected above.
	data := make([]byte, p.Mlen)
	for it := newRegIterator(h); !it.done(); {
		reg, val := it.next()
		setDensePipelineRegister(data, reg, val)
	}

	_, err := ret.Write(data)
	return ret.Bytes(), err
}

func (h *HLLPP) convertToSparse() ([]byte, error) {
	p := pipelineHLL{
		Card: h.Count(),
		Mlen: 4 * h.sparseLength,
		P:    h.p,
	}
	if writeDirtyEncoding {
		p.Encoding = PipelineExplicitDirty
	} else {
		p.Encoding = PipelineExplicitClean
	}

	// Write the size-invariant preamble.
	var ret bytes.Buffer
	if err := binary.Write(&ret, binary.LittleEndian, &p); err != nil {
		return nil, err
	}

	// Read registers out of Retailnext implementation and write to whichever
	// format we selected above.
	for it := newRegIterator(h); !it.done(); {
		reg, val := it.next()
		binary.Write(&ret, binary.LittleEndian, uint32(reg<<8)|uint32(val&0xff))
	}

	return ret.Bytes(), nil
}

// Straight port of HLL_DENSE_SET_REGISTER macro. dense.go's setRegister is
// subtly different in ways yet to be determined.
func setDensePipelineRegister(_p []byte, regnum uint32, val uint8) {
	var _byte uint32 = regnum * pipelineBitsPerRegister / 8
	var _fb uint32 = regnum * pipelineBitsPerRegister & 7
	var _fb8 uint32 = 8 - _fb
	var _v uint32 = uint32(val)

	var hllRegisterMax uint32 = (1 << pipelineBitsPerRegister) - 1
	_p[_byte] &= uint8(^(hllRegisterMax << _fb))
	_p[_byte] |= uint8(_v << _fb)
	_p[_byte+1] &= uint8(^(hllRegisterMax >> _fb8))
	_p[_byte+1] |= uint8(_v >> _fb8)
}

// Backing-agnostic iterator of HLL registers.
type regIterator struct {
	hll *HLLPP
	sr  *sparseReader
	idx uint32
}

func newRegIterator(h *HLLPP) *regIterator {
	it := regIterator{hll: h}
	if h.sparse {
		it.sr = newSparseReader(h.data)
	}
	return &it
}

func (i *regIterator) done() bool {
	if i.hll.sparse {
		return i.sr.Done()
	} else {
		return i.idx == i.hll.m
	}
}

func (i *regIterator) next() (reg uint32, val uint8) {
	if i.hll.sparse {
		return i.hll.decodeHash(i.sr.Next(), i.hll.p)
	} else {
		reg, val := i.idx, getRegister(i.hll.data, i.hll.bitsPerRegister, i.idx)
		i.idx++
		return reg, val
	}
}
