package hllpp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
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
	PipelineSparseClean   = 'S'
	PipelineSparseDirty   = 's'

	// Private constants.
	pipelineBitsPerRegister = 6

	// Technically, PipelineDB supports up to 8192 explicit registers before
	// converting to sparse representation. But we lower this because of
	// observed problems with explicit HLLs at medium cardinalities.
	maxExplicitRegisters = 600

	// Feature flags.

	// Whether to always write 'dense' PipelineDB HLLs. Currently, this is true
	// because I observe cardinality / union issues when taking the union of
	// explicit PipelineDB HLLs that were converted from Retailnext.
	alwaysWriteDense = false
)

var (
	// Whether to mark the cardinality calculation as clean or dirty. Clean is
	// better for production, but dirty is better for testing.
	WriteDirtyEncoding = false
)

// Converts dense or sparse data structure to Pipeline padded struct.
func (h *HLLPP) AsPipeline() ([]byte, error) {
	if !h.sparse || alwaysWriteDense {
		return h.convertToDense()
	} else {
		return h.convertToExplicit()
	}
}

func (h *HLLPP) convertToDense() ([]byte, error) {
	p := pipelineHLL{
		Card: h.Count(),
		Mlen: 1 + h.m*pipelineBitsPerRegister/8,
		P:    h.p,
	}
	if WriteDirtyEncoding {
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

func (h *HLLPP) convertToExplicit() ([]byte, error) {
	if h.sparse {
		h.flushTmpSet()
	}

	// Read registers out of Retailnext implementation and write to whichever
	// format we selected above.
	var registers RegisterSlice
	for it := newRegIterator(h); !it.done(); {
		reg, val := it.next()
		registers = append(registers, Register{reg, val})

		if len(registers) >= maxExplicitRegisters {
			// If the number of registers exceeds the maximum for PipelineDB's
			// EXPLICIT mode, stop now and fall backt to DENSE. Technically, we
			// should go to SPARSE to match PipelineDB's behavior.
			return h.convertToDense()
		}
	}

	var regBuf bytes.Buffer
	sort.Sort(registers)
	for i := range registers {
		packed := uint32(registers[i].Index<<8) | uint32(registers[i].Value&0xff)
		if err := binary.Write(&regBuf, binary.LittleEndian, packed); err != nil {
			return nil, err
		}
	}

	p := pipelineHLL{
		Card: h.Count(),
		Mlen: uint32(regBuf.Len()),
		P:    h.p,
	}
	if WriteDirtyEncoding {
		p.Encoding = PipelineExplicitDirty
	} else {
		p.Encoding = PipelineExplicitClean
	}

	// Write the size-invariant preamble.
	var ret bytes.Buffer
	if err := binary.Write(&ret, binary.LittleEndian, &p); err != nil {
		return nil, err
	}

	// Write the M register payload.
	n, err := ret.Write(regBuf.Bytes())
	if err != nil {
		return nil, err
	} else if n != regBuf.Len() {
		return nil, fmt.Errorf("short register buffer %d != %d", n, regBuf.Len())
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
