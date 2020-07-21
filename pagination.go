package bom

import "math"

// Pagination data
type Pagination struct {
	TotalCount  int32
	TotalPages  int32
	CurrentPage int32
	Size        int32
}

// NewPagination create pagination
func NewPagination(page int32, size int32) *Pagination {
	pg := new(Pagination)
	if page <= 0 {
		page = 1
	}
	pg.CurrentPage = page
	pg.Size = size
	return pg
}

// WithTotal enrich pagination total counts
func (p *Pagination) WithTotal(count int32) *Pagination {
	p.TotalCount = count
	p.TotalPages = p.getTotalPages()
	return p
}

// CalculateOffset limit offset calculation
func (p *Pagination) CalculateOffset() (int32, int32) {
	if p.CurrentPage == 0 {
		p.CurrentPage = 1
	}
	if p.Size <= 0 {
		p.Size = DefaultSize
	}
	o := float64(p.CurrentPage-1) * float64(p.Size)
	offset := int32(math.Ceil(o))
	return p.Size, offset
}

// getTotalPages internal method get total pages
func (p *Pagination) getTotalPages() int32 {
	d := float64(p.TotalCount) / float64(p.Size)
	if d < 0 {
		d = 1
	}
	return int32(math.Ceil(d))
}
