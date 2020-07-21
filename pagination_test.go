package bom

import (
	"reflect"
	"testing"
)

func TestNewPagination(t *testing.T) {
	type args struct {
		page int32
		size int32
	}
	tests := []struct {
		name string
		args args
		want *Pagination
	}{

		{name: "test standard initialization", args: args{page: 1, size: 20}, want: &Pagination{
			TotalCount:  0,
			TotalPages:  0,
			CurrentPage: 1,
			Size:        20,
		}},

		{name: "test the case when passed 0", args: args{page: 0, size: 20}, want: &Pagination{
			TotalCount:  0,
			TotalPages:  0,
			CurrentPage: 1,
			Size:        20,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewPagination(tt.args.page, tt.args.size); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewPagination() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPagination_CalculateOffset(t *testing.T) {
	type fields struct {
		CurrentPage int32
		Size        int32
	}
	tests := []struct {
		name   string
		fields fields
		want   int32
		want1  int32
	}{
		{name: "page1", fields: struct {
			CurrentPage int32
			Size        int32
		}{CurrentPage: 1, Size: 20}, want: 20, want1: 0},
		{name: "page2", fields: struct {
			CurrentPage int32
			Size        int32
		}{CurrentPage: 2, Size: 20}, want: 20, want1: 20},
		{name: "page3", fields: struct {
			CurrentPage int32
			Size        int32
		}{CurrentPage: 3, Size: 20}, want: 20, want1: 40},
		{name: "page4", fields: struct {
			CurrentPage int32
			Size        int32
		}{CurrentPage: 4, Size: 5}, want: 5, want1: 15},
		{name: "incorrect offest", fields: struct {
			CurrentPage int32
			Size        int32
		}{CurrentPage: 0, Size: 50}, want: 50, want1: 0},
		{name: "incorrect offest2", fields: struct {
			CurrentPage int32
			Size        int32
		}{CurrentPage: 0, Size: -100}, want: 20, want1: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pagination{
				CurrentPage: tt.fields.CurrentPage,
				Size:        tt.fields.Size,
			}
			got, got1 := p.CalculateOffset()
			if got != tt.want {
				t.Errorf("CalculateOffset() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("CalculateOffset() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestPagination_WithTotal(t *testing.T) {
	type fields struct {
		TotalCount  int32
		TotalPages  int32
		CurrentPage int32
		Size        int32
	}
	type args struct {
		count int32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Pagination
	}{
		{name: "init", fields: struct {
			TotalCount  int32
			TotalPages  int32
			CurrentPage int32
			Size        int32
		}{CurrentPage: 1, Size: 50}, args: struct{ count int32 }{count: 100}, want: &Pagination{
			TotalCount:  100,
			TotalPages:  2,
			CurrentPage: 1,
			Size:        50,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pagination{
				TotalCount:  tt.fields.TotalCount,
				TotalPages:  tt.fields.TotalPages,
				CurrentPage: tt.fields.CurrentPage,
				Size:        tt.fields.Size,
			}
			if got := p.WithTotal(tt.args.count); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithTotal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPagination_getTotalPages(t *testing.T) {
	type fields struct {
		TotalCount  int32
		TotalPages  int32
		CurrentPage int32
		Size        int32
	}
	tests := []struct {
		name   string
		fields fields
		want   int32
	}{
		{name: "case 1", fields: struct {
			TotalCount  int32
			TotalPages  int32
			CurrentPage int32
			Size        int32
		}{TotalCount: 100, Size: 50}, want: 2},

		{name: "case 2", fields: struct {
			TotalCount  int32
			TotalPages  int32
			CurrentPage int32
			Size        int32
		}{TotalCount: 80, Size: 50}, want: 2},

		{name: "case 3", fields: struct {
			TotalCount  int32
			TotalPages  int32
			CurrentPage int32
			Size        int32
		}{TotalCount: 51, Size: 50}, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pagination{
				TotalCount:  tt.fields.TotalCount,
				TotalPages:  tt.fields.TotalPages,
				CurrentPage: tt.fields.CurrentPage,
				Size:        tt.fields.Size,
			}
			if got := p.getTotalPages(); got != tt.want {
				t.Errorf("getTotalPages() = %v, want %v", got, tt.want)
			}
		})
	}
}
