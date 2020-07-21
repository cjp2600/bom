package bom

import "errors"

// Define common errors
var (
	ErrClientRequired = errors.New("mongodb client is required")
)
