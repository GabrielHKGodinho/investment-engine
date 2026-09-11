package apierror

import (
	"encoding/json"
	"net/http"
)

// Códigos de erro estáveis — o cliente pode checar isso programaticamente,
// diferente da mensagem, que pode mudar de texto sem quebrar ninguém.
const (
	CodeValidation   = "VALIDATION_ERROR"
	CodeUnauthorized = "UNAUTHORIZED"
	CodeNotFound     = "NOT_FOUND"
	CodeInternal     = "INTERNAL_ERROR"
)

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type apiError struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Fields  []FieldError `json:"fields,omitempty"`
}

type errorResponse struct {
	Error apiError `json:"error"`
}

// Write sends a standardized JSON error response.
// fields is optional — pass nil for errors that aren't about specific request fields.
func Write(w http.ResponseWriter, status int, code, message string, fields []FieldError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errorResponse{
		Error: apiError{Code: code, Message: message, Fields: fields},
	})
}
