package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

type ResponseError struct {
	Error string `json:"error"`
}

func WriteJSON(w http.ResponseWriter , status int, data any){
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, ResponseError{Error: message})
}

func ReadJSON(w http.ResponseWriter , r *http.Request , dst any) error {
	r.Body = http.MaxBytesReader(w ,r.Body , 1048576)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil{
		return err
	}	

	if err := dec.Decode(&struct{}{}); !errors.Is(err , io.EOF){
		return errors.New("request body must only contain a single JSON object")
	}
	return nil
}

