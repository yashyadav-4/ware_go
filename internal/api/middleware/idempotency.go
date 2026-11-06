package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yashyadav-4/ware_go/internal/repository/db"
)

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body *bytes.Buffer
}

func (rec *responseRecorder) WriteHeader(code int){
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *responseRecorder) Write(b []byte)(int , error){
	rec.body.Write(b)
	return rec.ResponseWriter.Write(b)
}

func writeError(w http.ResponseWriter , status int , message string){
	w.Header().Set("Content-Type" , "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}


func Idempotency(queries *db.Queries) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler{
		return http.HandlerFunc(func(w http.ResponseWriter , r *http.Request){

			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				writeError(w , http.StatusBadRequest , "Idempotency-Key Header is required")
				return 
			}

			bodyBytes , err := io.ReadAll(r.Body)
			if err != nil {
				writeError(w, http.StatusBadRequest, "failed to read request body")
				return
			}

			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			hash := sha256.Sum256(bodyBytes)
			requestHash := hex.EncodeToString(hash[:])

			existing , err := queries.GetIdempotencyKey(r.Context() , key)
			if err == nil{
				
				if existing.RequestHash != requestHash{
					writeError(w, http.StatusUnprocessableEntity, "idempotency key reused with different request payload")
					return
				}

				if existing.State == "IN_PROGRESS"{
					writeError(w, http.StatusConflict, "request with this idempotency key is currently being processed")
					return
				}

				if existing.State == "COMPLETED"{
					w.Header().Set("Content-Type" , "application/json")
					w.Header().Set("X-Idempotent-Replay" ,"true" )

					status := http.StatusOK
					if existing.ResponseStatus.Valid{
						status = int(existing.ResponseStatus.Int32)
					}

					w.WriteHeader(status)
					_, _ = w.Write(existing.ResponseBody)
					return
				}
			}else if !errors.Is(err , pgx.ErrNoRows){
				writeError(w, http.StatusInternalServerError, "failed to check idempotency key")
				return
			}

			//create idempotency key if not existed before
			_ ,err = queries.CreateIdempotencyKey(r.Context() , db.CreateIdempotencyKeyParams{
				Key: key,
				RequestHash: requestHash,
			})

			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == "23505" {
					writeError(w, http.StatusConflict, "concurrent request with this idempotency key")
					return
				}
				writeError(w, http.StatusInternalServerError, "failed to register idempotency key")
				return
			}

			recorder := &responseRecorder{
				ResponseWriter: w,
				statusCode: http.StatusOK,
				body: &bytes.Buffer{},
			}

			next.ServeHTTP(recorder ,r)

			completeCtx , cancel := context.WithTimeout(context.Background() , 5*time.Second)
			defer cancel()

			var statusInt pgtype.Int4
			_ = statusInt.Scan(int32(recorder.statusCode))

			_ , _ = queries.CompleteIdempotencyKey(completeCtx , db.CompleteIdempotencyKeyParams{
				Key:            key,
				ResponseStatus: statusInt,
				ResponseBody:   recorder.body.Bytes(),
			})
		})
	}
}
