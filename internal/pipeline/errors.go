package pipeline

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
)

const (
	// OnErrorFail cancels the run at the first bad record.
	OnErrorFail = "fail"
	// OnErrorSkip counts and logs bad records, then continues.
	OnErrorSkip = "skip"
	// OnErrorDLQ writes bad records to a dead-letter file and continues.
	OnErrorDLQ = "dlq"
)

// badLine is a record that failed parsing or transformation.
type badLine struct {
	Line int
	Raw  []byte
	Err  string
}

type dlqEntry struct {
	Line  int    `json:"line"`
	Raw   string `json:"raw"`
	Error string `json:"error"`
}

func normalizeOnError(policy string) (string, error) {
	if policy == "" {
		return OnErrorFail, nil
	}
	switch policy {
	case OnErrorFail, OnErrorSkip, OnErrorDLQ:
		return policy, nil
	default:
		return "", fmt.Errorf("on-error must be fail, skip, or dlq, got %q", policy)
	}
}

// handleBad applies the error policy. A nil error from handleBad means the run continues.
func handleBad(policy string, lineNo int, raw []byte, cause error, stats *Stats, bad *[]badLine) error {
	stats.Errors.Add(1)
	switch policy {
	case OnErrorSkip:
		log.Printf("skip line %d: %s", lineNo, cause.Error())
		return nil
	case OnErrorDLQ:
		cp := make([]byte, len(raw))
		copy(cp, raw)
		*bad = append(*bad, badLine{Line: lineNo, Raw: cp, Err: cause.Error()})
		return nil
	default:
		return cause
	}
}

func writeDeadLetters(w *bufio.Writer, bad []badLine) error {
	if len(bad) == 0 {
		return nil
	}
	if w == nil {
		return fmt.Errorf("dead-letter writer is nil")
	}
	for _, b := range bad {
		raw, err := json.Marshal(dlqEntry{Line: b.Line, Raw: string(b.Raw), Error: b.Err})
		if err != nil {
			return err
		}
		if _, err := w.Write(raw); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return nil
}
