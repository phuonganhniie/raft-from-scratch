package internal

import (
	"fmt"
	"log"
)

// dlog logs a debugging message if DebugCM > 1
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}
