package backup

import (
	"log"

	"github.com/leijurv/gb/config"
)

func (s *BackupSession) bucketerThread() {
	var tmp BlobPlan
	var tmpSize int64

	for {
		select {
		case <-s.bucketerPassthrough:
			// Scanner is done - flush remaining buffer and switch to passthrough mode
			if len(tmp) > 0 {
				log.Println("Passthrough mode: flushing remaining")
				s.uploaderCh <- tmp
				tmp = nil
				tmpSize = 0
			}
			log.Println("Bucketer now in passthrough mode")
			for plan := range s.bucketerCh {
				log.Println("Bucketer passthrough")
				s.uploaderCh <- []Planned{plan}
			}
			return
		case plan, ok := <-s.bucketerCh:
			if !ok {
				panic("unreachable?")
			}
			var sz int64
			if plan.stakedClaim != nil {
				sz = *plan.stakedClaim
			}
			if plan.confirmedSize != nil {
				sz = *plan.confirmedSize
			}
			log.Println("Bucketer received with size", sz)

			if sz >= config.Config().MinBlobSize {
				log.Println("Dumping solo")
				s.uploaderCh <- []Planned{plan}
			} else {
				tmp = append(tmp, plan)
				tmpSize += sz
				if tmpSize >= config.Config().MinBlobSize || int64(len(tmp)) > config.Config().MinBlobCount {
					log.Println("Dumping blob")
					s.uploaderCh <- tmp
					tmp = nil
					tmpSize = 0
				}
			}
		}
	}
}
