package backup

import (
	"log"

	"github.com/leijurv/gb/config"
)

func bucketerThread() {
	var tmp BlobPlan
	var tmpSize int64

	for {
		select {
		case <-bucketerPassthrough:
			// Scanner is done - flush remaining buffer and switch to passthrough mode
			if len(tmp) > 0 {
				log.Println("Passthrough mode: flushing remaining")
				uploaderCh <- tmp
				tmp = nil
				tmpSize = 0
			}
			log.Println("Bucketer now in passthrough mode")
			for plan := range bucketerCh {
				log.Println("Bucketer passthrough")
				uploaderCh <- []Planned{plan}
			}
			return
		case plan, ok := <-bucketerCh:
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
				uploaderCh <- []Planned{plan}
			} else {
				tmp = append(tmp, plan)
				tmpSize += sz
				if tmpSize >= config.Config().MinBlobSize || int64(len(tmp)) > config.Config().MinBlobCount {
					log.Println("Dumping blob")
					uploaderCh <- tmp
					tmp = nil
					tmpSize = 0
				}
			}
		}
	}
}
