package backup

import (
	"log"

	"github.com/leijurv/gb/config"
)

func bucketerThread() {
	minSize := config.Config().MinBlobSize
	var tmp BlobPlan
	tmpSize := int64(0)

	for plan := range bucketerCh {
		var sz int64
		if plan.stakedClaim == nil && plan.confirmedSize == nil {
			// empty entry to unstick
			if len(tmp) > 0 {
				log.Println("unstick")
				uploaderCh <- tmp // leftovers, not necessarily of min size, but still need to be accounted for
				tmp = nil
				tmpSize = 0
			}
			continue
		}
		if plan.stakedClaim != nil {
			sz = *plan.stakedClaim
		}
		if plan.confirmedSize != nil {
			sz = *plan.confirmedSize
		}
		log.Println("Bucketer received with size", sz)
		if sz < minSize {
			tmp = append(tmp, plan)
			tmpSize += sz
			if tmpSize >= minSize {
				log.Println("Dumping blob")
				uploaderCh <- tmp
				tmp = nil
				tmpSize = 0
			}
		} else {
			log.Println("Dumping solo")
			uploaderCh <- []Planned{plan} // big boys get to ride on their own
		}
	}
	if len(tmp) > 0 {
		uploaderCh <- tmp // leftovers, not necessarily of min size, but still need to be accounted for
	}
}
