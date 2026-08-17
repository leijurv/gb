package backup

import (
	"log"

	"github.com/leijurv/gb/config"
)

func (s *BackupSession) bucketerThread() {
	var tmp BlobPlan
	var tmpSize int64

	flush := func(why string) {
		if len(tmp) == 0 {
			return
		}
		log.Println("Dumping blob:", why, "-", len(tmp), "files,", tmpSize, "bytes")
		s.dispatchUpload(tmp)
		tmp = nil
		tmpSize = 0
	}

	for {
		select {
		case plan := <-s.bucketerCh:
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
				s.dispatchUpload(BlobPlan{plan}) // big boys get to ride on their own
				continue
			}
			tmp = append(tmp, plan) // small boys get grouped together
			tmpSize += sz
			if tmpSize >= config.Config().MinBlobSize || int64(len(tmp)) > config.Config().MinBlobCount {
				flush("full")
			}
		case <-s.bucketer.poke:
			switch s.bucketer.action() {
			case bucketerFlush:
				// everything still to come is parked on a size claim, and the only claims
				// left unreleased are held by files in tmp, so nothing anywhere can move
				// until we let this blob go
				flush("pipeline is parked on size claims held by this buffer")
			case bucketerDone:
				flush("end of backup")
				return
			}
		}
	}
}

// dispatchUpload hands a blob to the uploaders. The counter is bumped before the send so
// that a plan in flight on uploaderCh still counts as an upload in progress.
func (s *BackupSession) dispatchUpload(plan BlobPlan) {
	s.bucketer.update(func() { s.bucketer.uploading++ })
	s.uploaderCh <- plan
}
