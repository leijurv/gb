package stats

import (
	"fmt"
	"log"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

type FileExtensionStats struct {
	Extension string
	Count     int64
	TotalSize int64
}

type LargestFile struct {
	Path string
	Size int64
	Hash string
}

type CompressionStats struct {
	Algorithm      string
	OriginalSize   int64
	CompressedSize int64
	Count          int64
}

type StorageStats struct {
	Label     string
	BlobCount int64
	TotalSize int64
}

func ShowStats() {
	fmt.Println("=== GB BACKUP STATISTICS ===")
	fmt.Println()

	showBasicStats()
	fmt.Println()

	showDeduplicationStats()
	fmt.Println()

	showCompressionStats()
	fmt.Println()

	showTopLargestFiles()
	fmt.Println()

	showFileExtensionStats()
	fmt.Println()

	showTimeStats()
	fmt.Println()

	showStorageStats()
	fmt.Println()
}

func showBasicStats() {
	fmt.Println("📊 Basic Statistics")
	fmt.Println("─────────────────")

	var totalFiles, distinctFiles int64
	var totalOriginalBytes int64

	err := db.DB.QueryRow("SELECT COUNT(*) FROM files WHERE end IS NULL").Scan(&totalFiles)
	if err != nil {
		log.Println("Error getting total files:", err)
		return
	}

	err = db.DB.QueryRow("SELECT COUNT(DISTINCT hash) FROM files WHERE end IS NULL").Scan(&distinctFiles)
	if err != nil {
		log.Println("Error getting distinct files:", err)
		return
	}

	err = db.DB.QueryRow(`
		SELECT COALESCE(SUM(s.size), 0) 
		FROM files f 
		JOIN sizes s ON f.hash = s.hash 
		WHERE f.end IS NULL
	`).Scan(&totalOriginalBytes)
	if err != nil {
		log.Println("Error getting total bytes:", err)
		return
	}

	var totalStorageBytes int64
	err = db.DB.QueryRow("SELECT COALESCE(SUM(size), 0) FROM blobs").Scan(&totalStorageBytes)
	if err != nil {
		log.Println("Error getting storage bytes:", err)
		return
	}

	fmt.Printf("Total files:           %s\n", utils.FormatCommas(totalFiles))
	fmt.Printf("Distinct files:        %s\n", utils.FormatCommas(distinctFiles))
	fmt.Printf("Total original size:   %s\n", formatBytes(totalOriginalBytes))
	fmt.Printf("Total storage used:    %s\n", formatBytes(totalStorageBytes))

	if totalOriginalBytes > 0 {
		efficiencyPercent := float64(totalStorageBytes) / float64(totalOriginalBytes) * 100
		fmt.Printf("Storage efficiency:    %.1f%% (%.1f%% savings)\n",
			efficiencyPercent, 100-efficiencyPercent)
	}

	if totalFiles > 0 {
		avgFileSize := totalOriginalBytes / totalFiles
		fmt.Printf("Average file size:     %s\n", formatBytes(avgFileSize))
	}
}

func showDeduplicationStats() {
	fmt.Println("🔗 Deduplication Analysis")
	fmt.Println("────────────────────────")

	var totalFiles, duplicateInstances int64
	var totalBytes, uniqueBytes int64

	err := db.DB.QueryRow(`
		SELECT 
			COUNT(*) as total_files,
			SUM(s.size) as total_bytes
		FROM files f 
		JOIN sizes s ON f.hash = s.hash 
		WHERE f.end IS NULL
	`).Scan(&totalFiles, &totalBytes)
	if err != nil {
		log.Println("Error getting duplication stats:", err)
		return
	}

	err = db.DB.QueryRow(`
		SELECT 
			COUNT(DISTINCT f.hash) as unique_files,
			SUM(DISTINCT s.size) as unique_bytes
		FROM files f 
		JOIN sizes s ON f.hash = s.hash 
		WHERE f.end IS NULL
	`).Scan(&duplicateInstances, &uniqueBytes)
	if err != nil {
		log.Println("Error getting unique stats:", err)
		return
	}

	duplicateFiles := totalFiles - duplicateInstances
	bytesSavedByDedup := totalBytes - uniqueBytes

	fmt.Printf("Files with duplicates:  %s\n", utils.FormatCommas(duplicateFiles))
	fmt.Printf("Bytes saved by dedup:   %s\n", formatBytes(bytesSavedByDedup))

	if totalBytes > 0 {
		dedupSavingsPercent := float64(bytesSavedByDedup) / float64(totalBytes) * 100
		fmt.Printf("Deduplication savings:  %.1f%%\n", dedupSavingsPercent)
	}

	// Show top duplicate file groups
	rows, err := db.DB.Query(`
		SELECT s.size, COUNT(*) as count
		FROM files f 
		JOIN sizes s ON f.hash = s.hash 
		WHERE f.end IS NULL
		GROUP BY f.hash
		HAVING COUNT(*) > 1
		ORDER BY s.size * (COUNT(*) - 1) DESC
		LIMIT 5
	`)
	if err != nil {
		log.Println("Error getting top duplicates:", err)
		return
	}
	defer rows.Close()

	fmt.Println("Top duplicate groups (by bytes saved):")
	for rows.Next() {
		var size, count int64
		err = rows.Scan(&size, &count)
		if err != nil {
			continue
		}
		saved := size * (count - 1)
		fmt.Printf("  %s × %d copies = %s saved\n",
			formatBytes(size), count, formatBytes(saved))
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func showCompressionStats() {
	fmt.Println("🗜️  Compression Analysis")
	fmt.Println("───────────────────────")

	rows, err := db.DB.Query(`
		SELECT 
			be.compression_alg,
			COUNT(*) as count,
			SUM(s.size) as original_size,
			SUM(be.final_size) as compressed_size
		FROM blob_entries be
		JOIN sizes s ON be.hash = s.hash
		GROUP BY be.compression_alg
		ORDER BY SUM(s.size - be.final_size) DESC
	`)
	if err != nil {
		log.Println("Error getting compression stats:", err)
		return
	}
	defer rows.Close()

	var totalOriginal, totalCompressed int64
	var compressionStats []CompressionStats

	for rows.Next() {
		var cs CompressionStats
		err = rows.Scan(&cs.Algorithm, &cs.Count, &cs.OriginalSize, &cs.CompressedSize)
		if err != nil {
			continue
		}

		if cs.Algorithm == "" {
			cs.Algorithm = "none"
		}

		compressionStats = append(compressionStats, cs)
		totalOriginal += cs.OriginalSize
		totalCompressed += cs.CompressedSize
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

	totalSaved := totalOriginal - totalCompressed
	fmt.Printf("Total compression savings: %s (%.1f%%)\n",
		formatBytes(totalSaved),
		float64(totalSaved)/float64(totalOriginal)*100)
	fmt.Println()

	for _, cs := range compressionStats {
		saved := cs.OriginalSize - cs.CompressedSize
		if cs.OriginalSize > 0 {
			ratio := float64(saved) / float64(cs.OriginalSize) * 100
			fmt.Printf("%-10s: %s files, %s → %s (%.1f%% savings)\n",
				cs.Algorithm, utils.FormatCommas(cs.Count),
				formatBytes(cs.OriginalSize), formatBytes(cs.CompressedSize), ratio)
		}
	}
}

func showTopLargestFiles() {
	fmt.Println("📏 Top 10 Largest Files")
	fmt.Println("──────────────────────")

	rows, err := db.DB.Query(`
		SELECT f.path, s.size, f.hash
		FROM files f 
		JOIN sizes s ON f.hash = s.hash 
		WHERE f.end IS NULL
		ORDER BY s.size DESC
		LIMIT 10
	`)
	if err != nil {
		log.Println("Error getting largest files:", err)
		return
	}
	defer rows.Close()

	i := 1
	for rows.Next() {
		var lf LargestFile
		err = rows.Scan(&lf.Path, &lf.Size, &lf.Hash)
		if err != nil {
			continue
		}

		hashStr := fmt.Sprintf("%x", lf.Hash)
		fmt.Printf("%2d. %s\n", i, formatBytes(lf.Size))
		fmt.Printf("    %s\n", lf.Path)
		fmt.Printf("    %s\n", hashStr[:16]+"...")
		fmt.Println()
		i++
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func showFileExtensionStats() {
	fmt.Println("📁 File Extensions")
	fmt.Println("─────────────────")

	rows, err := db.DB.Query(`
		WITH extensions AS (
			SELECT 
				f.path,
				s.size,
				CASE 
					WHEN f.path LIKE '%.%' THEN 
						LOWER(SUBSTR(f.path, LENGTH(f.path) - LENGTH(REPLACE(f.path, '.', '')) + 1))
					ELSE '(no extension)'
				END as raw_ext
			FROM files f 
			JOIN sizes s ON f.hash = s.hash 
			WHERE f.end IS NULL
		)
		SELECT 
			CASE 
				WHEN raw_ext LIKE '%.%' THEN SUBSTR(raw_ext, INSTR(raw_ext, '.') + 1)
				ELSE raw_ext
			END as extension,
			COUNT(*) as count,
			SUM(size) as total_size
		FROM extensions
		GROUP BY extension
		ORDER BY total_size DESC
		LIMIT 15
	`)
	if err != nil {
		log.Println("Error getting extension stats:", err)
		return
	}
	defer rows.Close()

	var extStats []FileExtensionStats
	for rows.Next() {
		var es FileExtensionStats
		err = rows.Scan(&es.Extension, &es.Count, &es.TotalSize)
		if err != nil {
			continue
		}
		extStats = append(extStats, es)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}

	for i, es := range extStats {
		if i >= 10 {
			break
		}
		fmt.Printf("%-15s: %s files, %s\n",
			es.Extension, utils.FormatCommas(es.Count), formatBytes(es.TotalSize))
	}
}

func showTimeStats() {
	fmt.Println("⏰ Time Analysis")
	fmt.Println("───────────────")

	var oldestTime, newestTime int64
	var oldestPath, newestPath string

	err := db.DB.QueryRow(`
		SELECT path, start 
		FROM files 
		WHERE end IS NULL 
		ORDER BY start ASC 
		LIMIT 1
	`).Scan(&oldestPath, &oldestTime)
	if err != nil && err != db.ErrNoRows {
		log.Println("Error getting oldest file:", err)
		return
	}

	err = db.DB.QueryRow(`
		SELECT path, start 
		FROM files 
		WHERE end IS NULL 
		ORDER BY start DESC 
		LIMIT 1
	`).Scan(&newestPath, &newestTime)
	if err != nil && err != db.ErrNoRows {
		log.Println("Error getting newest file:", err)
		return
	}

	if oldestTime > 0 {
		fmt.Printf("Oldest backup:    %s\n", time.Unix(oldestTime, 0).Format("2006-01-02 15:04:05"))
		fmt.Printf("                  %s\n", oldestPath)
	}

	if newestTime > 0 {
		fmt.Printf("Newest backup:    %s\n", time.Unix(newestTime, 0).Format("2006-01-02 15:04:05"))
		fmt.Printf("                  %s\n", newestPath)
	}

	if oldestTime > 0 && newestTime > 0 {
		duration := time.Unix(newestTime, 0).Sub(time.Unix(oldestTime, 0))
		fmt.Printf("Backup timespan:  %s\n", formatDuration(duration))
	}
}

func showStorageStats() {
	fmt.Println("💾 Storage Distribution")
	fmt.Println("─────────────────────")

	rows, err := db.DB.Query(`
		SELECT 
			s.readable_label,
			COUNT(bs.blob_id) as blob_count,
			SUM(b.size) as total_size
		FROM storage s
		LEFT JOIN blob_storage bs ON s.storage_id = bs.storage_id
		LEFT JOIN blobs b ON bs.blob_id = b.blob_id
		GROUP BY s.storage_id, s.readable_label
		ORDER BY total_size DESC
	`)
	if err != nil {
		log.Println("Error getting storage stats:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ss StorageStats
		var totalSize *int64
		err = rows.Scan(&ss.Label, &ss.BlobCount, &totalSize)
		if err != nil {
			continue
		}

		if totalSize != nil {
			ss.TotalSize = *totalSize
		}

		fmt.Printf("%-20s: %s blobs, %s\n",
			ss.Label, utils.FormatCommas(ss.BlobCount), formatBytes(ss.TotalSize))
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	size := float64(bytes)
	unitIndex := 0

	for size >= 1024 && unitIndex < len(units)-1 {
		size /= 1024
		unitIndex++
	}

	if size >= 100 {
		return fmt.Sprintf("%.0f %s", size, units[unitIndex])
	} else if size >= 10 {
		return fmt.Sprintf("%.1f %s", size, units[unitIndex])
	} else {
		return fmt.Sprintf("%.2f %s", size, units[unitIndex])
	}
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24

	if days > 0 {
		return fmt.Sprintf("%d days, %d hours", days, hours)
	} else if hours > 0 {
		return fmt.Sprintf("%d hours", hours)
	} else {
		return d.String()
	}
}
