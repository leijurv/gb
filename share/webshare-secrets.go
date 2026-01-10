package share

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/leijurv/gb/s3"
	"github.com/leijurv/gb/storage"
)

func WebshareSecrets(label string, envFormat bool) {
	store, ok := storage.StorageSelect(label)
	if !ok {
		panic("no storage")
	}
	if s3, ok := store.(*s3.S3); ok {
		secrets := make(map[string]string)
		secrets["S3_ENDPOINT"] = s3.Data.Endpoint
		secrets["S3_REGION"] = s3.Data.Region
		secrets["S3_BUCKET"] = s3.Data.Bucket
		secrets["S3_GB_PATH"] = s3.NiceRootPath()
		reader := bufio.NewReader(os.Stdin)
		fmt.Fprintln(os.Stderr, "It is recommended to create a new read only S3 key for the webshare worker. Enter the key id or enter 'n' to reuse the key used by gb")
		input, err := reader.ReadString('\n')
		if err != nil {
			panic(err)
		}
		if strings.TrimSpace(input) == "n" {
			fmt.Fprintln(os.Stderr, "I will reuse the key")
			secrets["S3_ACCESS_KEY"] = s3.Data.KeyID
			secrets["S3_SECRET_KEY"] = s3.Data.SecretKey
		} else {
			secrets["S3_ACCESS_KEY"] = strings.TrimSpace(input)
			fmt.Fprintln(os.Stderr, "Now enter the secret key")
			input, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}
			secrets["S3_SECRET_KEY"] = strings.TrimSpace(input)
		}
		if envFormat {
			fmt.Print(dotEnv(secrets))
		} else {
			jsonData, err := json.Marshal(secrets)
			if err != nil {
				panic(err)
			}
			fmt.Print(string(jsonData))
		}
	} else {
		panic("storage is not s3")
	}
}

func dotEnv(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Sort keys for deterministic output
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		v := m[k]
		b.WriteString(fmt.Sprintf("%s=%s\n", k, v))
	}

	return b.String()
}
