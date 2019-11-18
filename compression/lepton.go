package compression

import (
	"io"
	"log"
	"os"
	"os/exec"
)

// https://github.com/dropbox/lepton/

type LeptonCompression struct{}

func BeginLeptonProcess(in io.Reader) (*exec.Cmd, io.ReadCloser) {
	cmd := exec.Command("lepton", "-allowprogressive", "-memory=2048M", "-threadmemory=256M", "-")
	cmd.Stderr = os.Stderr // very epic
	cmd.Stdin = in         // VERY VERY epic WOW
	out, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	err = cmd.Start()
	if err != nil {
		log.Println(err)
		panic("do u not have lepton installed & on your $PATH smh")
	}
	return cmd, out
}

func (n *LeptonCompression) Compress(out io.Writer, in io.Reader) error {
	cmd, lOut := BeginLeptonProcess(in)
	_, err := io.Copy(out, lOut)
	if err != nil {
		return err
	}
	return cmd.Wait() // only needed to check the exit code, the process has already ended since io.Copy returned, meaning it must have hit EOF
}

func (n *LeptonCompression) Decompress(in io.Reader) io.ReadCloser {
	_, lOut := BeginLeptonProcess(in)
	return lOut // is that even LEGAL?!
}

func (n *LeptonCompression) AlgName() string {
	return "lepton"
}

func (n *LeptonCompression) Fallible() bool {
	return true
}
