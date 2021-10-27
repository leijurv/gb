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
	_, err := io.CopyBuffer(out, lOut, make([]byte, 1024*1024))
	if err != nil {
		// don't panic here (i.e. don't call utils.Copy), because if lepton exits with nonzero, this error is non-nil, and we want to return that for fallback compression, not panic
		return err
	}
	return cmd.Wait() // only needed to check the exit code, the process has already ended since io.Copy returned, meaning it must have hit EOF
}

type WrappedReadCloserHackToAvoidZombieProcess struct {
	cmd *exec.Cmd
	in io.ReadCloser
}

func (w *WrappedReadCloserHackToAvoidZombieProcess) Read(p []byte) (int, error) {
	n, err := w.in.Read(p)
	return n, err
}

func (w *WrappedReadCloserHackToAvoidZombieProcess) Close() error {
	defer w.cmd.Wait()
	return w.in.Close()
}

func (n *LeptonCompression) Decompress(in io.Reader) io.ReadCloser {
	cmd, lOut := BeginLeptonProcess(in)
	return &WrappedReadCloserHackToAvoidZombieProcess{cmd, lOut}
}

func (n *LeptonCompression) AlgName() string {
	return "lepton"
}

func (n *LeptonCompression) Fallible() bool {
	return true
}

func (n *LeptonCompression) DecompressionTrollBashCommandIncludingThePipe() string {
	return " | lepton -allowprogressive -memory=2048M -threadmemory=256M -"
}
