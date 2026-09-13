package kube

import (
	"flag"
	"io"

	"k8s.io/klog/v2"
)

// SilenceKlog stops client-go's logging from reaching stderr.
//
// client-go logs through klog, which writes directly to stderr by default. Left
// alone, its output interleaves with the transcript line format and — because
// it bypasses the recorder entirely — never reaches the log file, so the audit
// artifact would be missing exactly the API-level detail worth having. Routing
// it to io.Discard is the honest trade: the tool reports API failures itself,
// as errors, through the recorder.
func SilenceKlog() {
	var fs flag.FlagSet
	klog.InitFlags(&fs)
	_ = fs.Set("logtostderr", "false")
	_ = fs.Set("alsologtostderr", "false")
	_ = fs.Set("stderrthreshold", "FATAL")
	klog.SetOutput(io.Discard)
}
