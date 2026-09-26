package host

import "testing"

func TestLiveTextOptionsCarryReasoningChoice(t *testing.T) {
	if got := composeLiveText(nil); got != nil {
		t.Fatalf("nil live text = %+v", got)
	}
	if got := composeLiveText(&LiveTextOptions{}); got == nil || got.IncludeReasoning {
		t.Fatalf("default live text = %+v, want reasoning off", got)
	}
	if got := composeLiveText(&LiveTextOptions{IncludeReasoning: true}); got == nil || !got.IncludeReasoning {
		t.Fatalf("enabled live text = %+v, want reasoning on", got)
	}
}
