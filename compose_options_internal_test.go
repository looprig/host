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

func TestLiveTextOptionsCarryToolStepChoice(t *testing.T) {
	if got := composeLiveText(&LiveTextOptions{}); got == nil || got.IncludeToolSteps {
		t.Fatalf("default live text = %+v, want tool steps off", got)
	}
	if got := composeLiveText(&LiveTextOptions{IncludeToolSteps: true}); got == nil || !got.IncludeToolSteps || got.IncludeReasoning {
		t.Fatalf("tool steps live text = %+v, want tool steps on and reasoning untouched", got)
	}
}
