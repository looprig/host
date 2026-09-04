package harnessadapter_test

import "time"

// testInstant is one fixed wall-clock reading, so a record's timestamps do not
// depend on how long a test took.
func testInstant() time.Time {
	return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
}
