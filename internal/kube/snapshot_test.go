package kube

import (
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// paginate is the reason §3's "fixed small number of list calls" is safe on a
// large cluster: client-go does not paginate List() itself, so without this a
// cluster with thousands of pods returns one enormous response.

func TestPaginateWalksEveryPage(t *testing.T) {
	t.Parallel()
	pages := [][]int{{1, 2, 3}, {4, 5, 6}, {7}}
	var seenTokens []string

	got, err := paginate(func(o metav1.ListOptions) ([]int, string, error) {
		seenTokens = append(seenTokens, o.Continue)
		idx := len(seenTokens) - 1
		token := ""
		if idx < len(pages)-1 {
			token = fmt.Sprintf("token-%d", idx+1)
		}
		return pages[idx], token, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []int{1, 2, 3, 4, 5, 6, 7}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	// The first call must not carry a continue token, and each subsequent one
	// must carry the previous page's.
	if seenTokens[0] != "" {
		t.Errorf("first request carried continue=%q", seenTokens[0])
	}
	if seenTokens[1] != "token-1" || seenTokens[2] != "token-2" {
		t.Errorf("continue tokens = %v, want the server's values echoed back", seenTokens)
	}
}

func TestPaginateSetsAPageLimit(t *testing.T) {
	t.Parallel()
	var limit int64
	_, err := paginate(func(o metav1.ListOptions) ([]int, string, error) {
		limit = o.Limit
		return nil, "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if limit == 0 {
		t.Error("no Limit was set; client-go would return the whole collection in one response")
	}
}

// A continue token expires if the caller is slow and etcd compacts the revision
// out from under it. Returning the partial set would silently under-report
// blast radius, so the listing restarts instead.
func TestPaginateRestartsWhenTheContinueTokenExpires(t *testing.T) {
	t.Parallel()
	expired := apierrors.NewResourceExpired("continue token expired")
	attempt := 0
	calls := 0

	got, err := paginate(func(o metav1.ListOptions) ([]int, string, error) {
		calls++
		if o.Continue == "" {
			attempt++
			return []int{1, 2}, "more", nil
		}
		// Fail the second page on the first attempt only.
		if attempt == 1 {
			return nil, "", expired
		}
		return []int{3, 4}, "", nil
	})
	if err != nil {
		t.Fatalf("paginate gave up instead of restarting: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("got %v, want a complete set of 4 after the restart", got)
	}
	if calls < 4 {
		t.Errorf("made %d calls; the listing does not appear to have restarted", calls)
	}
}

// Restarting forever would hang a CLI. After a bounded number of attempts the
// error surfaces rather than the caller receiving a short list.
func TestPaginateGivesUpAfterRepeatedExpiry(t *testing.T) {
	t.Parallel()
	expired := apierrors.NewResourceExpired("continue token expired")

	_, err := paginate(func(o metav1.ListOptions) ([]int, string, error) {
		if o.Continue == "" {
			return []int{1}, "more", nil
		}
		return nil, "", expired
	})
	if err == nil {
		t.Fatal("paginate returned success despite never completing a listing")
	}
}

// Any other API error must surface immediately: a partial list would be
// reported as the complete picture.
func TestPaginateSurfacesOtherErrors(t *testing.T) {
	t.Parallel()
	boom := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", fmt.Errorf("nope"))

	_, err := paginate(func(o metav1.ListOptions) ([]int, string, error) {
		if o.Continue == "" {
			return []int{1}, "more", nil
		}
		return nil, "", boom
	})
	if !apierrors.IsForbidden(err) {
		t.Errorf("err = %v, want the original Forbidden to surface", err)
	}
}

func TestInventorySnapshotIsNotMarkedFull(t *testing.T) {
	t.Parallel()
	// Classification silently produces wrong replica counts without the owner
	// lists, so the two snapshot kinds must stay distinguishable.
	if (&Snapshot{}).Full() {
		t.Error("a zero snapshot reports Full()")
	}
	if !NewSnapshotForTest(SnapshotFixture{}).Full() {
		t.Error("NewSnapshotForTest did not mark the snapshot full")
	}
}
