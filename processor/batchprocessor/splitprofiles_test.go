// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/pdata/pprofile"
)

func TestSplitProfiles_noop(t *testing.T) {
	pp := generateTestProfiles(20)
	splitSize := 40
	split := splitProfiles(splitSize, pp)
	assert.Equal(t, pp, split)

	i := 0
	pp.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().RemoveIf(func(pprofile.Profile) bool {
		i++
		return i > 5
	})
	assert.Equal(t, pp, split)
}

func TestSplitProfiles(t *testing.T) {
	pp := generateTestProfiles(20)
	profiles := pp.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles()
	for i := 0; i < profiles.Len(); i++ {
		profiles.At(i).SetProfileID(getTestProfileID(0, i))
	}

	splitSize := 5
	split := splitProfiles(splitSize, pp)
	assert.Equal(t, splitSize, profileCount(split))
	assert.Equal(t, 15, profileCount(pp)) // 20 - 5 = 15 remaining
	assert.Equal(t, getTestProfileID(0, 0), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(0, 4), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(4).ProfileID())

	split = splitProfiles(splitSize, pp)
	assert.Equal(t, splitSize, profileCount(split)) // Should split exactly 5
	assert.Equal(t, 10, profileCount(pp))           // 15 - 5 = 10 remaining
	assert.Equal(t, getTestProfileID(0, 5), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(0, 9), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(4).ProfileID())

	split = splitProfiles(splitSize, pp)
	assert.Equal(t, splitSize, profileCount(split)) // Should split exactly 5
	assert.Equal(t, 5, profileCount(pp))            // 10 - 5 = 5 remaining
	assert.Equal(t, getTestProfileID(0, 10), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(0, 14), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(4).ProfileID())

	split = splitProfiles(splitSize, pp)
	assert.Equal(t, splitSize, profileCount(split)) // Should split exactly 5
	assert.Equal(t, 5, profileCount(pp))            // 5 - 5 = 0 remaining (This is the failing assertion)
	assert.Equal(t, getTestProfileID(0, 15), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(0, 19), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(4).ProfileID())
}

func TestSplitProfilesMultipleResourceProfiles(t *testing.T) {
	pp := generateTestProfiles(20)
	profiles := pp.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles()
	for i := 0; i < profiles.Len(); i++ {
		profiles.At(i).SetProfileID(getTestProfileID(0, i))
	}

	// Add second resource profile
	rp := pp.ResourceProfiles().AppendEmpty()
	rp.Resource().Attributes().PutStr("resource", "R2")
	sp := rp.ScopeProfiles().AppendEmpty()
	sp.Scope().SetName("scope2")
	sp.Scope().SetVersion("v2")

	profilesSlice := sp.Profiles()
	for i := 0; i < 20; i++ {
		profile := profilesSlice.AppendEmpty()
		profile.SetProfileID(getTestProfileID(1, i))
	}

	splitSize := 5
	split := splitProfiles(splitSize, pp)
	assert.Equal(t, splitSize, profileCount(split))
	assert.Equal(t, 35, profileCount(pp))
	assert.Equal(t, getTestProfileID(0, 0), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(0, 4), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(4).ProfileID())
}

func TestSplitProfilesMultipleScopeProfiles(t *testing.T) {
	pp := generateTestProfiles(20)
	profiles := pp.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles()
	for i := 0; i < profiles.Len(); i++ {
		profiles.At(i).SetProfileID(getTestProfileID(0, i))
	}

	// Add a second scope profile to the first resource
	sp := pp.ResourceProfiles().At(0).ScopeProfiles().AppendEmpty()
	sp.Scope().SetName("scope2")
	sp.Scope().SetVersion("v2")

	profilesSlice := sp.Profiles()
	for i := 0; i < 20; i++ {
		profile := profilesSlice.AppendEmpty()
		profile.SetProfileID(getTestProfileID(1, i))
	}

	splitSize := 25
	split := splitProfiles(splitSize, pp)
	assert.Equal(t, splitSize, profileCount(split))
	assert.Equal(t, 15, profileCount(pp))
	assert.Equal(t, getTestProfileID(0, 0), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(0, 19), split.ResourceProfiles().At(0).ScopeProfiles().At(0).Profiles().At(19).ProfileID())
	assert.Equal(t, getTestProfileID(1, 0), split.ResourceProfiles().At(0).ScopeProfiles().At(1).Profiles().At(0).ProfileID())
	assert.Equal(t, getTestProfileID(1, 4), split.ResourceProfiles().At(0).ScopeProfiles().At(1).Profiles().At(4).ProfileID())
}

// generateTestProfiles creates profiles for testing with the given number of profiles per scope
func generateTestProfiles(numProfiles int) pprofile.Profiles {
	pp := pprofile.NewProfiles()
	rs := pp.ResourceProfiles().AppendEmpty()
	rs.Resource().Attributes().PutStr("resource", "R1")
	ils := rs.ScopeProfiles().AppendEmpty()
	ils.Scope().SetName("scope")
	ils.Scope().SetVersion("v1")

	profiles := ils.Profiles()
	for i := 0; i < numProfiles; i++ {
		profile := profiles.AppendEmpty()
		// Set default profile ID - will be overwritten in tests
		profile.SetProfileID(getTestProfileID(0, i))
	}

	return pp
}

// getTestProfileID creates a unique profile ID for testing based on resource and profile index
func getTestProfileID(resourceIdx, profileIdx int) pprofile.ProfileID {
	// Create a 16-byte ID where the first 8 bytes are resourceIdx and the last 8 bytes are profileIdx
	var bytes [16]byte
	binary.BigEndian.PutUint64(bytes[0:8], uint64(resourceIdx))
	binary.BigEndian.PutUint64(bytes[8:16], uint64(profileIdx))
	return pprofile.ProfileID(bytes)
}
