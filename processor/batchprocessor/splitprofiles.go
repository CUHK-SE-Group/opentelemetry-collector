// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor // import "go.opentelemetry.io/collector/processor/batchprocessor"

import (
	"go.opentelemetry.io/collector/pdata/pprofile"
)

// splitProfiles removes profiles from the input profiles and returns a new profiles object of the specified size.
func splitProfiles(size int, src pprofile.Profiles) pprofile.Profiles {
    if profileCount(src) <= size {
        return src
    }
    total := 0
    dest := pprofile.NewProfiles()

    src.ResourceProfiles().RemoveIf(func(rp pprofile.ResourceProfiles) bool {
        if total == size {
            return false
        }
        rpCount := resourceProfilesCount(rp)
        if total+rpCount <= size {
            total += rpCount
            rp.MoveTo(dest.ResourceProfiles().AppendEmpty())
            return true
        }
        // partial resource
        destRP := dest.ResourceProfiles().AppendEmpty()
        rp.Resource().CopyTo(destRP.Resource())
        rp.ScopeProfiles().RemoveIf(func(sp pprofile.ScopeProfiles) bool {
            if total == size {
                return false
            }
            spCount := sp.Profiles().Len()
            if total+spCount <= size {
                total += spCount
                sp.MoveTo(destRP.ScopeProfiles().AppendEmpty())
                return true
            }
            // partial scope
            destSP := destRP.ScopeProfiles().AppendEmpty()
            sp.Scope().CopyTo(destSP.Scope())
            sp.Profiles().RemoveIf(func(p pprofile.Profile) bool {
                if total == size {
                    return false
                }
                p.MoveTo(destSP.Profiles().AppendEmpty())
                total++
                return true
            })
            return false
        })
        return rp.ScopeProfiles().Len() == 0
    })

    return dest
}

// profileCount calculates the number of profile records in the profiles object.
func profileCount(profiles pprofile.Profiles) int {
	count := 0
	rps := profiles.ResourceProfiles()
	for i := 0; i < rps.Len(); i++ {
		rp := rps.At(i)
		sps := rp.ScopeProfiles()
		for j := 0; j < sps.Len(); j++ {
			sp := sps.At(j)
			count += sp.Profiles().Len()
		}
	}
	return count
}

// resourceProfilesCount calculates the total number of profiles in the pprofile.ResourceProfiles.
func resourceProfilesCount(rp pprofile.ResourceProfiles) (count int) {
	for k := 0; k < rp.ScopeProfiles().Len(); k++ {
		count += rp.ScopeProfiles().At(k).Profiles().Len()
	}
	return
}
