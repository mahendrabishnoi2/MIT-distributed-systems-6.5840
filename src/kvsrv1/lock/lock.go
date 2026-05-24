package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey    string
	lockState  string
	keyVersion rpc.Tversion
}

const free = ""

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockKey: l, keyVersion: 0}
	lk.lockState = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		lockState, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			errl := lk.ck.Put(lk.lockKey, lk.lockState, 0)
			if errl != rpc.OK {
				continue
			}
			lk.keyVersion = 1
			return
		}
		if lockState != free && lockState != lk.lockState {
			continue
		}
		if lockState == lk.lockState {
			return
		}
		err = lk.ck.Put(lk.lockKey, lk.lockState, version)
		if err != rpc.OK {
			continue
		}
		lk.keyVersion = version + 1
		return
	}
}

func (lk *Lock) Release() {
	for {
		lockState, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey { // not locked, key doesn't exist
			return
		}
		if lockState != lk.lockState { // I can't release lock as I didn't acquire lock
			continue
		}
		if version != lk.keyVersion { // even though random state matches, version didn't match, I didn't acquire lock, mabye random key collision
			continue
		}
		err = lk.ck.Put(lk.lockKey, free, lk.keyVersion)
		if err != rpc.OK {
			continue
		}
		lk.keyVersion++
		return
	}
}
