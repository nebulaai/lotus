package storage

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/lotus/chain/types"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"

	"github.com/filecoin-project/go-state-types/abi"
)

type CompleteGeneratePoSTCb func(posts []miner.SubmitWindowedPoStParams, err error)
type CompleteSubmitPoSTCb func(err error)

type stateMachineAPI interface {
	StateMinerProvingDeadline(context.Context, address.Address, types.TipSetKey) (*dline.Info, error)
	startGeneratePoST(ctx context.Context, ts *types.TipSet, deadline *dline.Info, onComplete CompleteGeneratePoSTCb) context.CancelFunc
	startSubmitPoST(ctx context.Context, ts *types.TipSet, deadline *dline.Info, posts []miner.SubmitWindowedPoStParams, onComplete CompleteSubmitPoSTCb) context.CancelFunc
	onAbort(ts *types.TipSet, deadline *dline.Info, state PoSTStatus)
	failPost(err error, ts *types.TipSet, deadline *dline.Info)
}

const SubmitConfidence = 4 // TODO: config

type PoSTStatus string

const (
	PoSTStatusNew             PoSTStatus = "PoSTStatusNew"
	PoSTStatusProving         PoSTStatus = "PoSTStatusProving"
	PoSTStatusProvingComplete PoSTStatus = "PoSTStatusProvingComplete"
	PoSTStatusSubmitting      PoSTStatus = "PoSTStatusSubmitting"
	PoSTStatusComplete        PoSTStatus = "PoSTStatusComplete"
)

func (ps PoSTStatus) String() string {
	return string(ps)
}

type stateMachine struct {
	api          stateMachineAPI
	actor        address.Address
	stateChanges chan PoSTStatus

	lk          sync.RWMutex
	state       PoSTStatus
	deadline    *dline.Info
	posts       []miner.SubmitWindowedPoStParams
	abortProof  context.CancelFunc
	abortSubmit func(bool)
	currentTS   *types.TipSet
	currentCtx  context.Context
}

func newStateMachine(api stateMachineAPI, actor address.Address, stateChanges chan PoSTStatus) *stateMachine {
	return &stateMachine{
		api:          api,
		actor:        actor,
		state:        PoSTStatusNew,
		stateChanges: stateChanges,
	}
}

func (a *stateMachine) setState(state PoSTStatus) {
	//oldState := a.state
	a.state = state
	if a.stateChanges != nil {
		a.stateChanges <- state
		//fmt.Println(oldState, "->", state)
	}
}

func (a *stateMachine) startGeneratePoST(deadline *dline.Info) {
	//fmt.Println("startGeneratePoST")
	a.deadline = deadline
	a.setState(PoSTStatusProving)

	ts := a.currentTS
	abort := a.api.startGeneratePoST(a.currentCtx, ts, deadline, func(posts []miner.SubmitWindowedPoStParams, err error) {
		a.lk.Lock()
		defer a.lk.Unlock()

		//fmt.Println("generate PoST response", len(posts), err)
		if err != nil {
			a.failProving(err, ts, deadline)
			return
		}
		a.completeGeneratePoST(posts)
	})
	a.abortProof = abort
}

// failProving is called when proving fails or is aborted
func (a *stateMachine) failProving(err error, ts *types.TipSet, deadline *dline.Info) {
	a.api.failPost(err, ts, deadline)
	log.Warnf("Aborting Window PoSt %s (Deadline: %+v)", PoSTStatusProving, deadline)
	a.api.onAbort(ts, deadline, PoSTStatusProving)

	// Reset to the starting state
	a.setState(PoSTStatusNew)
}

func (a *stateMachine) completeGeneratePoST(posts []miner.SubmitWindowedPoStParams) {
	//fmt.Println("complete generate PoST")

	a.posts = posts
	if len(a.posts) == 0 {
		a.setState(PoSTStatusComplete)
		return
	}

	a.setState(PoSTStatusProvingComplete)

	// Generating the proof has completed, so trigger a state change that will
	// check if we're at the right height to submit the proof
	a.runStateChange()
}

func (a *stateMachine) startSubmitPoST() {
	a.setState(PoSTStatusSubmitting)

	reorg := false
	ts := a.currentTS
	deadline := a.deadline
	abort := a.api.startSubmitPoST(a.currentCtx, ts, deadline, a.posts, func(err error) {
		a.lk.Lock()
		defer a.lk.Unlock()

		if err != nil {
			a.failSubmit(err, ts, deadline, reorg)
			return
		}
		a.setState(PoSTStatusComplete)
	})
	a.abortSubmit = func(r bool) {
		// If the chain reorged, we want to trigger the stateChange function
		// to run again after the abort has completed
		reorg = r
		abort()
	}
}

// failSubmit is called when proof submit fails or is aborted
func (a *stateMachine) failSubmit(err error, ts *types.TipSet, deadline *dline.Info, reorg bool) {
	a.api.failPost(err, ts, deadline)
	log.Warnf("Aborting Window PoSt %s (Deadline: %+v)", PoSTStatusSubmitting, deadline)
	a.api.onAbort(ts, deadline, PoSTStatusSubmitting)

	// If the abort was because of a chain reorg, go back to the proving
	// complete state and trigger a state change so that the proof can be
	// resubmitted
	if reorg {
		a.setState(PoSTStatusProvingComplete)
		a.runStateChange()
		return
	}

	// Otherwise reset to the starting state
	a.setState(PoSTStatusNew)
}

func (a *stateMachine) HeadChange(ctx context.Context, newTS *types.TipSet, reorged bool) error {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.currentCtx = ctx
	a.currentTS = newTS

	return a.stateChange(reorged)
}

func (a *stateMachine) runStateChange() {
	err := a.stateChange(false)
	if err != nil {
		log.Errorf("handling state change in window post state machine: %+v", err)
	}
}

func (a *stateMachine) stateChange(reorged bool) error {
	ctx := a.currentCtx
	newTS := a.currentTS

	// Get the current proving deadline info
	diNew, err := a.api.StateMinerProvingDeadline(ctx, a.actor, newTS.Key())
	if err != nil {
		return err
	}

	// If the current proving period hasn't started yet, bail out
	if !diNew.PeriodStarted() {
		return nil // not proving anything yet
	}

	// If the active PoST has expired, abort it
	newHeight := newTS.Height()
	//fmt.Println("height", newHeight, "expired?", a.expired(newHeight))
	if a.expired(newHeight) {
		abortedActiveState := a.abort(newTS, diNew)

		// If the state that was aborted was an active state, ie we were
		// proving or submitting proof, we need to wait for the state to change
		// before proceeding
		if abortedActiveState {
			return nil
		}

		// If the state that was aborted was not an active state, ie it was
		// PoSTStatusProvingComplete, we can safely continue
	}

	// If proof generation for the current deadline has not started, start it
	if a.state == PoSTStatusNew {
		a.startGeneratePoST(diNew)
		return nil
	}

	// If the proof has been generated, and it's not already being submitted,
	// submit it
	//fmt.Println("ready to submit?", a.readyToSubmitPoST(newHeight))
	if a.readyToSubmitPoST(newHeight) {
		a.startSubmitPoST()
		return nil
	}

	// If the proof is being submitted, and the chain has reorged, abort the
	// submit
	if a.state == PoSTStatusSubmitting && reorged {
		a.abortSubmit(true)
		return nil
	}

	// If the proof has been submitted
	//fmt.Println("post submitted?", a.postSubmitted())
	if a.state == PoSTStatusComplete {
		// Check if the current height is within the challenge period for the
		// next deadline
		diNext := nextDeadline(diNew)
		if newHeight < diNext.Challenge {
			//fmt.Println("post submitted but not ready for next challenge")
			return nil
		}

		// Start generating the proof for the next deadline
		a.startGeneratePoST(diNext)

		return nil
	}

	//fmt.Println("nothing to do on this state event")

	// Nothing to do, wait for next state event
	return nil
}
func (a *stateMachine) expired(at abi.ChainEpoch) bool {
	return !a.idle() && at >= a.deadline.Close
}

func (a *stateMachine) readyToSubmitPoST(height abi.ChainEpoch) bool {
	return a.state == PoSTStatusProvingComplete && height > a.deadline.Open+SubmitConfidence
}

func (a *stateMachine) idle() bool {
	return a.state == PoSTStatusNew || a.state == PoSTStatusComplete
}

func (a *stateMachine) Shutdown() {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.abort(a.currentTS, a.deadline)
}

// abort is called when the state machine is shut down or when the current
// deadline expires
// returns true if an active state was aborted, false if it was an idle (start
// or end) state
func (a *stateMachine) abort(ts *types.TipSet, deadline *dline.Info) bool {
	//fmt.Println("abort", deadline.CurrentEpoch)
	// If the window post is being proved or submitted, abort those operations
	// and wait for them to call abortWithError() (below)
	switch a.state {
	case PoSTStatusProving:
		//fmt.Println("aborting proving")
		a.abortProof()
		return true
	case PoSTStatusSubmitting:
		//fmt.Println("aborting submitting")
		a.abortSubmit(false)
		return true
	}

	// If the window post is not idle (eg in PoSTStatusProvingComplete)
	if !a.idle() {
		// Log a warning and inform the API that proving was aborted
		log.Warnf("Aborting Window PoSt (Deadline: %+v)", deadline)
		a.api.onAbort(ts, deadline, a.state)
	}

	// Reset to the starting state
	a.setState(PoSTStatusNew)
	return false
}

func (a *stateMachine) CurrentState() PoSTStatus {
	a.lk.RLock()
	defer a.lk.RUnlock()

	return a.state
}

func (a *stateMachine) CurrentTSDL() (*types.TipSet, *dline.Info) {
	a.lk.RLock()
	defer a.lk.RUnlock()

	var ts *types.TipSet
	if a.currentTS != nil {
		*ts = *a.currentTS
	}

	var dl *dline.Info
	if a.deadline != nil {
		*dl = *a.deadline
	}

	return ts, dl
}

func nextDeadline(currentDeadline *dline.Info) *dline.Info {
	periodStart := currentDeadline.PeriodStart
	newDeadline := currentDeadline.Index + 1
	if newDeadline == miner.WPoStPeriodDeadlines {
		newDeadline = 0
		periodStart = periodStart + miner.WPoStProvingPeriod
	}

	return miner.NewDeadlineInfo(periodStart, newDeadline, currentDeadline.CurrentEpoch)
}
