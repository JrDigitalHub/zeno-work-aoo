package orchestrator_test

import (
	"testing"

	"github.com/riverqueue/river"
	"github.com/JrDigitalHub/zeno-work-aoo/internal/orchestrator"
)

func TestJobArgsQueueNames(t *testing.T) {
	expectedQueues := map[string]bool{
		"discovery": false,
		"predator":  false,
		"sentinel":  false,
		"modeler":   false,
		"wallet":    false,
	}

	checkQueue := func(q string) {
		if _, ok := expectedQueues[q]; ok {
			expectedQueues[q] = true
		} else if q != river.QueueDefault {
			t.Errorf("Unexpected queue %q", q)
		}
	}

	checkQueue(orchestrator.DiscoveryJobArgs{}.InsertOpts().Queue)
	checkQueue(orchestrator.PredatorJobArgs{}.InsertOpts().Queue)
	checkQueue(orchestrator.SentinelTextOutputJobArgs{}.InsertOpts().Queue)
	checkQueue(orchestrator.ModelerResultJobArgs{}.InsertOpts().Queue)
	checkQueue(orchestrator.ProcessInvoiceJobArgs{}.InsertOpts().Queue)
	checkQueue(orchestrator.UpgradeWorkspaceJobArgs{}.InsertOpts().Queue)

	for q, found := range expectedQueues {
		if !found {
			t.Errorf("Queue %q was not targeted by any job args", q)
		}
	}
}
