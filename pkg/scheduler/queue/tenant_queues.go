// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"time"
)

type TenantID string

const emptyTenantID = TenantID("")

type QuerierID string

type tenantRequest struct {
	tenantID TenantID
	req      Request
}

// queueBroker encapsulates access to tenant queues for pending requests
// and maintains consistency with the tenant-querier assignments
type queueBroker struct {
	tenantQueuesTree *TreeQueue
	queueTree        *Tree

	tenantQuerierAssignments tenantQuerierAssignments

	maxTenantQueueSize               int
	additionalQueueDimensionsEnabled bool
	currentQuerier                   QuerierID

	treeShuffleShardState *shuffleShardState
}

func newQueueBroker(maxTenantQueueSize int, additionalQueueDimensionsEnabled bool, forgetDelay time.Duration) *queueBroker {
	currentQuerier := QuerierID("")
	tqas := &tenantQuerierAssignments{
		queriersByID:       map[QuerierID]*querierConn{},
		querierIDsSorted:   nil,
		querierForgetDelay: forgetDelay,
		tenantIDOrder:      nil,
		tenantsByID:        map[TenantID]*queueTenant{},
		tenantQuerierIDs:   map[TenantID]map[QuerierID]struct{}{},
	}

	sss := &shuffleShardState{
		tenantQuerierMap: tqas.tenantQuerierIDs,
		currentQuerier:   &currentQuerier,
	}
	// TODO (casie): Maybe set this using a flag, so we can also change how we build queue path accordingly
	tree := NewTree(
		sss,                // root
		&roundRobinState{}, // tenants
		&roundRobinState{}, // query components
	)
	qb := &queueBroker{
		tenantQueuesTree:                 NewTreeQueue("root"),
		queueTree:                        tree,
		tenantQuerierAssignments:         *tqas,
		maxTenantQueueSize:               maxTenantQueueSize,
		additionalQueueDimensionsEnabled: additionalQueueDimensionsEnabled,
		currentQuerier:                   currentQuerier,
		treeShuffleShardState:            sss,
	}

	return qb
}

func (qb *queueBroker) isEmpty() bool {
	//return qb.tenantQueuesTree.IsEmpty()
	return qb.queueTree.rootNode.IsEmpty()
}

// enqueueRequestBack is the standard interface to enqueue requests for dispatch to queriers.
//
// Tenants and tenant-querier shuffle sharding relationships are managed internally as needed.
func (qb *queueBroker) enqueueRequestBack(request *tenantRequest, tenantMaxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(request.tenantID, tenantMaxQueriers)
	if err != nil {
		return err
	}

	queuePath, err := qb.makeQueuePath(request)
	if err != nil {
		return err
	}

	if tenantQueueNode := qb.queueTree.rootNode.getNode(queuePath[:1]); tenantQueueNode != nil {
		if tenantQueueNode.ItemCount()+1 > qb.maxTenantQueueSize {
			return ErrTooManyRequests
		}
	}

	//err = qb.tenantQueuesTree.EnqueueBackByPath(queuePath, request)
	err = qb.queueTree.rootNode.enqueueBackByPath(qb.queueTree, queuePath, request)
	return err
}

// enqueueRequestFront should only be used for re-enqueueing previously dequeued requests
// to the front of the queue when there was a failure in dispatching to a querier.
//
// max tenant queue size checks are skipped even though queue size violations
// are not expected to occur when re-enqueuing a previously dequeued request.
func (qb *queueBroker) enqueueRequestFront(request *tenantRequest, tenantMaxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(request.tenantID, tenantMaxQueriers)
	if err != nil {
		return err
	}

	queuePath, err := qb.makeQueuePath(request)
	if err != nil {
		return err
	}
	return qb.queueTree.rootNode.enqueueFrontByPath(qb.queueTree, queuePath, request)
	//return qb.tenantQueuesTree.EnqueueFrontByPath(queuePath, request)
}

func (qb *queueBroker) makeQueuePath(request *tenantRequest) (QueuePath, error) {
	if qb.additionalQueueDimensionsEnabled {
		if schedulerRequest, ok := request.req.(*SchedulerRequest); ok {
			return append(QueuePath{string(request.tenantID)}, schedulerRequest.AdditionalQueueDimensions...), nil
		}
	}

	// else request.req is a frontend/v1.request, or additional queue dimensions are disabled
	return QueuePath{string(request.tenantID)}, nil
}

func (qb *queueBroker) dequeueRequestForQuerier(
	lastTenantIndex int,
	querierID QuerierID,
) (
	*tenantRequest,
	*queueTenant,
	int,
	error,
) {
	//tenant, tenantIndex, err := qb.tenantQuerierAssignments.getNextTenantForQuerier(lastTenantIndex, querierID)
	// check if querier is registered and is not shutting down
	if q := qb.tenantQuerierAssignments.queriersByID[querierID]; q == nil || q.shuttingDown {
		return nil, nil, qb.treeShuffleShardState.sharedQueuePosition, ErrQuerierShuttingDown
	}
	//if tenant == nil || err != nil {
	//	return nil, tenant, tenantIndex, err
	//}
	//
	//queuePath := QueuePath{string(tenant.tenantID)}
	//
	//queueElement := qb.tenantQueuesTree.DequeueByPath(queuePath)

	qb.currentQuerier = querierID
	qb.treeShuffleShardState.sharedQueuePosition = lastTenantIndex

	queuePath, queueElement := qb.queueTree.rootNode.dequeue()

	// TODO (casie): hacky - hardcoding second elt in queuePath to tenant ID
	var tenantID TenantID
	if len(queuePath) > 1 {
		tenantID = TenantID(queuePath[1])
	}
	tenant := qb.tenantQuerierAssignments.tenantsByID[tenantID]

	// dequeue returns the full path including root, but getNode expects the path _from_ root
	queueNodeAfterDequeue := qb.queueTree.rootNode.getNode(queuePath[1:])
	if queueNodeAfterDequeue == nil {
		// queue node was deleted due to being empty after dequeue
		qb.tenantQuerierAssignments.removeTenant(tenantID)
	}

	var request *tenantRequest
	if queueElement != nil {
		// re-casting to same type it was enqueued as; panic would indicate a bug
		request = queueElement.(*tenantRequest)
	}

	//return request, tenant, tenantIndex, nil
	return request, tenant, qb.treeShuffleShardState.sharedQueuePosition, nil
}

func (qb *queueBroker) addQuerierConnection(querierID QuerierID) (resharded bool) {
	return qb.tenantQuerierAssignments.addQuerierConnection(querierID)
}

func (qb *queueBroker) removeQuerierConnection(querierID QuerierID, now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.removeQuerierConnection(querierID, now)
}

func (qb *queueBroker) notifyQuerierShutdown(querierID QuerierID) (resharded bool) {
	return qb.tenantQuerierAssignments.notifyQuerierShutdown(querierID)
}

func (qb *queueBroker) forgetDisconnectedQueriers(now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.forgetDisconnectedQueriers(now)
}
