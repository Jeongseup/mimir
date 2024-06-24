// SPDX-License-Identifier: AGPL-3.0-only

package queue

import "math"

type queryComponentUtilizationCheck interface {
	TriggerUtilizationCheck() bool
	ExceedsThresholdForComponentName(name string) (bool, QueryComponent)
}

type queryComponentUtilizationReserveConnections struct {
	utilization      *QueryComponentUtilization
	connectedWorkers int
	waitingWorkers   int
	queueLen         int
}

func (qcurc *queryComponentUtilizationReserveConnections) ExceedsThresholdForComponentName(name string) (bool, QueryComponent) {
	if qcurc.connectedWorkers <= 1 {
		// corner case; cannot reserve capacity with only one worker available
		return false, ""
	}

	// allow the functionality to be turned off via setting targetReservedCapacity to 0
	minReservedConnections := 0
	if qcurc.utilization.targetReservedCapacity > 0 {
		// reserve at least one connection in case (connected workers) * (reserved capacity) is less than one
		minReservedConnections = int(
			math.Ceil(
				math.Max(qcurc.utilization.targetReservedCapacity*float64(qcurc.connectedWorkers), 1),
			),
		)
	}

	isIngester, isStoreGateway := queryComponentFlags(name)
	if isIngester {
		if qcurc.connectedWorkers-(qcurc.utilization.ingesterInflightRequests) <= minReservedConnections {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if qcurc.connectedWorkers-(qcurc.utilization.storeGatewayInflightRequests) <= minReservedConnections {
			return true, StoreGateway
		}
	}
	return false, ""
}

func (qcurc *queryComponentUtilizationReserveConnections) TriggerUtilizationCheck() bool {
	if qcurc.waitingWorkers > qcurc.queueLen {
		// excess querier-worker capacity; no need to reserve any for now
		return false
	}
	return true
}

type queryComponentUtilizationDequeueSkipOverThreshold struct {
	queryComponentUtilizationThreshold queryComponentUtilizationCheck
	currentNodeOrderIndex              int
	nodeOrder                          []string
	nodesChecked                       int
	nodesSkippedOrder                  []string

	//queueNodeCounts                    map[string]int
}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) checkedAllNodes() bool {
	return qcud.nodesChecked == len(qcud.nodeOrder)+1 && len(qcud.nodesSkippedOrder) == 0

}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	if qcud.currentNodeOrderIndex == localQueueIndex {
		// special case; cannot slice into nodeOrder with index -1
		// place at end of slice, which is the last slot before the local queue slot
		qcud.nodeOrder = append(qcud.nodeOrder, child.Name())
	} else {
		// insert into the order behind current child queue index
		// to prevent the possibility of new nodes continually jumping the line
		qcud.nodeOrder = append(
			qcud.nodeOrder[:qcud.currentNodeOrderIndex],
			append(
				[]string{child.Name()},
				qcud.nodeOrder[qcud.currentNodeOrderIndex:]...,
			)...,
		)
		// update current child queue index to its new place in the expanded slice
		qcud.currentNodeOrderIndex++
	}
}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) dequeueSelectNode(node *Node) (*Node, bool) {
	if qcud.currentNodeOrderIndex == localQueueIndex {
		return node, qcud.checkedAllNodes()
	}

	checkedAllNodesBeforeSkips := qcud.nodesChecked == len(qcud.nodeOrder)+1

	if !checkedAllNodesBeforeSkips {
		// have not made it through first rotation yet
		currentNodeName := qcud.nodeOrder[qcud.currentNodeOrderIndex]
		if qcud.queryComponentUtilizationThreshold.TriggerUtilizationCheck() {
			// triggered a utilization check
			exceedsThreshold, _ := qcud.queryComponentUtilizationThreshold.ExceedsThresholdForComponentName(currentNodeName)

			if exceedsThreshold {
				// triggered a utilization check *and* the query component corresponding to this node
				// has been determined to be utilizing querier-worker connections in excess of the target threshold

				// skip over this node for now, but add to the skipped order
				// in case we cannot find an item to dequeue from non-skipped nodes
				qcud.nodesSkippedOrder = append(qcud.nodesSkippedOrder, currentNodeName)
				qcud.currentNodeOrderIndex++

				// increment nodesChecked;
				// we will not return true for checkedAllNodes until we check the skipped nodes too
				qcud.nodesChecked++
				return nil, qcud.checkedAllNodes()
			} else {
				// triggered a check but query component corresponding to this node is not over utilization threshold
				qcud.nodesChecked++
				return node.queueMap[currentNodeName], qcud.checkedAllNodes()
			}
		} else {
			// no utilization check triggered; select the current node
			qcud.nodesChecked++
			return node.queueMap[currentNodeName], qcud.checkedAllNodes()
		}
	}

	// else; we have checked all nodes the first time
	// if we are here, none of the nodes that did not get skipped were able to be dequeued from
	// and checkedAllNodes() has not returned true yet, so there are skipped nodes to select from
	for len(qcud.nodesSkippedOrder) > 0 {
		currentNodeName := qcud.nodesSkippedOrder[0]
		qcud.nodesSkippedOrder = qcud.nodesSkippedOrder[1:]
		return node.queueMap[currentNodeName], qcud.checkedAllNodes()
	}
	return nil, qcud.checkedAllNodes()
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if dequeuedFrom != node && dequeuedFrom.IsEmpty() {
		childName := dequeuedFrom.Name()
		delete(node.queueMap, childName)
		for idx, name := range qcud.nodeOrder {
			if name == childName {
				qcud.nodeOrder = append(qcud.nodeOrder[:idx], qcud.nodeOrder[idx+1:]...)
				break
			}
		}
	} else {
		qcud.currentNodeOrderIndex++
	}
	if qcud.currentNodeOrderIndex >= len(qcud.nodeOrder) {
		qcud.currentNodeOrderIndex = localQueueIndex
	}
	qcud.nodesChecked = 0
}
