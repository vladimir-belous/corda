package net.corda.node.internal

import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TransactionResolutionException
import net.corda.core.contracts.TransactionState
import com.codahale.metrics.MetricRegistry
import net.corda.core.flows.FlowLogic
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.filterNotNull
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.node.NodeInfo
import net.corda.core.node.StateLoader
import net.corda.core.node.services.TransactionStorage
import net.corda.node.services.api.CheckpointStorage
import net.corda.core.node.ServiceHub
import net.corda.node.internal.cordapp.CordappProviderInternal
import net.corda.node.services.api.*
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.persistence.NodeAttachmentService
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.node.utilities.CordaPersistence
import java.util.stream.Stream
import kotlin.streams.toList

interface StartedNode<out N : AbstractNode> {
    val internals: N
    val services: StartedNodeServices
    val info: NodeInfo
    val checkpointStorage: CheckpointStorage
    val smm: StateMachineManager
    val attachments: NodeAttachmentService
    val inNodeNetworkMapService: NetworkMapService
    val network: MessagingService
    val database: CordaPersistence
    val rpcOps: CordaRPCOps
    val rpcFlows: RPCFlows
    val stateMachineRecordedTransactionMapping: StateMachineRecordedTransactionMappingStorage
    val metrics: MetricRegistry
    val schemaService: SchemaService
    val cordappProvider: CordappProviderInternal
    val notaryIdentity: NotaryIdentity?
    fun dispose() = internals.stop()
    fun <T : FlowLogic<*>> registerInitiatedFlow(initiatedFlowClass: Class<T>) = internals.registerInitiatedFlow(initiatedFlowClass)
}

interface StateLoaderInternal : StateLoader
class StateLoaderImpl(private val validatedTransactions: TransactionStorage) : StateLoaderInternal {
    @Throws(TransactionResolutionException::class)
    override fun loadState(stateRef: StateRef): TransactionState<*> {
        val stx = validatedTransactions.getTransaction(stateRef.txhash) ?: throw TransactionResolutionException(stateRef.txhash)
        return stx.resolveBaseTransaction(this).outputs[stateRef.index]
    }
}

interface StartedNodeServices : ServiceHub, FlowStarter {
    val database: CordaPersistence
}

class AllIdentities(legalIdentity: PartyAndCertificate, myNotaryIdentity: NotaryIdentity?) {
    // TODO  We keep only notary identity as additional legalIdentity if we run it on a node . Multiple identities need more design thinking.
    val identities = Stream.of(legalIdentity, myNotaryIdentity?.notaryIdentity).filterNotNull().toList()
}
