package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TimeWindow
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.crypto.TransactionSignature
import net.corda.core.crypto.keys
import net.corda.core.identity.Party
import net.corda.core.internal.FetchDataFlow
import net.corda.core.node.services.NotaryService
import net.corda.core.node.services.TrustedAuthorityNotaryService
import net.corda.core.node.services.UniquenessProvider
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.ContractUpgradeWireTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.UntrustworthyData
import net.corda.core.utilities.unwrap
import java.security.SignatureException
import java.util.function.Predicate

class NotaryFlow {
    /**
     * A flow to be used by a party for obtaining signature(s) from a [NotaryService] ascertaining the transaction
     * time-window is correct and none of its inputs have been used in another completed transaction.
     *
     * In case of a single-node or Raft notary, the flow will return a single signature. For the BFT notary multiple
     * signatures will be returned – one from each replica that accepted the input state commit.
     *
     * @throws NotaryException in case the any of the inputs to the transaction have been consumed
     *                         by another transaction or the time-window is invalid.
     */
    @InitiatingFlow
    open class Client(private val stx: SignedTransaction,
                      override val progressTracker: ProgressTracker) : FlowLogic<List<TransactionSignature>>() {
        constructor(stx: SignedTransaction) : this(stx, tracker())

        companion object {
            object REQUESTING : ProgressTracker.Step("Requesting signature by Notary service")
            object VALIDATING : ProgressTracker.Step("Validating response from Notary service")

            fun tracker() = ProgressTracker(REQUESTING, VALIDATING)
        }

        @Suspendable
        @Throws(NotaryException::class)
        override fun call(): List<TransactionSignature> {
            progressTracker.currentStep = REQUESTING

            val notaryParty = stx.notary ?: throw IllegalStateException("Transaction does not specify a Notary")
            check(serviceHub.networkMapCache.isNotary(notaryParty)) { "$notaryParty is not a notary on the network" }
            check(stx.inputs.all { stateRef -> serviceHub.loadState(stateRef).notary == notaryParty }) {
                "Input states must have the same Notary"
            }

            try {
                stx.resolveTransactionWithSignatures(serviceHub).verifySignaturesExcept(notaryParty.owningKey)
            } catch (ex: SignatureException) {
                throw NotaryException(NotaryError.TransactionInvalid(ex))
            }

            val response = try {
                val session = initiateFlow(notaryParty)
                if (serviceHub.networkMapCache.isValidatingNotary(notaryParty)) {
                    subFlow(SendTransactionWithRetry(session, stx))
                    session.receive<List<TransactionSignature>>()
                } else {
                    val ctx = stx.coreTransaction

                    val filteredTransaction = when (ctx) {
                        is ContractUpgradeWireTransaction -> ctx.buildFilteredTransaction()
                        is WireTransaction -> ctx.buildFilteredTransaction(Predicate { it is StateRef || it is TimeWindow || it == notaryParty })
                        else -> ctx
                    }

                    session.sendAndReceiveWithRetry(filteredTransaction)
                }
            } catch (e: NotaryException) {
                if (e.error is NotaryError.Conflict) {
                    e.error.conflict.verified()
                }
                throw e
            }

            return response.unwrap { signatures ->
                signatures.forEach { validateSignature(it, stx.id, notaryParty) }
                signatures
            }
        }

        private fun validateSignature(sig: TransactionSignature, txId: SecureHash, notaryParty: Party) {
            check(sig.by in notaryParty.owningKey.keys) { "Invalid signer for the notary result" }
            sig.verify(txId)
        }
    }

    /**
     * The [SendTransactionWithRetry] flow is equivalent to [SendTransactionFlow] but using [sendAndReceiveWithRetry]
     * instead of [sendAndReceive], [SendTransactionWithRetry] is intended to be use by the notary client only.
     */
    private class SendTransactionWithRetry(otherSideSession: FlowSession, stx: SignedTransaction) : SendTransactionFlow(otherSideSession, stx) {
        @Suspendable
        override fun sendPayloadAndReceiveDataRequest(otherSideSession: FlowSession, payload: Any): UntrustworthyData<FetchDataFlow.Request> {
            return otherSideSession.sendAndReceiveWithRetry(payload)
        }
    }

    /**
     * A flow run by a notary service that handles notarisation requests.
     *
     * It checks that the time-window command is valid (if present) and commits the input state, or returns a conflict
     * if any of the input states have been previously committed.
     *
     * Additional transaction validation logic can be added when implementing [receiveAndVerifyTx].
     */
    // See AbstractStateReplacementFlow.Acceptor for why it's Void?
    abstract class Service(val otherSideSession: FlowSession, val service: TrustedAuthorityNotaryService) : FlowLogic<Void?>() {

        @Suspendable
        override fun call(): Void? {
            check(serviceHub.myInfo.legalIdentities.any { serviceHub.networkMapCache.isNotary(it) }) {
                "We are not a notary on the network"
            }
            val (id, inputs, timeWindow, notary) = receiveAndVerifyTx()
            checkNotary(notary)
            service.validateTimeWindow(timeWindow)
            service.commitInputStates(inputs, id, otherSideSession.counterparty)
            signAndSendResponse(id)
            return null
        }

        /**
         * Implement custom logic to receive the transaction to notarise, and perform verification based on validity and
         * privacy requirements.
         */
        @Suspendable
        abstract fun receiveAndVerifyTx(): TransactionParts

        /** Check if transaction is intended to be signed by this notary. */
        @Suspendable
        protected fun checkNotary(notary: Party?) {
            if (notary?.owningKey != service.notaryIdentityKey) {
                throw NotaryException(NotaryError.WrongNotary)
            }
        }

        @Suspendable
        private fun signAndSendResponse(txId: SecureHash) {
            val signature = service.sign(txId)
            otherSideSession.send(listOf(signature))
        }
    }
}

/**
 * The minimum amount of information needed to notarise a transaction. Note that this does not include
 * any sensitive transaction details.
 */
data class TransactionParts(val id: SecureHash, val inputs: List<StateRef>, val timestamp: TimeWindow?, val notary: Party?)

class NotaryException(val error: NotaryError) : FlowException("Error response from Notary - $error")

@CordaSerializable
sealed class NotaryError {
    data class Conflict(val txId: SecureHash, val conflict: SignedData<UniquenessProvider.Conflict>) : NotaryError() {
        override fun toString() = "One or more input states for transaction $txId have been used in another transaction"
    }

    /** Thrown if the time specified in the [TimeWindow] command is outside the allowed tolerance. */
    object TimeWindowInvalid : NotaryError()

    data class TransactionInvalid(val cause: Throwable) : NotaryError() {
        override fun toString() = cause.toString()
    }

    object WrongNotary : NotaryError()
}
