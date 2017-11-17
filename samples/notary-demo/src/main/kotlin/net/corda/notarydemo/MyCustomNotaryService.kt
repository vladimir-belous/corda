package net.corda.notarydemo

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.TimeWindow
import net.corda.core.contracts.TransactionVerificationException
import net.corda.core.flows.*
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.node.services.TimeWindowChecker
import net.corda.core.node.services.TrustedAuthorityNotaryService
import net.corda.core.transactions.*
import net.corda.node.services.transactions.PersistentUniquenessProvider
import net.corda.testing.amqpSpecific
import java.security.PublicKey
import java.security.SignatureException

/**
 * A custom notary service should provide a constructor that accepts two parameters of types [AppServiceHub] and [PublicKey].
 *
 * Note that at present only a single-node notary service can be customised.
 */
// START 1
@CordaService
class MyCustomValidatingNotaryService(override val services: AppServiceHub, override val notaryIdentityKey: PublicKey) : TrustedAuthorityNotaryService() {
    override val timeWindowChecker = TimeWindowChecker(services.clock)
    override val uniquenessProvider = PersistentUniquenessProvider()

    override fun createServiceFlow(otherPartySession: FlowSession): FlowLogic<Void?> = MyValidatingNotaryFlow(otherPartySession, this)

    override fun start() {}
    override fun stop() {}
}
// END 1

@Suppress("UNUSED_PARAMETER")
// START 2
class MyValidatingNotaryFlow(otherSide: FlowSession, service: MyCustomValidatingNotaryService) : NotaryFlow.Service(otherSide, service) {
    /**
     * The received transaction is checked for contract-validity, for which the caller also has to to reveal the whole
     * transaction dependency chain.
     */
    @Suspendable
    override fun receiveAndVerifyTx(): TransactionParts {
        try {
            val stx = subFlow(ReceiveTransactionFlow(otherSideSession, checkSufficientSignatures = false))
            val notary = stx.notary
            checkNotary(notary)
            val transactionWithSignatures = stx.resolveTransactionWithSignatures(serviceHub)
            checkSignatures(transactionWithSignatures)

            val fullTransaction = when(stx.coreTransaction) {
                is NotaryChangeWireTransaction -> stx.resolveNotaryChangeTransaction(serviceHub)
                is ContractUpgradeWireTransaction -> stx.resolveContractUpgradeTransaction(serviceHub)
                else -> stx.toLedgerTransaction(serviceHub)
            }
            customVerify(fullTransaction)

            val timeWindow: TimeWindow? = if (stx.coreTransaction is WireTransaction) stx.tx.timeWindow else null
            return TransactionParts(stx.id, stx.inputs, timeWindow, notary!!)
        } catch (e: Exception) {
            throw when (e) {
                is TransactionVerificationException,
                is SignatureException -> NotaryException(NotaryError.TransactionInvalid(e))
                else -> e
            }
        }
    }

    private fun customVerify(transaction: FullTransaction) {
        // Add custom verification logic
    }

    private fun checkSignatures(tx: TransactionWithSignatures) {
        try {
            tx.verifySignaturesExcept(service.notaryIdentityKey)
        } catch (e: SignatureException) {
            throw NotaryException(NotaryError.TransactionInvalid(e))
        }
    }
}
// END 2
