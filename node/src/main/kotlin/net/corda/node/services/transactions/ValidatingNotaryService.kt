package net.corda.node.services.transactions

import net.corda.core.flows.FlowSession
import net.corda.core.flows.NotaryFlow
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.TimeWindowChecker
import net.corda.core.node.services.TrustedAuthorityNotaryService
import net.corda.node.internal.NotaryIdentity
import java.security.PublicKey

/** A Notary service that validates the transaction chain of the submitted transaction before committing it */
class ValidatingNotaryService(override val services: ServiceHub, override val notaryIdentityKey: PublicKey) : TrustedAuthorityNotaryService() {
    @Suppress("unused")
    constructor(services: ServiceHub, notaryIdentity: NotaryIdentity) : this(services, notaryIdentity.key)

    companion object {
        val id = constructId(validating = true)
    }
    override val timeWindowChecker = TimeWindowChecker(services.clock)
    override val uniquenessProvider = PersistentUniquenessProvider()

    override fun createServiceFlow(otherPartySession: FlowSession): NotaryFlow.Service = ValidatingNotaryFlow(otherPartySession, this)

    override fun start() {}
    override fun stop() {}
}