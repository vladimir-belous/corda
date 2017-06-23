package net.corda.node.services.transactions

import net.corda.core.flows.FlowSession
import net.corda.core.flows.NotaryFlow
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.TimeWindowChecker
import net.corda.core.node.services.TrustedAuthorityNotaryService
import net.corda.node.internal.NotaryIdentity
import net.corda.nodeapi.internal.ServiceType
import java.security.PublicKey

/** A simple Notary service that does not perform transaction validation */
class SimpleNotaryService(override val services: ServiceHub, override val notaryIdentityKey: PublicKey) : TrustedAuthorityNotaryService() {
    @Suppress("unused")
    constructor(services: ServiceHub, notaryIdentity: NotaryIdentity) : this(services, notaryIdentity.key)

    companion object {
        val type = ServiceType.notary.getSubType("simple")
    }

    override val timeWindowChecker = TimeWindowChecker(services.clock)
    override val uniquenessProvider = PersistentUniquenessProvider()

    override fun createServiceFlow(otherPartySession: FlowSession): NotaryFlow.Service = NonValidatingNotaryFlow(otherPartySession, this)

    override fun start() {}
    override fun stop() {}
}