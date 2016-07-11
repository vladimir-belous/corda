package com.r3corda.demos

import com.r3corda.core.serialization.SerializeAsToken
import com.r3corda.core.serialization.SerializeAsTokenContext
import com.r3corda.core.serialization.SingletonSerializationToken
import com.r3corda.node.utilities.MutableClock
import java.time.*
import javax.annotation.concurrent.ThreadSafe

/**
 * A [Clock] that can have the date advanced for use in demos.
 */
@ThreadSafe
class DemoClock(private var delegateClock: Clock = Clock.systemUTC()) : MutableClock(), SerializeAsToken {

    private val token = SingletonSerializationToken(this)

    override fun toToken(context: SerializeAsTokenContext) = SingletonSerializationToken.registerWithContext(token, this, context)

    @Synchronized fun updateDate(date: LocalDate): Boolean {
        val currentDate = LocalDate.now(this)
        if (currentDate.isBefore(date)) {
            // It's ok to increment
            delegateClock = Clock.offset(delegateClock, Duration.between(currentDate.atStartOfDay(), date.atStartOfDay()))
            notifyMutationObservers()
            return true
        }
        return false
    }

    @Synchronized override fun instant(): Instant {
        return delegateClock.instant()
    }

    // Do not use this. Instead seek to use ZonedDateTime methods.
    override fun withZone(zone: ZoneId): Clock {
        throw UnsupportedOperationException("Tokenized clock does not support withZone()")
    }

    @Synchronized override fun getZone(): ZoneId {
        return delegateClock.zone
    }

}
