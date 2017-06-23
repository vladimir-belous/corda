package net.corda.node.services.statemachine

import net.corda.core.internal.VisibleForTesting
import net.corda.core.flows.*
import net.corda.core.serialization.CordaSerializable
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.lazyhub.LazyHubFactory

/**
 * The internal concrete implementation of the FlowLogicRef marker interface.
 */
@CordaSerializable
data class FlowLogicRefImpl internal constructor(val flowLogicClassName: String, val args: Collection<Any?>) : FlowLogicRef

/**
 * A class for conversion to and from [FlowLogic] and [FlowLogicRef] instances.
 *
 * Validation of types is performed on the way in and way out in case this object is passed between JVMs which might have differing
 * whitelists.
 *
 * TODO: Align with API related logic for passing in FlowLogic references (FlowRef)
 * TODO: Actual support for AppContext / AttachmentsClassLoader
 * TODO: at some point check whether there is permission, beyond the annotations, to start flows. For example, as a security
 * measure we might want the ability for the node admin to blacklist a flow such that it moves immediately to the "Flow Hospital"
 * in response to a potential malicious use or buggy update to an app etc.
 */
class FlowLogicRefFactoryImpl(private val lazyHubFactory: LazyHubFactory) : SingletonSerializeAsToken(), FlowLogicRefFactory {
    // TODO: Replace with a per app classloader/cordapp provider/cordapp loader - this will do for now
    var classloader = javaClass.classLoader

    override fun create(flowClass: Class<out FlowLogic<*>>, vararg args: Any?): FlowLogicRef {
        if (!flowClass.isAnnotationPresent(SchedulableFlow::class.java)) {
            throw IllegalFlowLogicException(flowClass, "because it's not a schedulable flow")
        }
        return createForRPC(flowClass, *args)
    }

    override fun createForRPC(flowClass: Class<out FlowLogic<*>>, vararg args: Any?): FlowLogicRef {
        return createKotlin(flowClass, args.filterNotNull())
    }

    /**
     * Create a [FlowLogicRef] by trying to find a Kotlin constructor that matches the given args.
     *
     * TODO: Rethink language specific naming.
     */
    @VisibleForTesting
    internal fun createKotlin(type: Class<out FlowLogic<*>>, args: Collection<Any?>): FlowLogicRef {
        // Check we can find a constructor and populate the args to it, but don't call it
        createConstructor(type, args)
        return FlowLogicRefImpl(type.name, args)
    }

    override fun toFlowLogic(ref: FlowLogicRef): FlowLogic<*> {
        if (ref !is FlowLogicRefImpl) throw IllegalFlowLogicException(ref.javaClass, "FlowLogicRef was not created via correct FlowLogicRefFactory interface")
        val klass = Class.forName(ref.flowLogicClassName, true, classloader).asSubclass(FlowLogic::class.java)
        return createConstructor(klass, ref.args)()
    }

    private fun createConstructor(clazz: Class<out FlowLogic<*>>, args: Collection<Any?>): () -> FlowLogic<*> {
        val lh = lazyHubFactory.child()
        args.forEach {
            if (it != null) lh.obj(it)
        }
        lh.impl(clazz)
        try {
            return lh.getProvider(FlowLogic::class)
        } catch (e: RuntimeException) {
            throw IllegalFlowLogicException(clazz, "as could not find matching constructor for: $args")
        }
    }
}