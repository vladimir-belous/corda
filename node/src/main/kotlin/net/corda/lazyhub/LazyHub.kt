package net.corda.lazyhub

import net.corda.core.internal.declaredField
import net.corda.core.internal.toTypedArray
import net.corda.core.internal.uncheckedCast
import org.bouncycastle.asn1.ua.DSTU4145NamedCurves.params
import java.lang.reflect.Method
import java.util.*
import java.util.stream.Collectors
import kotlin.jvm.internal.CallableReference
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter

fun lazyHub(): MutableLazyHub = LazyHubImpl(null)

interface LazyHubFactory {
    fun child(): MutableLazyHub
}

interface LazyHub : LazyHubFactory {
    operator fun <T : Any> get(clazz: KClass<T>) = get(clazz.java)
    operator fun <T> get(clazz: Class<T>): T
    fun <T : Any> getAll(clazz: KClass<T>) = getAll(clazz.java)
    fun <T> getAll(clazz: Class<T>): Array<T>
    fun count(clazz: Class<*>): Int
    fun count(clazz: KClass<*>) = count(clazz.java)
    fun <T> select(clazz: Class<T>): () -> T
    fun <T : Any> select(clazz: KClass<T>) = select(clazz.java)
}

interface MutableLazyHub : LazyHub {
    fun obj(obj: Any)
    fun <T : Any> obj(service: KClass<T>, obj: T)
    fun impl(impl: KClass<*>, vararg params: Any)
    fun impl(impl: Class<*>, vararg params: Any)
    fun <T : Any, U : T> impl(service: KClass<T>, impl: KClass<U>)
    fun <T : Any> factory(factory: KFunction<T>)
}

private class LazyHubImpl(private val parent: LazyHubImpl?) : MutableLazyHub {
    private val adapters = mutableMapOf<Class<*>, MutableList<Adapter<*>>>()
    private fun getOrCreateAdapters(type: Class<*>) = adapters[type] ?: mutableListOf<Adapter<*>>().also { adapters[type] = it }
    private fun addAdapter(seen: MutableSet<Class<*>>, type: Class<*>, adapter: Adapter<*>) {
        getOrCreateAdapters(type) += adapter
        type.interfaces.forEach {
            if (seen.add(it)) addAdapter(seen, it, adapter)
        }
        type.superclass?.let {
            if (seen.add(it)) addAdapter(seen, it, adapter)
        }
    }

    protected fun addAdapter(adapter: Adapter<*>) = addAdapter(mutableSetOf(), adapter.type, adapter)
    protected fun <T> findAdapters(clazz: Class<T>): List<Adapter<T>>? {
        adapters[clazz]?.let { return uncheckedCast(it) }
        return parent?.findAdapters(clazz)
    }

    protected fun dropAdapters(clazz: Class<*>) {
        adapters.iterator().let {
            while (it.hasNext()) {
                if (clazz.isAssignableFrom(it.next().key)) it.remove()
            }
        }
    }

    override fun <T> get(clazz: Class<T>): T = findAdapters(clazz)!!.single().instance
    override fun <T> getAll(clazz: Class<T>): Array<T> {
        val adapters = findAdapters(clazz) ?: throw NoSuchElementException()
        val array = java.lang.reflect.Array.newInstance(clazz, adapters.size)
        adapters.forEachIndexed { i, a -> java.lang.reflect.Array.set(array, i, a.instance) }
        return uncheckedCast(array)
    }

    override fun <T> select(clazz: Class<T>) = findAdapters(clazz)!!.single().let { { it.instance } }
    override fun count(clazz: Class<*>) = findAdapters(clazz)?.size ?: 0
    override fun child(): MutableLazyHub = LazyHubImpl(this)
    override fun obj(obj: Any) {
        addAdapter(ObjAdapter(obj))
    }

    override fun <T : Any> obj(service: KClass<T>, obj: T) {
        dropAdapters(service.java)
        obj(obj)
    }

    override fun impl(impl: KClass<*>, vararg params: Any) {
        addAdapter(KImplAdapter(this, impl))
    }

    override fun impl(impl: Class<*>, vararg params: Any) {
        addAdapter(ImplAdapter(this, impl))
    }

    override fun <T : Any, U : T> impl(service: KClass<T>, impl: KClass<U>) {
        dropAdapters(service.java)
        impl(impl)
    }

    private fun Class<*>.findMethod(name: String, params: Array<Class<*>>): Method = try {
        // TODO: Support internal methods.
        getDeclaredMethod(name, *params)
    } catch (e: NoSuchMethodException) {
        superclass.findMethod(name, params)
    }

    override fun <T : Any> factory(factory: KFunction<T>) {
        val params = factory.parameters.map(::KParam)
        // TODO: Support static factory methods.
        val receiver = factory.declaredField<Any>(CallableReference::class, "receiver").value
        val method = receiver.javaClass.findMethod(factory.name, params.stream().map { it.clazz }.toTypedArray()).apply { isAccessible = true }
        val type: Class<T> = uncheckedCast(method.returnType.let { if (it == Void.TYPE) Unit.javaClass else it })
        addAdapter(FactoryAdapter(type) {
            uncheckedCast(method.invoke(receiver, *jArgs(method, params)))
        })
    }

    internal fun jArgs(context: Any?, params: List<Param>) = params.stream().map {
        if (it.clazz.isArray) {
            if (0 != count(it.clazz.componentType)) {
                getAll(it.clazz.componentType)
            } else {
                it.unsatisfiable == Unsatisfiable.GIVE_UP && throw UnsatisfiableParamException("Unsatisfiable param ${it.clazz} in factory: $context")
                if (it.unsatisfiable==Unsatisfiable.NULL) null else Skip
            }
        } else if (0 != count(it.clazz)) {
            this[it.clazz]
        } else {
            it.unsatisfiable == Unsatisfiable.GIVE_UP && throw UnsatisfiableParamException("Unsatisfiable param ${it.clazz} in factory: $context")
            if (it.unsatisfiable==Unsatisfiable.NULL) null else Skip
        }
    }.toTypedArray()

    internal fun kArgs(context: Any?, params: List<KParam>) = params.map {
        Pair(it.p, if (it.clazz.isArray) {
            if (0 != count(it.clazz.componentType)) {
                getAll(it.clazz.componentType)
            } else {
                it.unsatisfiable == Unsatisfiable.GIVE_UP && throw UnsatisfiableParamException("Unsatisfiable param ${it.clazz} in factory: $context")
                if (it.unsatisfiable==Unsatisfiable.NULL) null else Skip
            }
        } else if (0 != count(it.clazz)) {
            this[it.clazz]
        } else {
            it.unsatisfiable == Unsatisfiable.GIVE_UP && throw UnsatisfiableParamException("Unsatisfiable param ${it.clazz} in factory: $context")
            if (it.unsatisfiable==Unsatisfiable.NULL) null else Skip
        })
    }.filter {it.second!=Skip}.toMap()
}
private object Skip
class UnsatisfiableParamException(message: String) : Exception(message)

private interface Adapter<T> {
    val type: Class<T>
    val instance: T
}

private class ObjAdapter<T : Any>(override val instance: T) : Adapter<T> {
    override val type get() = instance.javaClass
}

private open class FactoryAdapter<T>(override val type: Class<T>, private val factory: () -> T) : Adapter<T> {
    override val instance by lazy { factory() }
}

private class KImplAdapter<T : Any>(container: LazyHubImpl, type: KClass<T>) : FactoryAdapter<T>(type.java, {
    println(type)
    val satisfiable = type.constructors.mapNotNull { factory ->
        val params = factory.parameters.map(::KParam)
        val hmm = { factory.callBy(container.kArgs(factory, params)) }
        hmm
    }
    satisfiable.single().invoke()
})

private class ImplAdapter<T : Any>(container: LazyHubImpl, type: Class<T>) : FactoryAdapter<T>(type, {
    val satisfiable = type.constructors.mapNotNull { factory ->
        val params = factory.parameterTypes.map(::JParam)
        val method = type.getConstructor(*params.stream().map { it.clazz }.toTypedArray())
        Pair(method, params)
    }
    satisfiable.single().let { (c, p) -> c.newInstance(*container.jArgs(c, p)) }
})

private enum class Unsatisfiable {
    GIVE_UP, NULL, SKIP
}

private interface Param {
    val clazz: Class<*>
    val unsatisfiable: Unsatisfiable
}

private class KParam(val p:KParameter) : Param {
    // TODO: Get the parameter types without going via String.
    companion object {
        private val arrayPattern = "kotlin[.]Array<(.+)>".toRegex()
        private fun findClass(name: String): Class<*> {
            val m = arrayPattern.matchEntire(name)
            return if (m != null) {
                java.lang.reflect.Array.newInstance(findClass(m.groupValues[1]), 0).javaClass
            } else {
                findSimpleClass(name)
            }
        }

        private fun findSimpleClass(name: String): Class<*> = run {
            try {
                Class.forName(name)
            } catch (e: ClassNotFoundException) {
                val i = name.lastIndexOf('.')
                if (-1 == i) throw e
                findSimpleClass("${name.substring(0, i)}\$${name.substring(i + 1)}")
            }
        }
    }

    override val clazz = findClass(p.type.toString().substringBefore('?'))
    override val unsatisfiable = if (p.isOptional) Unsatisfiable.SKIP else if (p.type.isMarkedNullable) Unsatisfiable.NULL else Unsatisfiable.GIVE_UP
}

private class JParam(override val clazz: Class<*>) : Param {
    override val unsatisfiable get() = Unsatisfiable.GIVE_UP
}
