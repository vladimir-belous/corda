package net.corda.lazyhub

import net.corda.core.internal.declaredField
import net.corda.core.internal.filterNotNull
import net.corda.core.internal.toTypedArray
import net.corda.core.internal.uncheckedCast
import net.corda.core.serialization.CordaSerializable
import java.lang.reflect.Constructor
import java.lang.reflect.ParameterizedType
import java.lang.reflect.TypeVariable
import java.util.*
import java.util.stream.Stream
import kotlin.collections.LinkedHashMap
import kotlin.reflect.*
import kotlin.reflect.jvm.internal.ReflectProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.streams.toList

private fun <T> Stream<T>.toTypedArray(ct: Class<T>): Array<T> = toArray { n -> uncheckedCast<Any, Array<T?>>(java.lang.reflect.Array.newInstance(ct, n)) }
private fun <K, V> Stream<Pair<K, V>>.toMap() = collect<LinkedHashMap<K, V>>(::LinkedHashMap, { m, (k, v) -> m.put(k, v) }, { _, _ -> throw UnsupportedOperationException() })
private fun Class<*>.complement() = if (isPrimitive) {
    java.lang.reflect.Array.get(java.lang.reflect.Array.newInstance(this, 1), 0).javaClass
} else try {
    getField("TYPE").get(null) as Class<*>
} catch (e: NoSuchFieldException) {
    null
}

private fun KType.toClass(): Class<*> {
    var jType = declaredField<ReflectProperties.Val<*>>("javaType\$delegate").value.invoke() // TODO: Cache Field object.
    if (jType == Void.TYPE) return Unit::class.java
    while (true) {
        when (jType) {
            is Class<*> -> return jType
            is TypeVariable<*> -> jType = jType.bounds.single() // TODO: What if more than one?
            is ParameterizedType -> jType = jType.rawType
        }
    }
}

fun lazyHub(): MutableLazyHub = LazyHubImpl(null)

@CordaSerializable
open class LazyHubException(message: String) : Exception(message)

class NoSuchProviderException(clazz: Class<*>) : LazyHubException(clazz.toString())
class TooManyProvidersException(clazz: Class<*>) : LazyHubException(clazz.toString())
class UnsatisfiableParamException(message: String) : LazyHubException(message)
class NoPublicConstructorsException(message: String) : LazyHubException(message)
interface LazyHubFactory {
    fun child(): MutableLazyHub
}

interface LazyHub : LazyHubFactory {
    operator fun <T : Any> get(clazz: KClass<T>) = get(clazz.java)
    operator fun <T> get(clazz: Class<T>) = getOrNull(clazz) ?: throw NoSuchProviderException(clazz)
    fun <T : Any> getAll(clazz: KClass<T>) = getAll(clazz.java)
    fun <T> getAll(clazz: Class<T>): Array<T>
    fun <T : Any> getOrNull(clazz: KClass<T>) = getOrNull(clazz.java)
    fun <T> getOrNull(clazz: Class<T>): T?
    fun <T : Any> getProvider(clazz: KClass<T>) = getProvider(clazz.java)
    fun <T> getProvider(clazz: Class<T>): () -> T
}

interface MutableLazyHub : LazyHub {
    fun obj(obj: Any)
    fun <T : Any> obj(service: KClass<T>, obj: T)
    fun impl(impl: KClass<*>)
    fun impl(impl: Class<*>)
    fun <S : Any, T : S> impl(service: KClass<S>, impl: KClass<T>)
    fun <T> factory(factory: KFunction<T>)
}

private infix fun Class<*>.satisfiedBy(clazz: Class<*>): Boolean {
    return isAssignableFrom(clazz) || complement() == clazz
}

private fun <T> KFunction<T>.callWith(argSuppliers: List<Pair<KParam, ArgSupplier>>): T {
    return callBy(argSuppliers.stream().map { (param, supplier) -> Pair(param.kParam, supplier()) }.toMap())
}

private class LazyHubImpl(private val parent: LazyHubImpl?) : MutableLazyHub {
    private val providers = mutableMapOf<Class<*>, MutableList<Provider<*>>>()
    private fun add(seen: MutableSet<Class<*>>, type: Class<*>, provider: Provider<*>) {
        providers[type]?.add(provider) ?: providers.put(type, mutableListOf(provider))
        Stream.concat(Arrays.stream(type.interfaces), Stream.of(type.superclass, type.complement()).filterNotNull()).forEach {
            if (seen.add(it)) add(seen, it, provider) // TODO: Enforce seen.
        }
    }

    private fun add(provider: Provider<*>) = add(mutableSetOf(), provider.type, provider)
    private fun <T> findProviders(clazz: Class<T>): List<Provider<T>>? = uncheckedCast(providers[clazz]) ?: parent?.findProviders(clazz)
    private fun dropAll(clazz: Class<*>) = providers.iterator().run { while (hasNext()) if (clazz satisfiedBy next().key) remove() }
    override fun <T> getOrNull(clazz: Class<T>) = ((findProviders(clazz) ?: throw NoSuchProviderException(clazz)).singleOrNull() ?: throw TooManyProvidersException(clazz)).obj
    override fun <T> getAll(clazz: Class<T>) = (findProviders(clazz) ?: throw NoSuchProviderException(clazz)).stream().map { it.obj }.toTypedArray(clazz)
    override fun <T> getProvider(clazz: Class<T>) = ((findProviders(clazz) ?: throw NoSuchProviderException(clazz)).singleOrNull() ?: throw TooManyProvidersException(clazz)).run { { obj } }
    override fun child(): MutableLazyHub = LazyHubImpl(this)
    override fun obj(obj: Any) = add(ConstProvider(obj))
    override fun <T : Any> obj(service: KClass<T>, obj: T) = run { dropAll(service.java); obj(obj) }
    override fun impl(impl: KClass<*>) = add(kImplProvider(this, impl))
    override fun impl(impl: Class<*>) = add(jImplProvider(this, impl))
    override fun <S : Any, T : S> impl(service: KClass<S>, impl: KClass<T>) = run { dropAll(service.java); impl(impl) }
    override fun <T> factory(factory: KFunction<T>) {
        factory.isAccessible = true
        val params = factory.parameters.map(::KParam)
        add(LazyProvider(uncheckedCast(factory.returnType.toClass())) { factory.callWith(argSuppliers(factory, params)) })
    }

    internal fun <P : Param> argSuppliers(function: Any, params: List<P>) = run {
        val (consumed, unconsumed) = (1..2).map { mutableSetOf<Provider<*>>() }
        val argSuppliers = params.mapTo(ArrayList(params.size)) { param ->
            if (param.type.isArray) {
                fun <T> arraySupplier(componentType: Class<T>) = findProviders(componentType)?.let {
                    ArgSupplier(arrayProvider(uncheckedCast(param.type), componentType, it))
                }
                arraySupplier(param.type.componentType)
            } else {
                findProviders(param.type)?.run {
                    val i = indexOfFirst { consumed.add(it) }
                    if (i != -1) {
                        unconsumed.addAll(subList(i + 1, size))
                        val provider = get(i)
                        unconsumed -= provider
                        ArgSupplier(provider)
                    } else null
                }
            }
        }
        unconsumed.isEmpty() || throw IllegalStateException("Unconsumed $unconsumed for function: $function")
        fun tryStealingIfNeeded(index: Int) {
            params[index].unsatisfiableHandler != forgetAboutIt && return
            (index - 1 downTo 0).forEach { leftIndex ->
                val leftSupplier = argSuppliers[leftIndex]
                if (leftSupplier?.provider != null && params[index].type satisfiedBy leftSupplier.provider.type) {
                    argSuppliers[index] = leftSupplier
                    argSuppliers[leftIndex] = null
                    tryStealingIfNeeded(leftIndex)
                    return
                }
            }
        }
        (0 until params.size).forEach { argSuppliers[it] ?: tryStealingIfNeeded(it) }
        params.zip(argSuppliers).mapNotNull { (param, supplier) ->
            (supplier ?: param.unsatisfiableHandler(function, param))?.let { Pair(param, it) }
        }
    }

    internal fun <C : Any, P : Param> greediestSatisfiableConstructor(type: Any, constructors: Stream<C>, getParams: (C) -> List<P>) = run {
        var fail: UnsatisfiableParamException? = null
        val satisfiable = constructors.map { constructor ->
            val params = getParams(constructor)
            try {
                Pair(constructor, argSuppliers(constructor, params))
            } catch (e: UnsatisfiableParamException) {
                fail?.addSuppressed(e) ?: run { fail = e }
                null
            }
        }.filterNotNull().toList()
        satisfiable.isEmpty() && throw fail ?: NoPublicConstructorsException("No public constructors: $type")
        fun Pair<*, List<Pair<*, ArgSupplier>>>.providerCount() = second.stream().filter { (_, supplier) -> supplier.provider != null }.count()
        val greediest = mutableListOf(satisfiable[0])
        var providerCount = greediest[0].providerCount()
        satisfiable.stream().skip(1).forEach next@ {
            val pc = it.providerCount()
            if (pc < providerCount) return@next
            if (pc > providerCount) {
                greediest.clear()
                providerCount = pc
            }
            greediest += it
        }
        greediest.single()
    }
}

/** Like [Provider] but capable of supplying null. */
private class ArgSupplier(val provider: Provider<*>?) {
    companion object {
        val NULL = ArgSupplier(null)
    }

    operator fun invoke() = provider?.obj
}

private interface Provider<T> {
    /** Most specific known type. */
    val type: Class<T>
    val obj: T
}

private class ConstProvider<T : Any>(override val obj: T) : Provider<T> {
    override val type get() = obj.javaClass
    override fun toString() = "${javaClass.simpleName}($obj)"
}

private class LazyProvider<T>(override val type: Class<T>, private val factory: () -> T) : Provider<T> {
    override val obj by lazy { factory() }
}

private fun <T> arrayProvider(type: Class<Array<T>>, componentType: Class<T>, providers: List<Provider<T>>) = LazyProvider(type) {
    providers.stream().map { it.obj }.toTypedArray(componentType)
}

private fun <T : Any> kImplProvider(container: LazyHubImpl, type: KClass<T>) = LazyProvider(type.java) {
    val (constructor, argSuppliers) = container.greediestSatisfiableConstructor(
            type,
            type.constructors.stream().filter { it.visibility == KVisibility.PUBLIC },
            { it.parameters.map(::KParam) })
    constructor.callWith(argSuppliers)
}

private fun <T> jImplProvider(container: LazyHubImpl, type: Class<T>) = LazyProvider(type) {
    val (constructor, argSuppliers) = container.greediestSatisfiableConstructor(
            type,
            Arrays.stream<Constructor<T>>(uncheckedCast(type.constructors)),
            { it.parameterTypes.map(::JParam) })
    constructor.newInstance(*argSuppliers.stream().map { (_, supplier) -> supplier() }.toTypedArray())
}
private typealias UnsatisfiableParamHandler = (Any, Param) -> ArgSupplier?
private val forgetAboutIt: UnsatisfiableParamHandler = { function, param -> throw UnsatisfiableParamException("Unsatisfiable param $param in function: $function") }
private val passInNull: UnsatisfiableParamHandler = { _, _ -> ArgSupplier.NULL }
private val passInNothing: UnsatisfiableParamHandler = { _, _ -> null }

private interface Param {
    val type: Class<*>
    val unsatisfiableHandler: UnsatisfiableParamHandler
}

private class KParam(val kParam: KParameter) : Param {
    override val type = kParam.type.toClass()
    // If it's nullable and has a default value, use the default value:
    override val unsatisfiableHandler = if (kParam.isOptional) passInNothing else if (kParam.type.isMarkedNullable) passInNull else forgetAboutIt

    override fun toString() = kParam.toString()
}

private class JParam(override val type: Class<*>) : Param {
    override val unsatisfiableHandler get() = forgetAboutIt
    override fun toString() = type.toString()
}
