package net.corda.lazyhub

import net.corda.core.internal.uncheckedCast
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Assert.assertSame
import org.junit.Test
import java.io.Closeable
import kotlin.reflect.KFunction
import kotlin.test.assertEquals
import kotlin.test.fail

class LazyHubTests {
    private val lh = lazyHub()

    class Config(val info: String)
    interface A
    interface B {
        val a: A
    }

    class AImpl(val config: Config) : A
    class BImpl(override val a: A) : B
    class Spectator {
        init {
            fail("Should not be instantiated.")
        }
    }

    @Test
    fun `basic functionality`() {
        val config = Config("woo")
        lh.obj(config)
        lh.impl(AImpl::class)
        lh.impl(BImpl::class)
        lh.impl(Spectator::class)
        val b = lh[B::class]
        // An impl is instantiated at most once per container:
        assertSame(b.a, lh[A::class])
        assertSame(b, lh[B::class])
        // More specific type to inspect config:
        val a = lh[AImpl::class]
        assertSame(b.a, a)
        assertSame(config, a.config)
    }

    private fun createA(config: Config): A = AImpl(config)
    @Test
    fun `factory works`() {
        lh.obj(Config("x"))
        lh.factory(this::createA) // Observe private is OK.
        assertSame(AImpl::class.java, lh[A::class].javaClass)
        // The factory declares A not AImpl as its return type:
        assertThatThrownBy { lh[AImpl::class] }.isInstanceOf(NoSuchProviderException::class.java)
    }

    private fun <X : Closeable, Y : X> ntv(a: Y) = a.toString()
    @Test
    fun `nested type variable`() {
        val ntv: Function1<Closeable, String> = this::ntv
        lh.factory(uncheckedCast<Any, KFunction<String>>(ntv))
        lh.obj(Closeable {})
        lh[String::class]
    }

    private fun spread(a: Int, b: Int) = "$a$b"
    private fun spread2(a: Int, b: Int = 34) = "$a$b"
    private fun spread3(a: Int = 12, b: Int) = "$a$b"
    private fun spread4(a: Int = 12, b: Int = 34, c: Int) = "$a$b$c"
    private fun spread5(a: Int?, b: Int?, c: Int) = "$a$b$c"
    private fun spread6(a: Int?, b: Int, c: Int) = "$a$b$c"
    @Test
    fun spreading() {
        lh.obj(12)
        lh.obj(34)
        lh.factory(this::spread)
        assertEquals("1234", lh[String::class])
    }

    @Test
    fun spreading2() {
        lh.obj(12)
        lh.factory(this::spread2)
        assertEquals("1234", lh[String::class])
    }

    @Test
    fun spreading3() {
        lh.obj(34)
        lh.factory(this::spread3)
        assertEquals("1234", lh[String::class])
    }

    @Test
    fun spreading4() {
        lh.obj(99)
        lh.obj(56)
        lh.factory(this::spread4)
        assertEquals("993456", lh[String::class])
    }

    @Test
    fun spreading5() {
        lh.obj(99)
        lh.obj(56)
        lh.factory(this::spread5)
        assertEquals("99null56", lh[String::class])
    }

    @Test
    fun spreading6() {
        lh.obj(12)
        lh.obj(34)
        lh.factory(this::spread6)
        assertEquals("null1234", lh[String::class])
    }

    open class NoPublicConstructor protected constructor()

    @Test
    fun `no public constructor kotlin`() {
        lh.impl(NoPublicConstructor::class)
        assertThatThrownBy { lh[NoPublicConstructor::class] }.isInstanceOf(NoPublicConstructorsException::class.java)
    }

    @Test
    fun `no public constructor java`() {
        lh.impl(NoPublicConstructor::class.java)
        assertThatThrownBy { lh[NoPublicConstructor::class] }.isInstanceOf(NoPublicConstructorsException::class.java)
    }
}
