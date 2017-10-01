import net.corda.lazyhub.lazyHub
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Assert.assertSame
import org.junit.Test
import kotlin.test.fail

class LazyHubTests {
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
        val c = lazyHub()
        val config = Config("woo")
        c.obj(config)
        c.impl(AImpl::class)
        c.impl(BImpl::class)
        c.impl(Spectator::class)
        val b = c[B::class]
        // An impl is instantiated at most once per container:
        assertSame(b.a, c[A::class])
        assertSame(b, c[B::class])
        // More specific type to inspect config:
        val a = c[AImpl::class]
        assertSame(b.a, a)
        assertSame(config, a.config)
    }

    private fun createA(config: Config): A = AImpl(config)
    @Test
    fun `factory works`() {
        val c = lazyHub()
        c.obj(Config("x"))
        c.factory(this::createA) // Observe private is OK.
        assertSame(AImpl::class.java, c[A::class].javaClass)
        // The factory declares A not AImpl as its return type:
        assertThatThrownBy { c[AImpl::class] }.isInstanceOf(KotlinNullPointerException::class.java)
    }
}
