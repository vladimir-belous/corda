package net.corda.core

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.CordaX500Name
import net.corda.core.internal.copyToDirectory
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.internal.list
import net.corda.core.messaging.startFlow
import net.corda.core.utilities.getOrThrow
import net.corda.nodeapi.User
import net.corda.smoketesting.NodeConfig
import net.corda.smoketesting.NodeProcess
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger
import kotlin.streams.toList

class NodeVersioningTest {
    private companion object {
        val user = User("user1", "test", permissions = setOf("ALL"))
        val port = AtomicInteger(15100)
    }

    private val factory = NodeProcess.Factory()

    private val aliceConfig = NodeConfig(
            legalName = CordaX500Name(organisation = "Alice Corp", locality = "Madrid", country = "ES"),
            p2pPort = port.andIncrement,
            rpcPort = port.andIncrement,
            webPort = port.andIncrement,
            isNotary = false,
            users = listOf(user)
    )

    @Test
    fun `platform version`() {
        val cordappsDir = (factory.baseDirectory(aliceConfig) / NodeProcess.CORDAPPS_DIR_NAME).createDirectories()
        // Find the jar file for the smoke tests of this module
        val selfCordapp = Paths.get("build", "libs").list {
            it.filter { "-smokeTests" in it.toString() }.toList().single()
        }
        selfCordapp.copyToDirectory(cordappsDir)

        val expectedPlatformVersion = 2
        factory.create(aliceConfig).use { alice ->
            alice.connect().use {
                val rpc = it.proxy
                assertThat(rpc.protocolVersion).isEqualTo(expectedPlatformVersion)
                assertThat(rpc.nodeInfo().platformVersion).isEqualTo(expectedPlatformVersion)
                assertThat(rpc.startFlow(NodeVersioningTest::GetPlatformVersionFlow).returnValue.getOrThrow()).isEqualTo(expectedPlatformVersion)
            }
        }
    }

    @StartableByRPC
    class GetPlatformVersionFlow : FlowLogic<Int>() {
        @Suspendable
        override fun call(): Int = serviceHub.myInfo.platformVersion
    }
}
