package net.corda.node.services.network

import net.corda.core.utilities.minutes
import net.corda.core.utilities.seconds
import net.corda.testing.ALICE
import net.corda.testing.BOB
import net.corda.testing.driver.driver
import net.corda.testing.node.services.network.NetworkMapServer
import org.assertj.core.api.Assertions
import org.junit.Test
import kotlin.test.assertEquals

class NetworkMapClientTest {
    @Test
    fun `nodes can see each other using the http network map`() {
        val networkMapServer = NetworkMapServer(1.minutes)
        val networkMapURL = "http://${networkMapServer.hostAndPort.host}:${networkMapServer.hostAndPort.port}"
        driver(globalOverride = mapOf("compatibilityZoneURL" to networkMapURL)) {
            val alice = startNode(providedName = ALICE.name)
            val bob = startNode(providedName = BOB.name)

            val notaryNode = defaultNotaryNode.get()
            val aliceNode = alice.get()
            val bobNode = bob.get()

            val notaryNetworkMapCache = notaryNode.rpc.networkMapSnapshot()
            val aliceNetworkMapCache = aliceNode.rpc.networkMapSnapshot()
            val bobNetworkMapCache = bobNode.rpc.networkMapSnapshot()

            assertEquals(3, notaryNetworkMapCache.size)
            assertEquals(3, aliceNetworkMapCache.size)
            assertEquals(3, bobNetworkMapCache.size)

            Assertions.assertThat(notaryNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(aliceNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(bobNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
        }
    }

    @Test
    fun `nodes process network map add updates correctly`() {
        val networkMapServer = NetworkMapServer(1.seconds)
        val networkMapURL = "http://${networkMapServer.hostAndPort.host}:${networkMapServer.hostAndPort.port}"
        driver(globalOverride = mapOf("compatibilityZoneURL" to networkMapURL)) {
            val alice = startNode(providedName = ALICE.name)

            val notaryNode = defaultNotaryNode.get()
            val aliceNode = alice.get()

            val notaryNetworkMapCache = notaryNode.rpc.networkMapSnapshot()
            val aliceNetworkMapCache = aliceNode.rpc.networkMapSnapshot()

            assertEquals(2, notaryNetworkMapCache.size)
            assertEquals(2, aliceNetworkMapCache.size)
            Assertions.assertThat(notaryNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo))
            Assertions.assertThat(aliceNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo))

            val bob = startNode(providedName = BOB.name)
            val bobNode = bob.get()
            val bobNetworkMapCache = bobNode.rpc.networkMapSnapshot()
            val notaryNetworkMapCache2 = notaryNode.rpc.networkMapSnapshot()
            val aliceNetworkMapCache2 = aliceNode.rpc.networkMapSnapshot()

            // Wait for network map client to poll for the next update.
            Thread.sleep(1.seconds.toMillis())

            assertEquals(3, notaryNetworkMapCache2.size)
            assertEquals(3, aliceNetworkMapCache2.size)
            assertEquals(3, bobNetworkMapCache.size)
            Assertions.assertThat(notaryNetworkMapCache2).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(aliceNetworkMapCache2).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(bobNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
        }
    }

    @Test
    fun `nodes process network map remove updates correctly`() {
        val networkMapServer = NetworkMapServer(1.seconds)
        val networkMapURL = "http://${networkMapServer.hostAndPort.host}:${networkMapServer.hostAndPort.port}"
        driver(globalOverride = mapOf("compatibilityZoneURL" to networkMapURL)) {
            val alice = startNode(providedName = ALICE.name, startInSameProcess = true)
            val bob = startNode(providedName = BOB.name)

            val notaryNode = defaultNotaryNode.get()
            val aliceNode = alice.get()
            val bobNode = bob.get()

            val notaryNetworkMapCache = notaryNode.rpc.networkMapSnapshot()
            val aliceNetworkMapCache = aliceNode.rpc.networkMapSnapshot()
            val bobNetworkMapCache = bobNode.rpc.networkMapSnapshot()

            assertEquals(3, notaryNetworkMapCache.size)
            assertEquals(3, aliceNetworkMapCache.size)
            assertEquals(3, bobNetworkMapCache.size)
            Assertions.assertThat(notaryNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(aliceNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(bobNetworkMapCache).containsAll(listOf(notaryNode.nodeInfo, aliceNode.nodeInfo, bobNode.nodeInfo))

            networkMapServer.removeNodeInfo(aliceNode.nodeInfo)

            // Wait for network map client to poll for the next update.
            Thread.sleep(1.seconds.toMillis())

            val notaryNetworkMapCache2 = notaryNode.rpc.networkMapSnapshot()
            val bobNetworkMapCache2 = bobNode.rpc.networkMapSnapshot()
            assertEquals(2, notaryNetworkMapCache2.size)
            assertEquals(2, bobNetworkMapCache2.size)
            Assertions.assertThat(notaryNetworkMapCache2).containsAll(listOf(notaryNode.nodeInfo, bobNode.nodeInfo))
            Assertions.assertThat(bobNetworkMapCache2).containsAll(listOf(notaryNode.nodeInfo, bobNode.nodeInfo))
        }
    }
}
