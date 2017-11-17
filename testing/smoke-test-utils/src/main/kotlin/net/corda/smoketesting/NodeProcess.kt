package net.corda.smoketesting

import net.corda.client.rpc.CordaRPCClient
import net.corda.client.rpc.CordaRPCConnection
import net.corda.client.rpc.internal.KryoClientSerializationScheme
import net.corda.core.internal.copyTo
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.internal.NetworkParametersCopier
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.common.internal.asContextEnv
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.ZoneId.systemDefault
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class NodeProcess(
        private val config: NodeConfig,
        private val nodeDir: Path,
        private val node: Process,
        private val client: CordaRPCClient
) : AutoCloseable {
    private companion object {
        val log = loggerFor<NodeProcess>()
    }

    fun connect(): CordaRPCConnection {
        val user = config.users[0]
        return client.start(user.username, user.password)
    }

    override fun close() {
        log.info("Stopping node '${config.commonName}'")
        node.destroy()
        if (!node.waitFor(60, SECONDS)) {
            log.warn("Node '${config.commonName}' has not shutdown correctly")
            node.destroyForcibly()
        }

        log.info("Deleting Artemis directories, because they're large!")
        (nodeDir / "artemis").toFile().deleteRecursively()
    }

    class Factory(
            private val buildDirectory: Path = Paths.get("build"),
            private val cordaJar: Path = Paths.get(this::class.java.getResource("/corda.jar").toURI())
    ) {
        private companion object {
            val javaPath: Path = Paths.get(System.getProperty("java.home"), "bin", "java")
            val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(systemDefault())
            val defaultNetworkParameters = run {
                KryoClientSerializationScheme.createSerializationEnv().asContextEnv {
                    // There are no notaries in the network parameters for smoke test nodes. If this is required then we would
                    // need to introduce the concept of a "network" which predefines the notaries, like the driver and MockNetwork
                    NetworkParametersCopier(testNetworkParameters(emptyList()))
                }
            }

            init {
                try {
                    Class.forName("net.corda.node.Corda")
                    throw Error("Smoke test has the node in its classpath. Please remove the offending dependency.")
                } catch (e: ClassNotFoundException) {
                    // If the class can't be found then we're good!
                }
            }
        }

        private val nodesDirectory = (buildDirectory / formatter.format(Instant.now())).createDirectories()

        fun baseDirectory(config: NodeConfig): Path = nodesDirectory / config.commonName

        fun create(config: NodeConfig): NodeProcess {
            val nodeDir = baseDirectory(config).createDirectories()
            log.info("Node directory: {}", nodeDir)

            config.toText().byteInputStream().copyTo(nodeDir / "node.conf")
            defaultNetworkParameters.install(nodeDir)

            val process = startNode(nodeDir)
            val client = CordaRPCClient(NetworkHostAndPort("localhost", config.rpcPort))
            val user = config.users[0]

            val setupExecutor = Executors.newSingleThreadScheduledExecutor()
            try {
                setupExecutor.scheduleWithFixedDelay({
                    try {
                        if (!process.isAlive) {
                            log.error("Node '${config.commonName}' has died.")
                            return@scheduleWithFixedDelay
                        }
                        val conn = client.start(user.username, user.password)
                        conn.close()

                        // Cancel the "setup" task now that we've created the RPC client.
                        setupExecutor.shutdown()
                    } catch (e: Exception) {
                        log.warn("Node '{}' not ready yet (Error: {})", config.commonName, e.message)
                    }
                }, 5, 1, SECONDS)

                val setupOK = setupExecutor.awaitTermination(120, SECONDS)
                check(setupOK && process.isAlive) { "Failed to create RPC connection" }
            } catch (e: Exception) {
                process.destroyForcibly()
                throw e
            } finally {
                setupExecutor.shutdownNow()
            }

            return NodeProcess(config, nodeDir, process, client)
        }

        private fun startNode(nodeDir: Path): Process {
            val builder = ProcessBuilder()
                    .command(javaPath.toString(), "-Dcapsule.log=verbose", "-jar", cordaJar.toString())
                    .directory(nodeDir.toFile())
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)

            builder.environment().putAll(mapOf(
                    "CAPSULE_CACHE_DIR" to (buildDirectory / "capsule").toString()
            ))

            return builder.start()
        }
    }
}
