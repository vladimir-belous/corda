package net.corda.testing.node.services.network

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.net.HostAndPort
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignedData
import net.corda.core.crypto.sha256
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.HandlerCollection
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer
import java.io.Closeable
import java.io.InputStream
import java.net.InetSocketAddress
import java.time.Duration
import javax.ws.rs.*
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.ok

class NetworkMapServer(cacheTimeout: Duration) : Closeable {
    private val server: Server

    val hostAndPort: HostAndPort
        get() {
            return server.connectors.mapNotNull { it as? ServerConnector }.first().let { HostAndPort.fromParts(it.host, it.localPort) }
        }

    private val service = InMemoryNetworkMapService(cacheTimeout)

    init {
        server = Server(InetSocketAddress("localhost", 0)).apply {
            handler = HandlerCollection().apply {
                addHandler(ServletContextHandler().apply {
                    contextPath = "/"
                    val resourceConfig = ResourceConfig().apply {
                        // Add your API provider classes (annotated for JAX-RS) here
                        register(service)
                    }
                    val jerseyServlet = ServletHolder(ServletContainer(resourceConfig)).apply { initOrder = 0 }// Initialise at server start
                    addServlet(jerseyServlet, "/*")
                })
            }
        }
        server.start()
    }

    fun removeNodeInfo(nodeInfo:NodeInfo){
        service.removeNodeInfo(nodeInfo)
    }

    override fun close() {
        server.stop()
    }

    @Path("network-map")
    class InMemoryNetworkMapService(private val cacheTimeout: Duration) {
        private val nodeInfoMap = mutableMapOf<SecureHash, NodeInfo>()
        @POST
        @Path("publish")
        @Consumes(MediaType.APPLICATION_OCTET_STREAM)
        fun publishNodeInfo(input: InputStream): Response {
            val registrationData = input.readBytes().deserialize<SignedData<NodeInfo>>()
            val nodeInfo = registrationData.verified()
            val nodeInfoHash = nodeInfo.serialize().sha256()
            nodeInfoMap.put(nodeInfoHash, nodeInfo)
            return ok().build()
        }

        @GET
        @Produces(MediaType.APPLICATION_JSON)
        fun getNetworkMap(): Response {
            return Response.ok(ObjectMapper().writeValueAsString(nodeInfoMap.keys.map { it.toString() })).header("Cache-Control", "max-age=${cacheTimeout.seconds}").build()
        }

        @GET
        @Path("{var}")
        @Produces(MediaType.APPLICATION_OCTET_STREAM)
        fun getNodeInfo(@PathParam("var") nodeInfoHash: String): Response {
            val nodeInfo = nodeInfoMap[SecureHash.parse(nodeInfoHash)]
            return if (nodeInfo != null) {
                Response.ok(nodeInfo.serialize().bytes)
            } else {
                Response.status(Response.Status.NOT_FOUND)
            }.build()
        }

        @GET
        @Path("my-hostname")
        fun getHostName(): Response {
            return Response.ok("test.host.name").build()
        }
        // Remove nodeInfo for testing.
        fun removeNodeInfo(nodeInfo: NodeInfo){
            nodeInfoMap.remove(nodeInfo.serialize().hash)
        }
    }
}
