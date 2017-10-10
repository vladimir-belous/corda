package net.corda.webserver

import net.corda.core.utilities.NetworkHostAndPort
import net.corda.nodeapi.User
import net.corda.nodeapi.config.NodeSSLConfiguration
import java.nio.file.Path

/**
 * [baseDirectory] is not retrieved from the config file but rather from a command line argument.
 */
data class WebServerConfig(
        override val baseDirectory: Path,
        override val keyStorePassword: String,
        override val trustStorePassword: String,
        val useHTTPS: Boolean,
        val myLegalName: String,
        val rpcAddress: NetworkHostAndPort,
        val webAddress: NetworkHostAndPort,
        val rpcUsers: List<User>
) : NodeSSLConfiguration {
    val exportJMXto: String get() = "http"
}
