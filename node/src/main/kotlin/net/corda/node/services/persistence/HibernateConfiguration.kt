package net.corda.node.services.persistence

import net.corda.core.internal.castIfPossible
import net.corda.core.node.services.IdentityService
import net.corda.core.schemas.MappedSchema
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.toHexString
import net.corda.node.services.api.SchemaService
import net.corda.node.utilities.DatabaseTransactionManager
import net.corda.node.utilities.parserTransactionIsolationLevel
import org.hibernate.SessionFactory
import org.hibernate.boot.MetadataSources
import org.hibernate.boot.model.naming.Identifier
import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder
import org.hibernate.cfg.Configuration
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment
import org.hibernate.service.UnknownUnwrapTypeException
import org.hibernate.type.AbstractSingleColumnStandardBasicType
import org.hibernate.type.descriptor.java.JavaTypeDescriptorRegistry
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor
import org.hibernate.type.descriptor.sql.BlobTypeDescriptor
import org.hibernate.type.descriptor.sql.VarbinaryTypeDescriptor
import java.sql.Connection
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class HibernateConfiguration(val schemaService: SchemaService, private val databaseProperties: Properties, private val createIdentityService: () -> IdentityService) {
    companion object {
        val logger = loggerFor<HibernateConfiguration>()
    }

    // TODO: make this a guava cache or similar to limit ability for this to grow forever.
    private val sessionFactories = ConcurrentHashMap<Set<MappedSchema>, SessionFactory>()

    private val transactionIsolationLevel = parserTransactionIsolationLevel(databaseProperties.getProperty("transactionIsolationLevel") ?: "")
    val sessionFactoryForRegisteredSchemas = schemaService.schemaOptions.keys.let {
        logger.info("Init HibernateConfiguration for schemas: $it")
        // Register the AbstractPartyDescriptor so Hibernate doesn't warn when encountering AbstractParty. Unfortunately
        // Hibernate warns about not being able to find a descriptor if we don't provide one, but won't use it by default
        // so we end up providing both descriptor and converter. We should re-examine this in later versions to see if
        // either Hibernate can be convinced to stop warning, use the descriptor by default, or something else.
        JavaTypeDescriptorRegistry.INSTANCE.addDescriptor(AbstractPartyDescriptor(createIdentityService))
        sessionFactoryForSchemas(it)
    }

    /** @param key must be immutable, not just read-only. */
    fun sessionFactoryForSchemas(key: Set<MappedSchema>) = sessionFactories.computeIfAbsent(key, { makeSessionFactoryForSchemas(key) })

    private fun makeSessionFactoryForSchemas(schemas: Set<MappedSchema>): SessionFactory {
        logger.info("Creating session factory for schemas: $schemas")
        val serviceRegistry = BootstrapServiceRegistryBuilder().build()
        val metadataSources = MetadataSources(serviceRegistry)
        // We set a connection provider as the auto schema generation requires it.  The auto schema generation will not
        // necessarily remain and would likely be replaced by something like Liquibase.  For now it is very convenient though.
        // TODO: replace auto schema generation as it isn't intended for production use, according to Hibernate docs.
        val config = Configuration(metadataSources).setProperty("hibernate.connection.provider_class", NodeDatabaseConnectionProvider::class.java.name)
                .setProperty("hibernate.hbm2ddl.auto", if (databaseProperties.getProperty("initDatabase", "true") == "true") "update" else "validate")
                .setProperty("hibernate.format_sql", "true")
                .setProperty("hibernate.connection.isolation", transactionIsolationLevel.toString())

        schemas.forEach { schema ->
            // TODO: require mechanism to set schemaOptions (databaseSchema, tablePrefix) which are not global to session
            schema.mappedTypes.forEach { config.addAnnotatedClass(it) }
        }

        val sessionFactory = buildSessionFactory(config, metadataSources, databaseProperties.getProperty("serverNameTablePrefix", ""))
        logger.info("Created session factory for schemas: $schemas")
        return sessionFactory
    }

    private fun buildSessionFactory(config: Configuration, metadataSources: MetadataSources, tablePrefix: String): SessionFactory {
        config.standardServiceRegistryBuilder.applySettings(config.properties)
        val metadata = metadataSources.getMetadataBuilder(config.standardServiceRegistryBuilder.build()).run {
            applyPhysicalNamingStrategy(object : PhysicalNamingStrategyStandardImpl() {
                override fun toPhysicalTableName(name: Identifier?, context: JdbcEnvironment?): Identifier {
                    val default = super.toPhysicalTableName(name, context)
                    return Identifier.toIdentifier(tablePrefix + default.text, default.isQuoted)
                }
            })
            // register custom converters
            applyAttributeConverter(AbstractPartyToX500NameAsStringConverter(createIdentityService))
            // Register a tweaked version of `org.hibernate.type.MaterializedBlobType` that truncates logged messages.
            // to avoid OOM when large blobs might get logged.
            applyBasicType(CordaMaterializedBlobType, CordaMaterializedBlobType.name)
            applyBasicType(CordaWrapperBinaryType, CordaWrapperBinaryType.name)
            build()
        }

        return metadata.sessionFactoryBuilder.run {
            allowOutOfTransactionUpdateOperations(true)
            applySecondLevelCacheSupport(false)
            applyQueryCacheSupport(false)
            enableReleaseResourcesOnCloseEnabled(true)
            build()
        }
    }

    // Supply Hibernate with connections from our underlying Exposed database integration.  Only used
    // during schema creation / update.
    class NodeDatabaseConnectionProvider : ConnectionProvider {
        override fun closeConnection(conn: Connection) {
            conn.autoCommit = false
            val tx = DatabaseTransactionManager.current()
            tx.commit()
            tx.close()
        }

        override fun supportsAggressiveRelease(): Boolean = true

        override fun getConnection(): Connection {
            return DatabaseTransactionManager.newTransaction().connection
        }

        override fun <T : Any?> unwrap(unwrapType: Class<T>): T {
            return unwrapType.castIfPossible(this) ?: throw UnknownUnwrapTypeException(unwrapType)
        }

        override fun isUnwrappableAs(unwrapType: Class<*>?): Boolean = unwrapType == NodeDatabaseConnectionProvider::class.java
    }

    // A tweaked version of `org.hibernate.type.MaterializedBlobType` that truncates logged messages.  Also logs in hex.
    private object CordaMaterializedBlobType : AbstractSingleColumnStandardBasicType<ByteArray>(BlobTypeDescriptor.DEFAULT, CordaPrimitiveByteArrayTypeDescriptor) {
        override fun getName(): String {
            return "materialized_blob"
        }
    }

    // A tweaked version of `org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor` that truncates logged messages.
    private object CordaPrimitiveByteArrayTypeDescriptor : PrimitiveByteArrayTypeDescriptor() {
        private val LOG_SIZE_LIMIT = 1024

        override fun extractLoggableRepresentation(value: ByteArray?): String {
            return if (value == null) super.extractLoggableRepresentation(value) else {
                if (value.size <= LOG_SIZE_LIMIT) {
                    return "[size=${value.size}, value=${value.toHexString()}]"
                } else {
                    return "[size=${value.size}, value=${value.copyOfRange(0, LOG_SIZE_LIMIT).toHexString()}...truncated...]"
                }
            }
        }
    }

    // A tweaked version of `org.hibernate.type.WrapperBinaryType` that deals with ByteArray (java primitive byte[] type).
    private object CordaWrapperBinaryType : AbstractSingleColumnStandardBasicType<ByteArray>(VarbinaryTypeDescriptor.INSTANCE, PrimitiveByteArrayTypeDescriptor.INSTANCE) {
        override fun getRegistrationKeys(): Array<String> {
            return arrayOf(name, "ByteArray", ByteArray::class.java.name)
        }

        override fun getName(): String {
            return "corda-wrapper-binary"
        }
    }
}