package net.corda.node.internal

import com.codahale.metrics.MetricRegistry
import com.google.common.collect.MutableClassToInstanceMap
import com.google.common.util.concurrent.MoreExecutors
import net.corda.confidential.SwapIdentitiesFlow
import net.corda.confidential.SwapIdentitiesHandler
import net.corda.core.CordaException
import net.corda.core.concurrent.CordaFuture
import net.corda.core.cordapp.CordappProvider
import net.corda.core.flows.*
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.*
import net.corda.core.internal.concurrent.doneFuture
import net.corda.core.internal.concurrent.flatMap
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.internal.toX509CertHolder
import net.corda.core.internal.uncheckedCast
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.RPCOps
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.node.*
import net.corda.core.messaging.*
import net.corda.core.node.AppServiceHub
import net.corda.core.node.NodeInfo
import net.corda.core.node.ServiceHub
import net.corda.core.node.StateLoader
import net.corda.core.node.services.*
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.debug
import net.corda.node.VersionInfo
import net.corda.lazyhub.LazyHub
import net.corda.lazyhub.MutableLazyHub
import net.corda.lazyhub.lazyHub
import net.corda.node.internal.classloading.requireAnnotation
import net.corda.node.internal.cordapp.CordappLoader
import net.corda.node.internal.cordapp.CordappProviderImpl
import net.corda.node.internal.cordapp.CordappProviderInternal
import net.corda.node.services.ContractUpgradeHandler
import net.corda.node.services.FinalityHandler
import net.corda.node.services.NotaryChangeHandler
import net.corda.node.services.api.*
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.NotaryConfig
import net.corda.node.services.config.configureWithDevSSLCertificate
import net.corda.node.services.events.FlowStarterInternal
import net.corda.node.services.events.NodeSchedulerService
import net.corda.node.services.events.ScheduledActivityObserver
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.PersistentKeyManagementService
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.messaging.sendRequest
import net.corda.node.services.network.*
import net.corda.node.services.network.NetworkMapService.RegistrationRequest
import net.corda.node.services.network.NetworkMapService.RegistrationResponse
import net.corda.node.services.persistence.DBCheckpointStorage
import net.corda.node.services.persistence.DBTransactionMappingStorage
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.persistence.NodeAttachmentService
import net.corda.node.services.schema.HibernateObserver
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.services.statemachine.*
import net.corda.node.services.transactions.*
import net.corda.node.services.upgrade.ContractUpgradeServiceImpl
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.services.vault.VaultSoftLockManager
import net.corda.node.utilities.*
import net.corda.node.utilities.AddOrRemove.ADD
import org.apache.activemq.artemis.utils.ReusableLatch
import org.slf4j.Logger
import rx.Observable
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.security.KeyPair
import java.security.KeyStoreException
import java.security.PublicKey
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.collections.set
import kotlin.reflect.KClass
import net.corda.core.crypto.generateKeyPair as cryptoGenerateKeyPair

class RPCFlows(cordappProvider: CordappProviderInternal) {
    val rpcFlows = cordappProvider.cordapps.flatMap { it.rpcFlows }
}

interface CordappServicesInternal : CordappServices
private class CordappServicesImpl : CordappServicesInternal {
    val cordappServices: MutableClassToInstanceMap<SerializeAsToken> = MutableClassToInstanceMap.create<SerializeAsToken>()
    override fun <T : SerializeAsToken> cordaService(type: Class<T>): T {
        require(type.isAnnotationPresent(CordaService::class.java)) { "${type.name} is not a Corda service" }
        return cordappServices.getInstance(type) ?: throw IllegalArgumentException("Corda service ${type.name} does not exist")
    }
}

class NotaryIdentity(val notaryIdentity: PartyAndCertificate) {
    val key get() = notaryIdentity.owningKey
}

/**
 * A base node implementation that can be customised either for production (with real implementations that do real
 * I/O), or a mock implementation suitable for unit test environments.
 *
 * Marked as SingletonSerializeAsToken to prevent the invisible reference to AbstractNode in the ServiceHub accidentally
 * sweeping up the Node into the Kryo checkpoint serialization via any flows holding a reference to ServiceHub.
 */
// TODO: Where this node is the initial network map service, currently no networkMapService is provided.
// In theory the NodeInfo for the node should be passed in, instead, however currently this is constructed by the
// AbstractNode. It should be possible to generate the NodeInfo outside of AbstractNode, so it can be passed in.
abstract class AbstractNode(config: NodeConfiguration,
                            val platformClock: Clock,
                            protected val versionInfo: VersionInfo,
                            protected val cordappLoader: CordappLoader,
                            @VisibleForTesting val busyNodeLatch: ReusableLatch = ReusableLatch()) : SingletonSerializeAsToken(), FlowFactorySource {
    open val configuration = config.apply {
        require(minimumPlatformVersion <= versionInfo.platformVersion) {
            "minimumPlatformVersion cannot be greater than the node's own version"
        }
    }

    class StartedNodeImpl<out N : AbstractNode>(
            override val internals: N,
            override val info: NodeInfo,
            override val checkpointStorage: CheckpointStorage,
            override val smm: StateMachineManager,
            override val attachments: NodeAttachmentService,
            override val inNodeNetworkMapService: NetworkMapService,
            override val network: MessagingService,
            override val database: CordaPersistence,
            override val rpcOps: CordaRPCOps,
            override val rpcFlows: RPCFlows,
            override val stateMachineRecordedTransactionMapping: StateMachineRecordedTransactionMappingStorage,
            override val metrics: MetricRegistry,
            override val schemaService: SchemaService,
            override val cordappProvider: CordappProviderInternal,
            override val notaryIdentity: NotaryIdentity? = null,
            serviceHub: ServiceHub,
            flowStarter: FlowStarter,
            nodeSchedulerService: NodeSchedulerService,
            netMapRegistration: NetMapRegistration) : StartedNode<N> {
        override val services: StartedNodeServices = object : StartedNodeServices, ServiceHub by serviceHub, FlowStarter by flowStarter {
            override val database get() = this@StartedNodeImpl.database
        }

        init {
            internals.runOnStop += network::stop
            // If we successfully  loaded network data from database, we set this future to Unit.
            internals._nodeReadyFuture.captureLater(netMapRegistration.future)
            database.transaction {
                smm.start()
                // Shut down the SMM so no Fibers are scheduled.
                internals.runOnStop += { smm.stop(internals.acceptableLiveFiberCountOnStop()) }
                nodeSchedulerService.start()
            }
        }
    }

    // TODO: Persist this, as well as whether the node is registered.
    /**
     * Sequence number of changes sent to the network map service, when registering/de-registering this node.
     */
    var networkMapSeq: Long = 1

    protected abstract val log: Logger
    protected abstract val networkMapAddress: SingleMessageRecipient?

    // We will run as much stuff in this single thread as possible to keep the risk of thread safety bugs low during the
    // low-performance prototyping period.
    protected abstract val serverThread: AffinityExecutor

    private val cordappServices = CordappServicesImpl()
    private val flowFactories = ConcurrentHashMap<Class<out FlowLogic<*>>, InitiatedFlowFactory<*>>()
    protected val partyKeys = mutableSetOf<KeyPair>()

    protected val runOnStop = ArrayList<() -> Any?>()
    protected val _nodeReadyFuture = openFuture<Unit>()
    /** Completes once the node has successfully registered with the network map service
     * or has loaded network map data from local database */
    val nodeReadyFuture: CordaFuture<Unit>
        get() = _nodeReadyFuture
    /** A [CordaX500Name] with null common name. */
    protected val myLegalName: CordaX500Name by lazy {
        val cert = loadKeyStore(configuration.nodeKeystore, configuration.keyStorePassword).getX509Certificate(X509Utilities.CORDA_CLIENT_CA)
        CordaX500Name.build(cert.subjectX500Principal).copy(commonName = null)
    }

    /** Set to non-null once [start] has been successfully called. */
    open val started get() = _started
    @Volatile private var _started: StartedNode<AbstractNode>? = null

    private fun saveOwnNodeInfo(info: NodeInfo, keyManagementService: KeyManagementService) {
        NodeInfoWatcher.saveToFile(configuration.baseDirectory, info, keyManagementService)
    }

    private fun initCertificate() {
        if (configuration.devMode) {
            log.warn("Corda node is running in dev mode.")
            configuration.configureWithDevSSLCertificate()
        }
        validateKeystore()
    }

    open fun generateNodeInfo() {
        check(started == null) { "Node has already been started" }
        initCertificate()
        log.info("Generating nodeInfo ...")
        val lh = lazyHub()
        configure(lh) // TODO: Trim this down.
        lh.factory(this::saveOwnNodeInfo)
        initialiseDatabasePersistence(lh) {
            lh.getAll(Unit::class) // Run side-effects, specifically saveOwnNodeInfo.
        }
    }

    open fun start(): StartedNode<AbstractNode> {
        check(started == null) { "Node has already been started" }
        initCertificate()
        log.info("Node starting up ...")
        val di = lazyHub()
        configure(di)
        // Override network map type if this node isn't running one:
        if (configuration.networkMapService != null) di.obj(NetworkMapService::class, NullNetworkMapService)
        // Do all of this in a database transaction so anything that might need a connection has one.
        return initialiseDatabasePersistence(di) { database ->
            di.obj(database)
            di.obj(database.hibernateConfig)
            di.getAll(Unit::class) // Run side-effects.
            di[StartedNode::class]
        }.also { _started = it }
    }

    private fun registerNotaries(notaries: Array<NotaryService>?) {
        notaries?.forEach(this::handleCustomNotaryService)
    }

    private fun registerStops(serverThread: ExecutorService?) {
        if (serverThread != null) {
            runOnStop += {
                // We wait here, even though any in-flight messages should have been drained away because the
                // server thread can potentially have other non-messaging tasks scheduled onto it. The timeout value is
                // arbitrary and might be inappropriate.
                MoreExecutors.shutdownAndAwaitTermination(serverThread, 50, SECONDS)
            }
        }
    }

    private class ServiceInstantiationException(cause: Throwable?) : CordaException("Service Instantiation Error", cause)

    open protected fun configure(di: MutableLazyHub) {
        di.obj(this)
        di.obj(di)
        di.obj(busyNodeLatch)
        di.obj(configuration)
        di.obj(cordappLoader)
        di.obj(platformClock)
        di.obj(serverThread)
        di.obj(cordappServices)
        di.obj(versionInfo)
        di.impl(AllIdentities::class)
        di.impl(ContractUpgradeServiceImpl::class)
        di.impl(CordaRPCOpsImpl::class)
        di.impl(CordappProviderImpl::class)
        di.impl(DBCheckpointStorage::class)
        di.impl(DBTransactionMappingStorage::class)
        di.impl(DummyAuditService::class)
        di.impl(FlowLogicRefFactoryImpl::class)
        di.impl(FlowStarterImpl::class)
        di.impl(MetricRegistry::class)
        di.impl(MonitoringService::class)
        di.impl(NetworkMapCacheImpl::class)
        di.impl(NodeAttachmentService::class)
        di.impl(NodeSchedulerService::class)
        di.impl(NodeSchemaService::class)
        di.impl(NodeVaultService::class)
        di.impl(PersistentNetworkMapCache::class)
        di.impl(RPCFlows::class)
        di.impl(ServiceHubImpl::class)
        di.impl(StartedNodeImpl::class)
        di.impl(StateLoaderImpl::class)
        di.impl(StateMachineManager::class)
        di.impl(TransactionRecorderImpl::class)
        di.factory(this::makeIdentityService)
        di.factory(this::makeInfo)
        di.factory(this::makeKeyManagementService)
        di.factory(this::makeMessagingService)
        di.factory(this::makeTransactionStorage)
        di.factory(this::makeTransactionVerifierService)
        di.factory(this::obtainNodeIdentity)
        di.factory(this::registerWithNetworkMapIfConfigured)
        // Side-effects, i.e. factories returning Unit:
        di.factory(this::registerTokenizableServices)
        di.factory(this::registerNotaries)
        di.factory(this::registerStops)
        di.factory(this::makeVaultObservers)
        di.factory(this::saveOwnNodeInfo)
        di.factory(this::startMessagingService)
        di.factory(this::installCoreFlows)
        di.factory(this::installCordaServices)
        di.factory(this::registerCordappFlows)
        configureNotary(di)
        configureNotaryIdentity(di)
    }

    private fun installCordaServices(services: ServiceHub, flowStarter: FlowStarter, smm: StateMachineManager, myNotaryIdentity: NotaryIdentity?) {
        val loadedServices = cordappLoader.cordapps.flatMap { it.services }
        filterServicesToInstall(loadedServices).forEach {
            try {
                installCordaServiceImpl(it, services, flowStarter, smm, myNotaryIdentity)
            } catch (e: NoSuchMethodException) {
                log.error("${it.name}, as a Corda service, must have a constructor with a single parameter of type " +
                        ServiceHub::class.java.name)
            } catch (e: ServiceInstantiationException) {
                log.error("Corda service ${it.name} failed to instantiate", e.cause)
            } catch (e: Exception) {
                log.error("Unable to install Corda service ${it.name}", e)
            }
        }
    }

    private fun filterServicesToInstall(loadedServices: List<Class<out SerializeAsToken>>): List<Class<out SerializeAsToken>> {
        val customNotaryServiceList = loadedServices.filter { isNotaryService(it) }
        if (customNotaryServiceList.isNotEmpty()) {
            if (configuration.notary?.custom == true) {
                require(customNotaryServiceList.size == 1) {
                    "Attempting to install more than one notary service: ${customNotaryServiceList.joinToString()}"
                }
            }
            else return loadedServices - customNotaryServiceList
        }
        return loadedServices
    }

    /**
     * If the [serviceClass] is a notary service, it will only be enable if the "custom" flag is set in
     * the notary configuration.
     */
    private fun isNotaryService(serviceClass: Class<*>) = NotaryService::class.java.isAssignableFrom(serviceClass)

    /**
     * This customizes the ServiceHub for each CordaService that is initiating flows
     */
    private class AppServiceHubImpl<T : SerializeAsToken>(val serviceHub: ServiceHub, private val flowStarter: FlowStarter) : AppServiceHub, ServiceHub by serviceHub {
        lateinit var serviceInstance: T
        override fun <T> startTrackedFlow(flow: FlowLogic<T>): FlowProgressHandle<T> {
            val stateMachine = startFlowChecked(flow)
            return FlowProgressHandleImpl(
                    id = stateMachine.id,
                    returnValue = stateMachine.resultFuture,
                    progress = stateMachine.logic.track()?.updates ?: Observable.empty()
            )
        }

        override fun <T> startFlow(flow: FlowLogic<T>): FlowHandle<T> {
            val stateMachine = startFlowChecked(flow)
            return FlowHandleImpl(id = stateMachine.id, returnValue = stateMachine.resultFuture)
        }

        private fun <T> startFlowChecked(flow: FlowLogic<T>): FlowStateMachineImpl<T> {
            val logicType = flow.javaClass
            require(logicType.isAnnotationPresent(StartableByService::class.java)) { "${logicType.name} was not designed for starting by a CordaService" }
            val currentUser = FlowInitiator.Service(serviceInstance.javaClass.name)
            return flowStarter.startFlow(flow, currentUser)
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is AppServiceHubImpl<*>) return false

            if (serviceHub != other.serviceHub) return false
            if (serviceInstance != other.serviceInstance) return false

            return true
        }

        override fun hashCode(): Int {
            var result = serviceHub.hashCode()
            result = 31 * result + serviceInstance.hashCode()
            return result
        }
    }

    /**
     * Use this method to install your Corda services in your tests. This is automatically done by the node when it
     * starts up for all classes it finds which are annotated with [CordaService].
     */
    fun <T : SerializeAsToken> installCordaService(serviceClass: Class<T>) = installCordaServiceImpl(serviceClass, started!!.services, started!!.services, started!!.smm, started!!.notaryIdentity)

    private fun <T : SerializeAsToken> installCordaServiceImpl(serviceClass: Class<T>, services: ServiceHub, flowStarter: FlowStarter, smm: StateMachineManager, myNotaryIdentity: NotaryIdentity?): T {
        serviceClass.requireAnnotation<CordaService>()
        val service = try {
            val serviceContext = AppServiceHubImpl<T>(services, flowStarter)
            if (isNotaryService(serviceClass)) {
                check(myNotaryIdentity != null) { "Trying to install a notary service but no notary identity specified" }
                val constructor = serviceClass.getDeclaredConstructor(AppServiceHub::class.java, PublicKey::class.java).apply { isAccessible = true }
                serviceContext.serviceInstance = constructor.newInstance(serviceContext, myNotaryIdentity!!.notaryIdentity.owningKey)
                serviceContext.serviceInstance
            } else {
                try {
                    val extendedServiceConstructor = serviceClass.getDeclaredConstructor(AppServiceHub::class.java).apply { isAccessible = true }
                    serviceContext.serviceInstance = extendedServiceConstructor.newInstance(serviceContext)
                    serviceContext.serviceInstance
                } catch (ex: NoSuchMethodException) {
                    val constructor = serviceClass.getDeclaredConstructor(ServiceHub::class.java).apply { isAccessible = true }
                    log.warn("${serviceClass.name} is using legacy CordaService constructor with ServiceHub parameter. Upgrade to an AppServiceHub parameter to enable updated API features.")
                    constructor.newInstance(services)
                }
            }
        } catch (e: InvocationTargetException) {
            throw ServiceInstantiationException(e.cause)
        }
        cordappServices.cordappServices.putInstance(serviceClass, service)
        smm.tokenizableServices += service

        if (service is NotaryService) handleCustomNotaryService(service)

        log.info("Installed ${serviceClass.name} Corda service")
        return service
    }

    private fun handleCustomNotaryService(service: NotaryService) {
        runOnStop += service::stop
        service.start()
        installCoreFlow(NotaryFlow.Client::class, service::createServiceFlow)
    }

    private fun registerCordappFlows(cordappProvider: CordappProviderInternal) {
        cordappProvider.cordapps.flatMap { it.initiatedFlows }
                .forEach {
                    try {
                        registerInitiatedFlowInternal(it, track = false)
                    } catch (e: NoSuchMethodException) {
                        log.error("${it.name}, as an initiated flow, must have a constructor with a single parameter " +
                                "of type ${Party::class.java.name}")
                    } catch (e: Exception) {
                        log.error("Unable to register initiated flow ${it.name}", e)
                    }
                }
    }

    /**
     * Use this method to register your initiated flows in your tests. This is automatically done by the node when it
     * starts up for all [FlowLogic] classes it finds which are annotated with [InitiatedBy].
     * @return An [Observable] of the initiated flows started by counter-parties.
     */
    fun <T : FlowLogic<*>> registerInitiatedFlow(initiatedFlowClass: Class<T>): Observable<T> {
        return registerInitiatedFlowInternal(initiatedFlowClass, track = true)
    }

    // TODO remove once not needed
    private fun deprecatedFlowConstructorMessage(flowClass: Class<*>): String {
        return "Installing flow factory for $flowClass accepting a ${Party::class.java.simpleName}, which is deprecated. " +
                "It should accept a ${FlowSession::class.java.simpleName} instead"
    }

    private fun <F : FlowLogic<*>> registerInitiatedFlowInternal(initiatedFlow: Class<F>, track: Boolean): Observable<F> {
        val constructors = initiatedFlow.declaredConstructors.associateBy { it.parameterTypes.toList() }
        val flowSessionCtor = constructors[listOf(FlowSession::class.java)]?.apply { isAccessible = true }
        val ctor: (FlowSession) -> F = if (flowSessionCtor == null) {
            // Try to fallback to a Party constructor
            val partyCtor = constructors[listOf(Party::class.java)]?.apply { isAccessible = true }
            if (partyCtor == null) {
                throw IllegalArgumentException("$initiatedFlow must have a constructor accepting a ${FlowSession::class.java.name}")
            } else {
                log.warn(deprecatedFlowConstructorMessage(initiatedFlow))
            }
            { flowSession: FlowSession -> uncheckedCast(partyCtor.newInstance(flowSession.counterparty)) }
        } else {
            { flowSession: FlowSession -> uncheckedCast(flowSessionCtor.newInstance(flowSession)) }
        }
        val initiatingFlow = initiatedFlow.requireAnnotation<InitiatedBy>().value.java
        val (version, classWithAnnotation) = initiatingFlow.flowVersionAndInitiatingClass
        require(classWithAnnotation == initiatingFlow) {
            "${InitiatedBy::class.java.name} must point to ${classWithAnnotation.name} and not ${initiatingFlow.name}"
        }
        val flowFactory = InitiatedFlowFactory.CorDapp(version, initiatedFlow.appName, ctor)
        val observable = internalRegisterFlowFactory(initiatingFlow, flowFactory, initiatedFlow, track)
        log.info("Registered ${initiatingFlow.name} to initiate ${initiatedFlow.name} (version $version)")
        return observable
    }

    @VisibleForTesting
    fun <F : FlowLogic<*>> internalRegisterFlowFactory(initiatingFlowClass: Class<out FlowLogic<*>>,
                                                       flowFactory: InitiatedFlowFactory<F>,
                                                       initiatedFlowClass: Class<F>,
                                                       track: Boolean): Observable<F> {
        val observable = if (track) {
            started!!.smm.changes.filter { it is StateMachineManager.Change.Add }.map { it.logic }.ofType(initiatedFlowClass)
        } else {
            Observable.empty()
        }
        flowFactories[initiatingFlowClass] = flowFactory
        return observable
    }

    /**
     * Installs a flow that's core to the Corda platform. Unlike CorDapp flows which are versioned individually using
     * [InitiatingFlow.version], core flows have the same version as the node's platform version. To cater for backwards
     * compatibility [flowFactory] provides a second parameter which is the platform version of the initiating party.
     * @suppress
     */
    @VisibleForTesting
    fun installCoreFlow(clientFlowClass: KClass<out FlowLogic<*>>, flowFactory: (FlowSession) -> FlowLogic<*>) {
        require(clientFlowClass.java.flowVersionAndInitiatingClass.first == 1) {
            "${InitiatingFlow::class.java.name}.version not applicable for core flows; their version is the node's platform version"
        }
        flowFactories[clientFlowClass.java] = InitiatedFlowFactory.Core(flowFactory)
        log.debug { "Installed core flow ${clientFlowClass.java.name}" }
    }


    private fun installCoreFlows() {
        installCoreFlow(FinalityFlow::class, ::FinalityHandler)
        installCoreFlow(NotaryChangeFlow::class, ::NotaryChangeHandler)
        installCoreFlow(ContractUpgradeFlow.Initiate::class, ::ContractUpgradeHandler)
        installCoreFlow(SwapIdentitiesFlow::class, ::SwapIdentitiesHandler)
    }

    private fun registerTokenizableServices(smm: StateMachineManager,
                                            attachments: NodeAttachmentService,
                                            network: MessagingService,
                                            vaultService: VaultService,
                                            keyManagementService: KeyManagementService,
                                            identityService: IdentityService,
                                            clock: Clock,
                                            schedulerService: SchedulerService,
                                            auditService: AuditService,
                                            monitoringService: MonitoringService,
                                            networkMapCache: NetworkMapCache,
                                            schemaService: SchemaService,
                                            transactionVerifierService: TransactionVerifierService,
                                            validatedTransactions: TransactionStorage,
                                            contractUpgradeService: ContractUpgradeService,
                                            services: ServiceHub,
                                            cordappProvider: CordappProvider,
                                            node: AbstractNode,
                                            notaryService: NotaryService? = null) {
        smm.tokenizableServices.addAll(listOf(
                attachments,
                network,
                vaultService,
                keyManagementService,
                identityService,
                clock,
                schedulerService,
                auditService,
                monitoringService,
                networkMapCache,
                schemaService,
                transactionVerifierService,
                validatedTransactions,
                contractUpgradeService,
                services,
                cordappProvider,
                node,
                notaryService).filterNotNull())
    }

    protected open fun makeTransactionStorage(database: CordaPersistence): WritableTransactionStorage = DBTransactionStorage()

    private fun makeVaultObservers(smm: StateMachineManager, vaultService: VaultService, schedulerService: SchedulerService, database: CordaPersistence, flowLogicRefFactory: FlowLogicRefFactory) {
        VaultSoftLockManager(vaultService, smm)
        ScheduledActivityObserver(vaultService, schedulerService, flowLogicRefFactory)
        HibernateObserver(vaultService.rawUpdates, database.hibernateConfig)
    }

    private fun makeInfo(network: MessagingService, allIdentities: AllIdentities): NodeInfo {
        val addresses = myAddresses(network) // TODO There is no support for multiple IP addresses yet.
        return NodeInfo(addresses, allIdentities.identities, versionInfo.platformVersion, platformClock.instant().toEpochMilli())
    }

    /**
     * Obtain the node's notary identity if it's configured to be one. If part of a distributed notary then this will be
     * the distributed identity shared across all the nodes of the cluster.
     */
    private fun configureNotaryIdentity(di: MutableLazyHub) {
        configuration.notary?.let {
            di.obj(NotaryIdentity(obtainIdentity(it)))
        }
    }

    @VisibleForTesting
    protected open fun acceptableLiveFiberCountOnStop(): Int = 0

    private fun validateKeystore() {
        val containCorrectKeys = try {
            // This will throw IOException if key file not found or KeyStoreException if keystore password is incorrect.
            val sslKeystore = loadKeyStore(configuration.sslKeystore, configuration.keyStorePassword)
            val identitiesKeystore = loadKeyStore(configuration.nodeKeystore, configuration.keyStorePassword)
            sslKeystore.containsAlias(X509Utilities.CORDA_CLIENT_TLS) && identitiesKeystore.containsAlias(X509Utilities.CORDA_CLIENT_CA)
        } catch (e: KeyStoreException) {
            log.warn("Certificate key store found but key store password does not match configuration.")
            false
        } catch (e: IOException) {
            false
        }
        require(containCorrectKeys) {
            "Identity certificate not found. " +
                    "Please either copy your existing identity key and certificate from another node, " +
                    "or if you don't have one yet, fill out the config file and run corda.jar --initial-registration. " +
                    "Read more at: https://docs.corda.net/permissioning.html"
        }
    }

    // Specific class so that MockNode can catch it.
    class DatabaseConfigurationException(msg: String) : CordaException(msg)

    protected open fun <T> initialiseDatabasePersistence(lh: LazyHub, insideTransaction: (CordaPersistence) -> T): T {
        val props = configuration.dataSourceProperties
        if (props.isNotEmpty()) {
            val database = configureDatabase(props, configuration.database, lh.getProvider(IdentityService::class), lh[SchemaService::class])
            // Now log the vendor string as this will also cause a connection to be tested eagerly.
            database.transaction {
                log.info("Connected to ${database.dataSource.connection.metaData.databaseProductName} database.")
            }
            runOnStop += database::close
            return database.transaction {
                insideTransaction(database)
            }
        } else {
            throw DatabaseConfigurationException("There must be a database configured.")
        }
    }

    class NetMapRegistration(val future: CordaFuture<Unit>)

    private fun registerWithNetworkMapIfConfigured(info: NodeInfo, network: MessagingService, networkMapCache: NetworkMapCacheInternal, keyManagementService: KeyManagementService, inNodeNetworkMapService: NetworkMapService): NetMapRegistration {
        networkMapCache.addNode(info)
        // In the unit test environment, we may sometimes run without any network map service
        return NetMapRegistration(if (networkMapAddress == null && inNodeNetworkMapService == NullNetworkMapService) {
            networkMapCache.runWithoutMapService()
            noNetworkMapConfigured(networkMapCache)  // TODO This method isn't needed as runWithoutMapService sets the Future in the cache
        } else {
            val netMapRegistration = registerWithNetworkMap(info, network, networkMapCache, keyManagementService)
            // We may want to start node immediately with database data and not wait for network map registration (but send it either way).
            // So we are ready to go.
            if (networkMapCache.loadDBSuccess) {
                log.info("Node successfully loaded network map data from the database.")
                doneFuture(Unit)
            } else {
                netMapRegistration
            }
        })
    }

    /**
     * Register this node with the network map cache, and load network map from a remote service (and register for
     * updates) if one has been supplied.
     */
    protected open fun registerWithNetworkMap(info: NodeInfo, network: MessagingService, networkMapCache: NetworkMapCacheInternal, keyManagementService: KeyManagementService): CordaFuture<Unit> {
        val address: SingleMessageRecipient = networkMapAddress ?:
                network.getAddressOfParty(PartyInfo.SingleNode(info.legalIdentitiesAndCerts.first().party, info.addresses)) as SingleMessageRecipient
        // Register for updates, even if we're the one running the network map.
        return sendNetworkMapRegistration(address, info, network, keyManagementService).flatMap { (error) ->
            check(error == null) { "Unable to register with the network map service: $error" }
            // The future returned addMapService will complete on the same executor as sendNetworkMapRegistration, namely the one used by net
            networkMapCache.addMapService(network, address, true, null)
        }
    }

    private fun sendNetworkMapRegistration(networkMapAddress: SingleMessageRecipient, info: NodeInfo, network: MessagingService, keyManagementService: KeyManagementService): CordaFuture<RegistrationResponse> {
        // Register this node against the network
        val instant = platformClock.instant()
        val expires = instant + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
        val reg = NodeRegistration(info, info.serial, ADD, expires)
        val request = RegistrationRequest(reg.toWire(keyManagementService, info.legalIdentitiesAndCerts.first().owningKey), network.myAddress)
        return network.sendRequest(NetworkMapService.REGISTER_TOPIC, request, networkMapAddress)
    }

    /** Return list of node's addresses. It's overridden in MockNetwork as we don't have real addresses for MockNodes. */
    protected abstract fun myAddresses(network: MessagingService): List<NetworkHostAndPort>

    /** This is overriden by the mock node implementation to enable operation without any network map service */
    protected open fun noNetworkMapConfigured(networkMapCache: NetworkMapCacheInternal): CordaFuture<Unit> {
        if (networkMapCache.loadDBSuccess) {
            return doneFuture(Unit)
        } else {
            // TODO: There should be a consistent approach to configuration error exceptions.
            throw IllegalStateException("Configuration error: this node isn't being asked to act as the network map, nor " +
                    "has any other map node been configured.")
        }
    }

    protected open fun makeKeyManagementService(identityService: IdentityService): KeyManagementService {
        return PersistentKeyManagementService(identityService, partyKeys)
    }

    private fun configureNotary(lh: MutableLazyHub) {
        val notaryConfig = configuration.notary ?: return
        if (notaryConfig.validating) {
            if (notaryConfig.raft != null) {
                lh.impl(RaftValidatingNotaryService::class)
            } else if (notaryConfig.bftSMaRt != null) {
                throw IllegalArgumentException("Validating BFTSMaRt notary not supported")
            } else {
                lh.impl(ValidatingNotaryService::class)
            }
        } else {
            if (notaryConfig.raft != null) {
                lh.impl(RaftNonValidatingNotaryService::class)
            } else if (notaryConfig.bftSMaRt != null) {
                lh.impl(BFTNonValidatingNotaryService::class)
            } else {
                lh.impl(SimpleNotaryService::class)
            }
        }
    }

    private fun makeIdentityService(allIdentities: AllIdentities, legalIdentity: PartyAndCertificate): IdentityService {
        val trustStore = KeyStoreWrapper(configuration.trustStoreFile, configuration.trustStorePassword)
        val caKeyStore = KeyStoreWrapper(configuration.nodeKeystore, configuration.keyStorePassword)
        val caCertificates = arrayOf(legalIdentity.certificate, caKeyStore.certificateAndKeyPair(X509Utilities.CORDA_CLIENT_CA).certificate.cert)
        return PersistentIdentityService(allIdentities.identities, trustRoot = trustStore.getX509Certificate(X509Utilities.CORDA_ROOT_CA), caCertificates = *caCertificates)
    }

    protected abstract fun makeTransactionVerifierService(network: MessagingService): TransactionVerifierService

    open fun stop() {
        // TODO: We need a good way of handling "nice to have" shutdown events, especially those that deal with the
        // network, including unsubscribing from updates from remote services. Possibly some sort of parameter to stop()
        // to indicate "Please shut down gracefully" vs "Shut down now".
        // Meanwhile, we let the remote service send us updates until the acknowledgment buffer overflows and it
        // unsubscribes us forcibly, rather than blocking the shutdown process.

        // Run shutdown hooks in opposite order to starting
        for (toRun in runOnStop.reversed()) {
            toRun()
        }
        runOnStop.clear()
    }

    protected abstract fun makeMessagingService(database: CordaPersistence, networkMapCache: NetworkMapCacheInternal, monitoringService: MonitoringService, legalIdentity: PartyAndCertificate, notaryIdentity: NotaryIdentity?): MessagingService

    protected abstract fun startMessagingService(rpcOps: RPCOps, network: MessagingService)

    private fun obtainNodeIdentity() = obtainIdentityImpl(null)
    private fun obtainIdentity(notaryConfig: NotaryConfig) = obtainIdentityImpl(notaryConfig)
    private fun obtainIdentityImpl(notaryConfig: NotaryConfig?): PartyAndCertificate {
        // Load the private identity key, creating it if necessary. The identity key is a long term well known key that
        // is distributed to other peers and we use it (or a key signed by it) when we need to do something
        // "permissioned". The identity file is what gets distributed and contains the node's legal name along with
        // the public key. Obviously in a real system this would need to be a certificate chain of some kind to ensure
        // the legal name is actually validated in some way.
        val keyStore = KeyStoreWrapper(configuration.nodeKeystore, configuration.keyStorePassword)

        val (id, singleName) = if (notaryConfig == null) {
            // Node's main identity
            Pair("identity", myLegalName)
        } else {
            val notaryId = notaryConfig.run {
                NotaryService.constructId(validating, raft != null, bftSMaRt != null, custom)
            }
            if (notaryConfig.bftSMaRt == null && notaryConfig.raft == null) {
                // Node's notary identity
                Pair(notaryId, myLegalName.copy(commonName = notaryId))
            } else {
                // The node is part of a distributed notary whose identity must already be generated beforehand
                Pair(notaryId, null)
            }
        }

        // TODO: Integrate with Key management service?
        val privateKeyAlias = "$id-private-key"

        if (!keyStore.containsAlias(privateKeyAlias)) {
            singleName ?: throw IllegalArgumentException(
                    "Unable to find in the key store the identity of the distributed notary ($id) the node is part of")
            // TODO: Remove use of [ServiceIdentityGenerator.generateToDisk].
            log.info("$privateKeyAlias not found in key store ${configuration.nodeKeystore}, generating fresh key!")
            keyStore.signAndSaveNewKeyPair(singleName, privateKeyAlias, generateKeyPair())
        }

        val (x509Cert, keys) = keyStore.certificateAndKeyPair(privateKeyAlias)

        // TODO: Use configuration to indicate composite key should be used instead of public key for the identity.
        val compositeKeyAlias = "$id-composite-key"
        val certificates = if (keyStore.containsAlias(compositeKeyAlias)) {
            // Use composite key instead if it exists
            val certificate = keyStore.getCertificate(compositeKeyAlias)
            // We have to create the certificate chain for the composite key manually, this is because we don't have a keystore
            // provider that understand compositeKey-privateKey combo. The cert chain is created using the composite key certificate +
            // the tail of the private key certificates, as they are both signed by the same certificate chain.
            listOf(certificate) + keyStore.getCertificateChain(privateKeyAlias).drop(1)
        } else {
            keyStore.getCertificateChain(privateKeyAlias).let {
                check(it[0].toX509CertHolder() == x509Cert) { "Certificates from key store do not line up!" }
                it.asList()
            }
        }

        val nodeCert = certificates[0] as? X509Certificate ?: throw ConfigurationException("Node certificate must be an X.509 certificate")
        val subject = CordaX500Name.build(nodeCert.subjectX500Principal)
        // TODO Include the name of the distributed notary, which the node is part of, in the notary config so that we
        // can cross-check the identity we get from the key store
        if (singleName != null && subject != singleName) {
            throw ConfigurationException("The name '$singleName' for $id doesn't match what's in the key store: $subject")
        }

        partyKeys += keys
        return PartyAndCertificate(CertificateFactory.getInstance("X509").generateCertPath(certificates))
    }

    protected open fun generateKeyPair() = cryptoGenerateKeyPair()
    class ServiceHubImpl(private val database: CordaPersistence,
                         override val validatedTransactions: TransactionStorage,
                         override val transactionVerifierService: TransactionVerifierService,
                         override val networkMapCache: NetworkMapCache,
                         override val vaultService: VaultService,
                         override val contractUpgradeService: ContractUpgradeService,
                         override val keyManagementService: KeyManagementService,
                         override val identityService: IdentityService,
                         override val attachments: AttachmentStorage,
                         override val clock: Clock,
                         override val myInfo: NodeInfo,
                         override val cordappProvider: CordappProvider,
                         stateLoader: StateLoaderInternal,
                         transactionRecorder: TransactionRecorderInternal,
                         cordappServices: CordappServicesInternal) : ServiceHubInternal, StateLoader by stateLoader, TransactionRecorder by transactionRecorder, CordappServices by cordappServices, SingletonSerializeAsToken() {
        override fun jdbcSession() = database.createSession()
    }

    override fun getFlowFactory(initiatingFlowClass: Class<out FlowLogic<*>>): InitiatedFlowFactory<*>? {
        return flowFactories[initiatingFlowClass]
    }
}

class FlowStarterImpl(private val serverThread: AffinityExecutor, private val smm: StateMachineManager, override val flowLogicRefFactory: FlowLogicRefFactory) : FlowStarterInternal {
    override fun <T> startFlow(logic: FlowLogic<T>, flowInitiator: FlowInitiator, ourIdentity: Party?): FlowStateMachineImpl<T> {
        return serverThread.fetchFrom { smm.add(logic, flowInitiator, ourIdentity) }
    }
}

interface ServiceHubInternal : ServiceHub
