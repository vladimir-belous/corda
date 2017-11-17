package net.corda.node.services.events

import co.paralleluniverse.fibers.Suspendable
import com.codahale.metrics.MetricRegistry
import com.nhaarman.mockito_kotlin.*
import net.corda.core.contracts.*
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowLogicRef
import net.corda.core.flows.FlowLogicRefFactory
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.NodeInfo
import net.corda.core.node.ServiceHub
import net.corda.core.node.ServicesForResolution
import net.corda.core.node.StatesToRecord
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.days
import net.corda.node.internal.FlowStarterImpl
import net.corda.node.internal.cordapp.CordappLoader
import net.corda.node.internal.cordapp.CordappProviderImpl
import net.corda.node.services.api.MonitoringService
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.identity.InMemoryIdentityService
import net.corda.node.services.network.NetworkMapCacheImpl
import net.corda.node.services.persistence.DBCheckpointStorage
import net.corda.node.services.statemachine.FlowLogicRefFactoryImpl
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.node.services.statemachine.StateMachineManagerImpl
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.utilities.AffinityExecutor
import net.corda.node.utilities.CordaPersistence
import net.corda.node.utilities.configureDatabase
import net.corda.testing.*
import net.corda.testing.contracts.DummyContract
import net.corda.testing.node.*
import net.corda.testing.node.MockServices.Companion.makeTestDataSourceProperties
import net.corda.testing.node.MockServices.Companion.makeTestDatabaseProperties
import net.corda.testing.node.MockServices.Companion.makeTestIdentityService
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.nio.file.Paths
import java.security.PublicKey
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

class NodeSchedulerServiceTest : SingletonSerializeAsToken() {
    companion object {
        private val myInfo = NodeInfo(listOf(MOCK_HOST_AND_PORT), listOf(DUMMY_IDENTITY_1), 1, serial = 1L)
    }

    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule(true)
    private val realClock: Clock = Clock.systemUTC()
    private val stoppedClock: Clock = Clock.fixed(realClock.instant(), realClock.zone)
    private val testClock = TestClock(stoppedClock)

    private val schedulerGatedExecutor = AffinityExecutor.Gate(true)

    abstract class Services : ServiceHubInternal, TestReference

    private lateinit var services: Services
    private lateinit var scheduler: NodeSchedulerService
    private lateinit var smmExecutor: AffinityExecutor.ServiceAffinityExecutor
    private lateinit var database: CordaPersistence
    private lateinit var countDown: CountDownLatch
    private lateinit var smmHasRemovedAllFlows: CountDownLatch
    private lateinit var kms: MockKeyManagementService
    private lateinit var mockSMM: StateMachineManager
    private val ourIdentity = ALICE_NAME
    var calls: Int = 0

    /**
     * Have a reference to this test added to [ServiceHub] so that when the [FlowLogic] runs it can access the test instance.
     * The [TestState] is serialized and deserialized so attempting to use a transient field won't work, as it just
     * results in NPE.
     */
    interface TestReference {
        val testReference: NodeSchedulerServiceTest
    }

    @Before
    fun setup() {
        countDown = CountDownLatch(1)
        smmHasRemovedAllFlows = CountDownLatch(1)
        calls = 0
        val dataSourceProps = makeTestDataSourceProperties()
        val databaseProperties = makeTestDatabaseProperties()
        database = configureDatabase(dataSourceProps, databaseProperties, ::makeTestIdentityService)
        val identityService = InMemoryIdentityService(trustRoot = DEV_TRUST_ROOT)
        kms = MockKeyManagementService(identityService, ALICE_KEY)
        val configuration = testNodeConfiguration(Paths.get("."), CordaX500Name("Alice", "London", "GB"))
        val validatedTransactions = MockTransactionStorage()
        val servicesForResolution = rigorousMock<ServicesForResolution>().also {
            doReturn(validatedTransactions).whenever(it).validatedTransactions
            doReturn(identityService).whenever(it).identityService
            doReturn(validatedTransactions).whenever(it).validatedTransactions
        }
        database.transaction {
            services = rigorousMock<Services>().also {
                doReturn(configuration).whenever(it).configuration
                doReturn(MonitoringService(MetricRegistry())).whenever(it).monitoringService
                doReturn(validatedTransactions).whenever(it).validatedTransactions
                doReturn(NetworkMapCacheImpl(MockNetworkMapCache(database), identityService)).whenever(it).networkMapCache
                doCallRealMethod().whenever(it).signInitialTransaction(any(), any<PublicKey>())
                doReturn(myInfo).whenever(it).myInfo
                doReturn(kms).whenever(it).keyManagementService
                doReturn(CordappProviderImpl(CordappLoader.createWithTestPackages(listOf("net.corda.testing.contracts")), MockAttachmentStorage())).whenever(it).cordappProvider
                doCallRealMethod().whenever(it).recordTransactions(any<StatesToRecord>(), any())
                doCallRealMethod().whenever(it).recordTransactions(any<Iterable<SignedTransaction>>())
                doCallRealMethod().whenever(it).recordTransactions(any<SignedTransaction>(), anyVararg())
                doCallRealMethod().whenever(it).loadState(any())
                doReturn(NodeVaultService(testClock, kms, servicesForResolution, database.hibernateConfig)).whenever(it).vaultService
                doReturn(this@NodeSchedulerServiceTest).whenever(it).testReference
            }
            smmExecutor = AffinityExecutor.ServiceAffinityExecutor("test", 1)
            mockSMM = StateMachineManagerImpl(services, DBCheckpointStorage(), smmExecutor, database)
            scheduler = NodeSchedulerService(testClock, database, FlowStarterImpl(smmExecutor, mockSMM), services, schedulerGatedExecutor, serverThread = smmExecutor)
            mockSMM.changes.subscribe { change ->
                if (change is StateMachineManager.Change.Removed && mockSMM.allStateMachines.isEmpty()) {
                    smmHasRemovedAllFlows.countDown()
                }
            }
            mockSMM.start(emptyList())
            scheduler.start()
        }
    }

    @After
    fun tearDown() {
        // We need to make sure the StateMachineManager is done before shutting down executors.
        if (mockSMM.allStateMachines.isNotEmpty()) {
            smmHasRemovedAllFlows.await()
        }
        smmExecutor.shutdown()
        smmExecutor.awaitTermination(60, TimeUnit.SECONDS)
        database.close()
    }

    // Ignore IntelliJ when it says these properties can be private, if they are we cannot serialise them
    // in AMQP.
    @Suppress("MemberVisibilityCanPrivate")
    class TestState(val flowLogicRef: FlowLogicRef, val instant: Instant, val myIdentity: Party) : LinearState, SchedulableState {
        override val participants: List<AbstractParty>
            get() = listOf(myIdentity)

        override val linearId = UniqueIdentifier()

        override fun nextScheduledActivity(thisStateRef: StateRef, flowLogicRefFactory: FlowLogicRefFactory): ScheduledActivity? {
            return ScheduledActivity(flowLogicRef, instant)
        }
    }

    class TestFlowLogic(private val increment: Int = 1) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            (serviceHub as TestReference).testReference.calls += increment
            (serviceHub as TestReference).testReference.countDown.countDown()
        }
    }

    class Command : TypeOnlyCommandData()

    @Test
    fun `test activity due now`() {
        val time = stoppedClock.instant()
        scheduleTX(time)

        assertThat(calls).isEqualTo(0)
        schedulerGatedExecutor.waitAndRun()
        countDown.await()
        assertThat(calls).isEqualTo(1)
    }

    @Test
    fun `test activity due in the past`() {
        val time = stoppedClock.instant() - 1.days
        scheduleTX(time)

        assertThat(calls).isEqualTo(0)
        schedulerGatedExecutor.waitAndRun()
        countDown.await()
        assertThat(calls).isEqualTo(1)
    }

    @Test
    fun `test activity due in the future`() {
        val time = stoppedClock.instant() + 1.days
        scheduleTX(time)

        val backgroundExecutor = Executors.newSingleThreadExecutor()
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        assertThat(calls).isEqualTo(0)
        testClock.advanceBy(1.days)
        backgroundExecutor.shutdown()
        assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS))
        countDown.await()
        assertThat(calls).isEqualTo(1)
    }

    @Test
    fun `test activity due in the future and schedule another earlier`() {
        val time = stoppedClock.instant() + 1.days
        scheduleTX(time + 1.days)

        val backgroundExecutor = Executors.newSingleThreadExecutor()
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        assertThat(calls).isEqualTo(0)
        scheduleTX(time, 3)

        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        testClock.advanceBy(1.days)
        countDown.await()
        assertThat(calls).isEqualTo(3)
        backgroundExecutor.shutdown()
        assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS))
    }

    @Test
    fun `test activity due in the future and schedule another later`() {
        val time = stoppedClock.instant() + 1.days
        scheduleTX(time)

        val backgroundExecutor = Executors.newSingleThreadExecutor()
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        assertThat(calls).isEqualTo(0)
        scheduleTX(time + 1.days, 3)

        testClock.advanceBy(1.days)
        countDown.await()
        assertThat(calls).isEqualTo(1)
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        testClock.advanceBy(1.days)
        backgroundExecutor.shutdown()
        assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS))
    }

    @Test
    fun `test activity due in the future and schedule another for same time`() {
        val time = stoppedClock.instant() + 1.days
        scheduleTX(time)

        val backgroundExecutor = Executors.newSingleThreadExecutor()
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        assertThat(calls).isEqualTo(0)
        scheduleTX(time, 3)

        testClock.advanceBy(1.days)
        countDown.await()
        assertThat(calls).isEqualTo(1)
        backgroundExecutor.shutdown()
        assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS))
    }

    @Test
    fun `test activity due in the future and schedule another for same time then unschedule original`() {
        val time = stoppedClock.instant() + 1.days
        val scheduledRef1 = scheduleTX(time)

        val backgroundExecutor = Executors.newSingleThreadExecutor()
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        assertThat(calls).isEqualTo(0)
        scheduleTX(time, 3)

        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        database.transaction {
            scheduler.unscheduleStateActivity(scheduledRef1!!.ref)
        }
        testClock.advanceBy(1.days)
        countDown.await()
        assertThat(calls).isEqualTo(3)
        backgroundExecutor.shutdown()
        assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS))
    }

    @Test
    fun `test activity due in the future then unschedule`() {
        val scheduledRef1 = scheduleTX(stoppedClock.instant() + 1.days)

        val backgroundExecutor = Executors.newSingleThreadExecutor()
        backgroundExecutor.execute { schedulerGatedExecutor.waitAndRun() }
        assertThat(calls).isEqualTo(0)

        database.transaction {
            scheduler.unscheduleStateActivity(scheduledRef1!!.ref)
        }
        testClock.advanceBy(1.days)
        assertThat(calls).isEqualTo(0)
        backgroundExecutor.shutdown()
        assertTrue(backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS))
    }

    private fun scheduleTX(instant: Instant, increment: Int = 1): ScheduledStateRef? {
        var scheduledRef: ScheduledStateRef? = null
        database.transaction {
            apply {
                val freshKey = kms.freshKey()
                val state = TestState(FlowLogicRefFactoryImpl.createForRPC(TestFlowLogic::class.java, increment), instant, myInfo.chooseIdentity())
                val builder = TransactionBuilder(null).apply {
                    addOutputState(state, DummyContract.PROGRAM_ID, DUMMY_NOTARY)
                    addCommand(Command(), freshKey)
                }
                val usefulTX = services.signInitialTransaction(builder, freshKey)
                val txHash = usefulTX.id

                services.recordTransactions(usefulTX)
                scheduledRef = ScheduledStateRef(StateRef(txHash, 0), state.instant)
                scheduler.scheduleStateActivity(scheduledRef!!)
            }
        }
        return scheduledRef
    }
}
