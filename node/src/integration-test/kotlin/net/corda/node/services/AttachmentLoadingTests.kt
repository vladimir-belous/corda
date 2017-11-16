package net.corda.node.services

import net.corda.core.contracts.Contract
import net.corda.core.contracts.PartyAndReference
import net.corda.core.cordapp.CordappProvider
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.UnexpectedFlowEndException
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.internal.toLedgerTransaction
import net.corda.core.serialization.SerializationFactory
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.getOrThrow
import net.corda.core.utilities.loggerFor
import net.corda.node.internal.cordapp.CordappLoader
import net.corda.node.internal.cordapp.CordappProviderImpl
import net.corda.testing.*
import net.corda.testing.DUMMY_BANK_A
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.driver.DriverDSLExposedInterface
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.driver
import net.corda.testing.node.MockCordappConfigProvider
import net.corda.testing.node.MockServices
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import java.net.URLClassLoader
import java.nio.file.Files
import kotlin.test.assertFailsWith

class AttachmentLoadingTests {
    private class Services : MockServices() {
        private val provider = CordappProviderImpl(CordappLoader.createDevMode(listOf(isolatedJAR)), MockCordappConfigProvider(), attachments)
        private val cordapp get() = provider.cordapps.first()
        val attachmentId get() = provider.getCordappAttachmentId(cordapp)!!
        val appContext get() = provider.getAppContext(cordapp)
        override val cordappProvider: CordappProvider = provider
    }

    private companion object {
        val logger = loggerFor<AttachmentLoadingTests>()
        val isolatedJAR = AttachmentLoadingTests::class.java.getResource("isolated.jar")!!
        val ISOLATED_CONTRACT_ID = "net.corda.finance.contracts.isolated.AnotherDummyContract"

        val bankAName = CordaX500Name("BankA", "Zurich", "CH")
        val bankBName = CordaX500Name("BankB", "Zurich", "CH")
        val flowInitiatorClass: Class<out FlowLogic<*>> =
                Class.forName("net.corda.finance.contracts.isolated.IsolatedDummyFlow\$Initiator", true, URLClassLoader(arrayOf(isolatedJAR)))
                        .asSubclass(FlowLogic::class.java)

        private fun DriverDSLExposedInterface.createTwoNodes(): List<NodeHandle> {
            return listOf(
                    startNode(providedName = bankAName),
                    startNode(providedName = bankBName)
            ).transpose().getOrThrow()
        }

        private fun DriverDSLExposedInterface.installIsolatedCordappTo(nodeName: CordaX500Name) {
            // Copy the app jar to the first node. The second won't have it.
            val path = (baseDirectory(nodeName.toString()) / "cordapps").createDirectories() / "isolated.jar"
            logger.info("Installing isolated jar to $path")
            isolatedJAR.openStream().buffered().use { input ->
                Files.newOutputStream(path).buffered().use { output ->
                    input.copyTo(output)
                }
            }
        }
    }

    private lateinit var services: Services

    @Before
    fun setup() {
        services = Services()
    }

    @Test
    fun `test a wire transaction has loaded the correct attachment`() = withTestSerialization {
        val appClassLoader = services.appContext.classLoader
        val contractClass = appClassLoader.loadClass(ISOLATED_CONTRACT_ID).asSubclass(Contract::class.java)
        val generateInitialMethod = contractClass.getDeclaredMethod("generateInitial", PartyAndReference::class.java, Integer.TYPE, Party::class.java)
        val contract = contractClass.newInstance()
        val txBuilder = generateInitialMethod.invoke(contract, DUMMY_BANK_A.ref(1), 1, DUMMY_NOTARY) as TransactionBuilder
        val context = SerializationFactory.defaultFactory.defaultContext.withClassLoader(appClassLoader)
        val ledgerTx = txBuilder.toLedgerTransaction(services, context)
        contract.verify(ledgerTx)

        val actual = ledgerTx.attachments.first()
        val expected = services.attachments.openAttachment(services.attachmentId)!!

        assertEquals(expected, actual)
    }

    @Test
    fun `test that attachments retrieved over the network are not used for code`() {
        driver {
            installIsolatedCordappTo(bankAName)
            val (bankA, bankB) = createTwoNodes()
            assertFailsWith<UnexpectedFlowEndException>("Party C=CH,L=Zurich,O=BankB rejected session request: Don't know net.corda.finance.contracts.isolated.IsolatedDummyFlow\$Initiator") {
                bankA.rpc.startFlowDynamic(flowInitiatorClass, bankB.nodeInfo.legalIdentities.first()).returnValue.getOrThrow()
            }
        }
    }

    @Test
    fun `tests that if the attachment is loaded on both sides already that a flow can run`() {
        driver {
            installIsolatedCordappTo(bankAName)
            installIsolatedCordappTo(bankBName)
            val (bankA, bankB) = createTwoNodes()
            bankA.rpc.startFlowDynamic(flowInitiatorClass, bankB.nodeInfo.legalIdentities.first()).returnValue.getOrThrow()
        }
    }
}
