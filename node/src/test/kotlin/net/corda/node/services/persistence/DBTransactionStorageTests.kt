package net.corda.node.services.persistence

import net.corda.core.contracts.StateRef
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignatureMetadata
import net.corda.core.crypto.TransactionSignature
import net.corda.core.node.StatesToRecord
import net.corda.core.node.services.VaultService
import net.corda.core.toFuture
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.node.services.api.VaultServiceInternal
import net.corda.node.services.schema.HibernateObserver
import net.corda.node.services.transactions.PersistentUniquenessProvider
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.utilities.CordaPersistence
import net.corda.node.utilities.configureDatabase
import net.corda.testing.*
import net.corda.testing.node.MockServices
import net.corda.testing.node.MockServices.Companion.makeTestDataSourceProperties
import net.corda.testing.node.MockServices.Companion.makeTestDatabaseProperties
import net.corda.testing.node.MockServices.Companion.makeTestIdentityService
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class DBTransactionStorageTests {
    @Rule
    @JvmField
    val testSerialization = SerializationEnvironmentRule()
    lateinit var database: CordaPersistence
    lateinit var transactionStorage: DBTransactionStorage
    lateinit var services: MockServices
    val vault: VaultService get() = services.vaultService

    @Before
    fun setUp() {
        LogHelper.setLevel(PersistentUniquenessProvider::class)
        val dataSourceProps = makeTestDataSourceProperties()
        database = configureDatabase(dataSourceProps, makeTestDatabaseProperties(), ::makeTestIdentityService)
        database.transaction {
            services = object : MockServices(BOB_KEY) {
                override val vaultService: VaultServiceInternal
                    get() {
                        val vaultService = NodeVaultService(clock, keyManagementService, this, database.hibernateConfig)
                        hibernatePersister = HibernateObserver.install(vaultService.rawUpdates, database.hibernateConfig)
                        return vaultService
                    }

                override fun recordTransactions(txs: Iterable<SignedTransaction>) {
                    for (stx in txs) {
                        validatedTransactions.addTransaction(stx)
                    }
                    // Refactored to use notifyAll() as we have no other unit test for that method with multiple transactions.
                    vaultService.notifyAll(StatesToRecord.ONLY_RELEVANT, txs.map { it.tx })
                }
            }
        }
        newTransactionStorage()
    }

    @After
    fun cleanUp() {
        database.close()
        LogHelper.reset(PersistentUniquenessProvider::class)
    }

    @Test
    fun `empty store`() {
        database.transaction {
            assertThat(transactionStorage.getTransaction(newTransaction().id)).isNull()
        }
        database.transaction {
            assertThat(transactionStorage.transactions).isEmpty()
        }
        newTransactionStorage()
        database.transaction {
            assertThat(transactionStorage.transactions).isEmpty()
        }
    }

    @Test
    fun `one transaction`() {
        val transaction = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(transaction)
        }
        assertTransactionIsRetrievable(transaction)
        database.transaction {
            assertThat(transactionStorage.transactions).containsExactly(transaction)
        }
        newTransactionStorage()
        assertTransactionIsRetrievable(transaction)
        database.transaction {
            assertThat(transactionStorage.transactions).containsExactly(transaction)
        }
    }

    @Test
    fun `two transactions across restart`() {
        val firstTransaction = newTransaction()
        val secondTransaction = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(firstTransaction)
        }
        newTransactionStorage()
        database.transaction {
            transactionStorage.addTransaction(secondTransaction)
        }
        assertTransactionIsRetrievable(firstTransaction)
        assertTransactionIsRetrievable(secondTransaction)
        database.transaction {
            assertThat(transactionStorage.transactions).containsOnly(firstTransaction, secondTransaction)
        }
    }

    @Test
    fun `two transactions with rollback`() {
        val firstTransaction = newTransaction()
        val secondTransaction = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(firstTransaction)
            transactionStorage.addTransaction(secondTransaction)
            rollback()
        }

        database.transaction {
            assertThat(transactionStorage.transactions).isEmpty()
        }
    }

    @Test
    fun `two transactions in same DB transaction scope`() {
        val firstTransaction = newTransaction()
        val secondTransaction = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(firstTransaction)
            transactionStorage.addTransaction(secondTransaction)
        }
        assertTransactionIsRetrievable(firstTransaction)
        assertTransactionIsRetrievable(secondTransaction)
        database.transaction {
            assertThat(transactionStorage.transactions).containsOnly(firstTransaction, secondTransaction)
        }
    }

    @Test
    fun `transaction saved twice in same DB transaction scope`() {
        val firstTransaction = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(firstTransaction)
            transactionStorage.addTransaction(firstTransaction)
        }
        assertTransactionIsRetrievable(firstTransaction)
        database.transaction {
            assertThat(transactionStorage.transactions).containsOnly(firstTransaction)
        }
    }

    @Test
    fun `transaction saved twice in two DB transaction scopes`() {
        val firstTransaction = newTransaction()
        val secondTransaction = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(firstTransaction)
        }

        database.transaction {
            transactionStorage.addTransaction(secondTransaction)
            transactionStorage.addTransaction(firstTransaction)
        }
        assertTransactionIsRetrievable(firstTransaction)
        database.transaction {
            assertThat(transactionStorage.transactions).containsOnly(firstTransaction, secondTransaction)
        }
    }

    @Test
    fun `updates are fired`() {
        val future = transactionStorage.updates.toFuture()
        val expected = newTransaction()
        database.transaction {
            transactionStorage.addTransaction(expected)
        }
        val actual = future.get(1, TimeUnit.SECONDS)
        assertEquals(expected, actual)
    }

    private fun newTransactionStorage() {
        database.transaction {
            transactionStorage = DBTransactionStorage()
        }
    }

    private fun assertTransactionIsRetrievable(transaction: SignedTransaction) {
        database.transaction {
            assertThat(transactionStorage.getTransaction(transaction.id)).isEqualTo(transaction)
        }
    }

    private fun newTransaction(): SignedTransaction {
        val wtx = WireTransaction(
                inputs = listOf(StateRef(SecureHash.randomSHA256(), 0)),
                attachments = emptyList(),
                outputs = emptyList(),
                commands = listOf(dummyCommand()),
                notary = DUMMY_NOTARY,
                timeWindow = null
        )
        return SignedTransaction(wtx, listOf(TransactionSignature(ByteArray(1), ALICE_PUBKEY, SignatureMetadata(1, Crypto.findSignatureScheme(ALICE_PUBKEY).schemeNumberID))))
    }
}
