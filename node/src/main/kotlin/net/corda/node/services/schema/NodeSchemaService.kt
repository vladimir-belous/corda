package net.corda.node.services.schema

import net.corda.core.contracts.ContractState
import net.corda.core.contracts.FungibleAsset
import net.corda.core.contracts.LinearState
import net.corda.core.internal.schemas.NodeInfoSchemaV1
import net.corda.core.schemas.CommonSchemaV1
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentState
import net.corda.core.schemas.QueryableState
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.node.internal.cordapp.CordappLoader
import net.corda.node.services.api.SchemaService
import net.corda.node.services.events.NodeSchedulerService
import net.corda.node.services.identity.PersistentIdentityService
import net.corda.node.services.keys.PersistentKeyManagementService
import net.corda.node.services.messaging.NodeMessagingClient
import net.corda.node.services.persistence.DBCheckpointStorage
import net.corda.node.services.persistence.DBTransactionMappingStorage
import net.corda.node.services.persistence.DBTransactionStorage
import net.corda.node.services.persistence.NodeAttachmentService
import net.corda.node.services.transactions.BFTNonValidatingNotaryService
import net.corda.node.services.transactions.PersistentUniquenessProvider
import net.corda.node.services.transactions.RaftUniquenessProvider
import net.corda.node.services.upgrade.ContractUpgradeServiceImpl
import net.corda.node.services.vault.VaultSchemaV1

/**
 * Most basic implementation of [SchemaService].
 * @param cordappLoader if not null, custom schemas will be extracted from its cordapps.
 * TODO: support loading schema options from node configuration.
 * TODO: support configuring what schemas are to be selected for persistence.
 * TODO: support plugins for schema version upgrading or custom mapping not supported by original [QueryableState].
 * TODO: create whitelisted tables when a CorDapp is first installed
 */
class NodeSchemaService(cordappLoader: CordappLoader?) : SchemaService, SingletonSerializeAsToken() {

    // Entities for compulsory services
    object NodeServices

    object NodeServicesV1 : MappedSchema(schemaFamily = NodeServices.javaClass, version = 1,
            mappedTypes = listOf(DBCheckpointStorage.DBCheckpoint::class.java,
                    DBTransactionStorage.DBTransaction::class.java,
                    DBTransactionMappingStorage.DBTransactionMapping::class.java,
                    PersistentKeyManagementService.PersistentKey::class.java,
                    PersistentUniquenessProvider.PersistentUniqueness::class.java,
                    PersistentUniquenessProvider.PersistentNotaryCommit::class.java,
                    NodeSchedulerService.PersistentScheduledState::class.java,
                    NodeAttachmentService.DBAttachment::class.java,
                    NodeMessagingClient.ProcessedMessage::class.java,
                    NodeMessagingClient.RetryMessage::class.java,
                    NodeAttachmentService.DBAttachment::class.java,
                    RaftUniquenessProvider.RaftState::class.java,
                    BFTNonValidatingNotaryService.PersistedCommittedState::class.java,
                    PersistentIdentityService.PersistentIdentity::class.java,
                    PersistentIdentityService.PersistentIdentityNames::class.java,
                    ContractUpgradeServiceImpl.DBContractUpgrade::class.java
            ))

    // Required schemas are those used by internal Corda services
    // For example, cash is used by the vault for coin selection (but will be extracted as a standalone CorDapp in future)
    private val requiredSchemas: Map<MappedSchema, SchemaService.SchemaOptions> =
            mapOf(Pair(CommonSchemaV1, SchemaService.SchemaOptions()),
                    Pair(VaultSchemaV1, SchemaService.SchemaOptions()),
                    Pair(NodeInfoSchemaV1, SchemaService.SchemaOptions()),
                    Pair(NodeServicesV1, SchemaService.SchemaOptions()))

    override val schemaOptions: Map<MappedSchema, SchemaService.SchemaOptions> = if (cordappLoader == null) {
        requiredSchemas
    } else {
        val customSchemas = cordappLoader.cordapps.flatMap { it.customSchemas }.toSet()
        requiredSchemas.plus(customSchemas.map { mappedSchema -> Pair(mappedSchema, SchemaService.SchemaOptions()) })
    }

    // Currently returns all schemas supported by the state, with no filtering or enrichment.
    override fun selectSchemas(state: ContractState): Iterable<MappedSchema> {
        val schemas = mutableSetOf<MappedSchema>()
        if (state is QueryableState)
            schemas += state.supportedSchemas()
        if (state is LinearState)
            schemas += VaultSchemaV1   // VaultLinearStates
        if (state is FungibleAsset<*>)
            schemas += VaultSchemaV1   // VaultFungibleStates

        return schemas
    }

    // Because schema is always one supported by the state, just delegate.
    override fun generateMappedObject(state: ContractState, schema: MappedSchema): PersistentState {
        if ((schema is VaultSchemaV1) && (state is LinearState))
            return VaultSchemaV1.VaultLinearStates(state.linearId, state.participants)
        if ((schema is VaultSchemaV1) && (state is FungibleAsset<*>))
            return VaultSchemaV1.VaultFungibleStates(state.owner, state.amount.quantity, state.amount.token.issuer.party, state.amount.token.issuer.reference, state.participants)
        return (state as QueryableState).generateMappedObject(schema)
    }
}
