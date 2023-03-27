//! ApiKey to tag request types.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_api_keys>

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ApiKey {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    LeaderAndIsr,
    StopReplica,
    UpdateMetadata,
    ControlledShutdown,
    OffsetCommit,
    OffsetFetch,
    FindCoordinator,
    JoinGroup,
    Heartbeat,
    LeaveGroup,
    SyncGroup,
    DescribeGroups,
    ListGroups,
    SaslHandshake,
    ApiVersions,
    CreateTopics,
    DeleteTopics,
    DeleteRecords,
    InitProducerId,
    OffsetForLeaderEpoch,
    AddPartitionsToTxn,
    AddOffsetsToTxn,
    EndTxn,
    WriteTxnMarkers,
    TxnOffsetCommit,
    DescribeAcls,
    CreateAcls,
    DeleteAcls,
    DescribeConfigs,
    AlterConfigs,
    AlterReplicaLogDirs,
    DescribeLogDirs,
    SaslAuthenticate,
    CreatePartitions,
    CreateDelegationToken,
    RenewDelegationToken,
    ExpireDelegationToken,
    DescribeDelegationToken,
    DeleteGroups,
    ElectLeaders,
    IncrementalAlterConfigs,
    AlterPartitionReassignments,
    ListPartitionReassignments,
    OffsetDelete,
    DescribeClientQuotas,
    AlterClientQuotas,
    DescribeUserScramCredentials,
    AlterUserScramCredentials,
    AlterIsr,
    UpdateFeatures,
    DescribeCluster,
    DescribeProducers,
    DescribeTransactions,
    ListTransactions,
    AllocateProducerIds,
    Unknown(i16),
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        match key {
            0 => Self::Produce,
            1 => Self::Fetch,
            2 => Self::ListOffsets,
            3 => Self::Metadata,
            4 => Self::LeaderAndIsr,
            5 => Self::StopReplica,
            6 => Self::UpdateMetadata,
            7 => Self::ControlledShutdown,
            8 => Self::OffsetCommit,
            9 => Self::OffsetFetch,
            10 => Self::FindCoordinator,
            11 => Self::JoinGroup,
            12 => Self::Heartbeat,
            13 => Self::LeaveGroup,
            14 => Self::SyncGroup,
            15 => Self::DescribeGroups,
            16 => Self::ListGroups,
            17 => Self::SaslHandshake,
            18 => Self::ApiVersions,
            19 => Self::CreateTopics,
            20 => Self::DeleteTopics,
            21 => Self::DeleteRecords,
            22 => Self::InitProducerId,
            23 => Self::OffsetForLeaderEpoch,
            24 => Self::AddPartitionsToTxn,
            25 => Self::AddOffsetsToTxn,
            26 => Self::EndTxn,
            27 => Self::WriteTxnMarkers,
            28 => Self::TxnOffsetCommit,
            29 => Self::DescribeAcls,
            30 => Self::CreateAcls,
            31 => Self::DeleteAcls,
            32 => Self::DescribeConfigs,
            33 => Self::AlterConfigs,
            34 => Self::AlterReplicaLogDirs,
            35 => Self::DescribeLogDirs,
            36 => Self::SaslAuthenticate,
            37 => Self::CreatePartitions,
            38 => Self::CreateDelegationToken,
            39 => Self::RenewDelegationToken,
            40 => Self::ExpireDelegationToken,
            41 => Self::DescribeDelegationToken,
            42 => Self::DeleteGroups,
            43 => Self::ElectLeaders,
            44 => Self::IncrementalAlterConfigs,
            45 => Self::AlterPartitionReassignments,
            46 => Self::ListPartitionReassignments,
            47 => Self::OffsetDelete,
            48 => Self::DescribeClientQuotas,
            49 => Self::AlterClientQuotas,
            50 => Self::DescribeUserScramCredentials,
            51 => Self::AlterUserScramCredentials,
            56 => Self::AlterIsr,
            57 => Self::UpdateFeatures,
            60 => Self::DescribeCluster,
            61 => Self::DescribeProducers,
            65 => Self::DescribeTransactions,
            66 => Self::ListTransactions,
            67 => Self::AllocateProducerIds,
            _ => Self::Unknown(key),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ListOffsets => 2,
            ApiKey::Metadata => 3,
            ApiKey::LeaderAndIsr => 4,
            ApiKey::StopReplica => 5,
            ApiKey::UpdateMetadata => 6,
            ApiKey::ControlledShutdown => 7,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 9,
            ApiKey::FindCoordinator => 10,
            ApiKey::JoinGroup => 11,
            ApiKey::Heartbeat => 12,
            ApiKey::LeaveGroup => 13,
            ApiKey::SyncGroup => 14,
            ApiKey::DescribeGroups => 15,
            ApiKey::ListGroups => 16,
            ApiKey::SaslHandshake => 17,
            ApiKey::ApiVersions => 18,
            ApiKey::CreateTopics => 19,
            ApiKey::DeleteTopics => 20,
            ApiKey::DeleteRecords => 21,
            ApiKey::InitProducerId => 22,
            ApiKey::OffsetForLeaderEpoch => 23,
            ApiKey::AddPartitionsToTxn => 24,
            ApiKey::AddOffsetsToTxn => 25,
            ApiKey::EndTxn => 26,
            ApiKey::WriteTxnMarkers => 27,
            ApiKey::TxnOffsetCommit => 28,
            ApiKey::DescribeAcls => 29,
            ApiKey::CreateAcls => 30,
            ApiKey::DeleteAcls => 31,
            ApiKey::DescribeConfigs => 32,
            ApiKey::AlterConfigs => 33,
            ApiKey::AlterReplicaLogDirs => 34,
            ApiKey::DescribeLogDirs => 35,
            ApiKey::SaslAuthenticate => 36,
            ApiKey::CreatePartitions => 37,
            ApiKey::CreateDelegationToken => 38,
            ApiKey::RenewDelegationToken => 39,
            ApiKey::ExpireDelegationToken => 40,
            ApiKey::DescribeDelegationToken => 41,
            ApiKey::DeleteGroups => 42,
            ApiKey::ElectLeaders => 43,
            ApiKey::IncrementalAlterConfigs => 44,
            ApiKey::AlterPartitionReassignments => 45,
            ApiKey::ListPartitionReassignments => 46,
            ApiKey::OffsetDelete => 47,
            ApiKey::DescribeClientQuotas => 48,
            ApiKey::AlterClientQuotas => 49,
            ApiKey::DescribeUserScramCredentials => 50,
            ApiKey::AlterUserScramCredentials => 51,
            ApiKey::AlterIsr => 56,
            ApiKey::UpdateFeatures => 57,
            ApiKey::DescribeCluster => 60,
            ApiKey::DescribeProducers => 61,
            ApiKey::DescribeTransactions => 65,
            ApiKey::ListTransactions => 66,
            ApiKey::AllocateProducerIds => 67,
            ApiKey::Unknown(code) => code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_roundrip_int16(code: i16) {
            let api_key = ApiKey::from(code);
            let code2 = i16::from(api_key);
            assert_eq!(code, code2);
        }

        #[test]
        fn test_roundrip_api_key(key: ApiKey) {
            let key = match key {
                // Ensure key is actually unknown
                ApiKey::Unknown(x) => ApiKey::from(x),
                _ => key,
            };

            let code = i16::from(key);
            let key2 = ApiKey::from(code);
            assert_eq!(key, key2);
        }
    }
}
