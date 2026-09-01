#include "vchunk_transfer_engine.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace mooncake {
namespace {

VChunkMetadataRecord MakeRecord() {
    VChunkMetadataRecord record;
    record.vchunk_id = "id";
    record.tenant_id = "tenant";
    record.key = "key";
    record.total_size = 4096 + 17;
    record.slice_count = 2;
    record.slice_size_level = VCSliceSizeLevel::k4K;
    record.row_size = 2;
    record.status = VChunkStatus::CREATING;
    record.created_at_ms = 1;
    record.last_updated_at_ms = 1;
    record.slices = {
        {0, "a", 1000, 4096, 4096, VCSliceStatus::PENDING, 0},
        {1, "b", 2000, 17, 4096, VCSliceStatus::PENDING, 0}};
    return record;
}

TEST(VChunkTransferEngineTest, BuildsAllRequestsBeforeSubmission) {
    auto record = MakeRecord();
    std::array<char, 4096 + 17> buffer{};
    int resolutions = 0;
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [&](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            ++resolutions;
            return name == "a" ? 11 : 22;
        });
    ASSERT_TRUE(requests.has_value());
    ASSERT_EQ(requests->size(), 2U);
    EXPECT_EQ(resolutions, 2);
    EXPECT_EQ((*requests)[0].source, buffer.data());
    EXPECT_EQ((*requests)[0].target_id, 11U);
    EXPECT_EQ((*requests)[0].target_offset, 1000U);
    EXPECT_EQ((*requests)[0].length, 4096U);
    EXPECT_EQ((*requests)[1].source, buffer.data() + 4096);
    EXPECT_EQ((*requests)[1].target_id, 22U);
    EXPECT_EQ((*requests)[1].length, 17U);
}

TEST(VChunkTransferEngineTest, RejectsAnyUnresolvableSliceAsAWhole) {
    auto record = MakeRecord();
    std::array<char, 4096 + 17> buffer{};
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::READ,
        [](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            if (name == "b") {
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            return 11;
        });
    ASSERT_FALSE(requests.has_value());
    EXPECT_EQ(requests.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST(VChunkTransferEngineTest, ReusesHandleWithinOneSegment) {
    auto record = MakeRecord();
    record.slices[1].target_segment_name = "a";
    record.row_size = 1;
    std::array<char, 4096 + 17> buffer{};
    int resolutions = 0;
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [&](const std::string&)
            -> tl::expected<SegmentHandle, ErrorCode> {
            ++resolutions;
            return 11;
        });
    ASSERT_TRUE(requests.has_value());
    EXPECT_EQ(resolutions, 1);
}

TEST(VChunkTransferEngineTest, RejectsUnknownMetadataVersion) {
    auto record = MakeRecord();
    record.schema_version = kVChunkMetadataSchemaVersion + 1;
    std::array<char, 4096 + 17> buffer{};
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [](const std::string&)
            -> tl::expected<SegmentHandle, ErrorCode> { return 11; });
    ASSERT_FALSE(requests.has_value());
    EXPECT_EQ(requests.error(), ErrorCode::INVALID_VERSION);
}

TEST(VChunkTransferEngineTest, GroupsWritesBySegment) {
    auto record = MakeRecord();
    std::array<char, 4096 + 17> buffer{};
    auto batches = BuildVChunkTransferBatches(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            return name == "a" ? 11 : 22;
        });
    ASSERT_TRUE(batches.has_value());
    ASSERT_EQ(batches->size(), 2U);
    EXPECT_EQ((*batches)[0].segment_name, "a");
    EXPECT_EQ((*batches)[0].requests.size(), 1U);
    EXPECT_EQ((*batches)[1].segment_name, "b");
    EXPECT_EQ((*batches)[1].requests.size(), 1U);
}

TEST(VChunkTransferEngineTest, MergesAdjacentReadsOnOneSegment) {
    auto record = MakeRecord();
    record.slices[1].target_segment_name = "a";
    record.slices[1].target_offset = 1000 + 4096;
    record.row_size = 1;
    std::array<char, 4096 + 17> buffer{};
    auto batches = BuildVChunkTransferBatches(
        record, buffer.data(), buffer.size(), TransferRequest::READ,
        [](const std::string&)
            -> tl::expected<SegmentHandle, ErrorCode> { return 11; });
    ASSERT_TRUE(batches.has_value());
    ASSERT_EQ(batches->size(), 1U);
    ASSERT_EQ((*batches)[0].requests.size(), 1U);
    EXPECT_EQ((*batches)[0].requests[0].length, buffer.size());
}

TEST(VChunkTransferEngineTest, FallsBackToHealthyReplicaPerSlice) {
    auto record = MakeRecord();
    record.replica_num = 2;
    auto replica_a = record.slices[0];
    replica_a.target_segment_name = "c";
    replica_a.replica_index = 1;
    auto replica_b = record.slices[1];
    replica_b.target_segment_name = "d";
    replica_b.replica_index = 1;
    record.slices.push_back(replica_a);
    record.slices.push_back(replica_b);
    std::array<char, 4096 + 17> buffer{};

    auto batches = BuildVChunkTransferBatches(
        record, buffer.data(), buffer.size(), TransferRequest::READ,
        [](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            if (name == "a") {
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            if (name == "b") return 22;
            if (name == "c") return 33;
            return 44;
        });
    ASSERT_TRUE(batches.has_value());
    ASSERT_EQ(batches->size(), 2U);
    EXPECT_EQ((*batches)[0].segment_name, "c");
    EXPECT_EQ((*batches)[1].segment_name, "b");
}

TEST(VChunkTransferEngineTest, ExcludesFailedSegmentOnReadRetry) {
    auto record = MakeRecord();
    record.replica_num = 2;
    auto replica_a = record.slices[0];
    replica_a.target_segment_name = "c";
    replica_a.replica_index = 1;
    auto replica_b = record.slices[1];
    replica_b.target_segment_name = "d";
    replica_b.replica_index = 1;
    record.slices.push_back(replica_a);
    record.slices.push_back(replica_b);
    std::array<char, 4096 + 17> buffer{};

    auto batches = BuildVChunkTransferBatches(
        record, buffer.data(), buffer.size(), TransferRequest::READ,
        [](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            return name == "c" ? 33 : 44;
        },
        true, {"a", "b"});
    ASSERT_TRUE(batches.has_value());
    ASSERT_EQ(batches->size(), 2U);
    EXPECT_EQ((*batches)[0].segment_name, "c");
    EXPECT_EQ((*batches)[1].segment_name, "d");
}

}  // namespace
}  // namespace mooncake
